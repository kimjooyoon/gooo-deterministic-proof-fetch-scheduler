package scheduler

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

func DigestBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ParseSource(path string) (Source, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Source{}, err
	}
	return ParseSourceBytes(raw)
}

func ParseSourceBytes(raw []byte) (Source, error) {
	source := Source{SourceDigest: DigestBytes(raw)}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		if comment := strings.Index(line, " #"); comment >= 0 {
			line = strings.TrimSpace(line[:comment])
		}
		fields, err := parseFields(line)
		if err != nil {
			return Source{}, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "gooo" {
			if len(fields) != 3 || fields[1] != "deterministic_proof_fetch_scheduler" || fields[2] != "v1" {
				return Source{}, fmt.Errorf("line %d: invalid gooo header", lineNumber)
			}
			source.Schema = Schema
			source.Version = fields[2]
			continue
		}
		values, err := parseKeyValues(fields[1:])
		if err != nil {
			return Source{}, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		switch fields[0] {
		case "authority":
			source.Authority, err = parseAuthority(values)
		case "guardrail":
			source.Guardrail, err = parseGuardrail(values)
		case "concurrency":
			source.ConcurrencyBound, err = parseInt(values, "bound")
		case "retry":
			source.Retry, err = parseRetry(values)
		case "canonical":
			source.CanonicalOrder = splitList(values["order"])
		case "lock":
			lock := Lock{ID: values["id"], Coordinate: values["coordinate"], Digest: values["digest"], Dependencies: splitList(values["deps"]), Behavior: values["behavior"]}
			lock.LatencyMS, err = parseInt(values, "latency_ms")
			if err == nil {
				source.Locks = append(source.Locks, lock)
			}
		case "case":
			item := CaseDecl{ID: values["id"], Kind: values["kind"], Expected: values["expected"]}
			item.Ordinal, err = parseInt(values, "ordinal")
			if err == nil {
				source.Cases = append(source.Cases, item)
			}
		default:
			return Source{}, fmt.Errorf("line %d: unknown declaration %q", lineNumber, fields[0])
		}
		if err != nil {
			return Source{}, fmt.Errorf("line %d: %w", lineNumber, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return Source{}, err
	}
	return source, nil
}

func parseAuthority(values map[string]string) (Authority, error) {
	var authority Authority
	var err error
	fields := []struct {
		name string
		dest *int
	}{
		{"repository_writes", &authority.RepositoryWrites},
		{"input_repository_writes", &authority.InputRepositoryWrites},
		{"local_test_executions", &authority.LocalTestExecutions},
		{"cross_project_required_gates", &authority.CrossProjectRequiredGates},
		{"automatic_commit", &authority.AutomaticCommit},
		{"automatic_push", &authority.AutomaticPush},
		{"automatic_merge", &authority.AutomaticMerge},
		{"automatic_tag", &authority.AutomaticTag},
		{"automatic_release", &authority.AutomaticRelease},
	}
	for _, field := range fields {
		*field.dest, err = parseInt(values, field.name)
		if err != nil {
			return authority, err
		}
	}
	return authority, nil
}

func parseGuardrail(values map[string]string) (Guardrail, error) {
	var guardrail Guardrail
	guardrail.ForbiddenLocalValidation = strings.Split(values["forbidden_local_validation"], "|")
	guardrail.OperationalRefuted = values["operational_refuted"] == "true"
	guardrail.FailureDeletesForbidden = values["failure_deletes_forbidden"] == "true"
	var err error
	guardrail.RuntimeAuthority, err = parseAuthority(values)
	return guardrail, err
}

func parseRetry(values map[string]string) (RetryPolicy, error) {
	var retry RetryPolicy
	var err error
	retry.MaxAttempts, err = parseInt(values, "max_attempts")
	if err != nil {
		return retry, err
	}
	retry.BackoffMS, err = parseInt(values, "backoff_ms")
	if err != nil {
		return retry, err
	}
	for _, raw := range splitList(values["retryable_statuses"]) {
		status, parseErr := strconv.Atoi(raw)
		if parseErr != nil {
			return retry, fmt.Errorf("invalid retryable status %q", raw)
		}
		retry.RetryableStatuses = append(retry.RetryableStatuses, status)
	}
	return retry, nil
}

func parseFields(line string) ([]string, error) {
	fields := []string{}
	for index := 0; index < len(line); {
		for index < len(line) && (line[index] == ' ' || line[index] == '\t') {
			index++
		}
		if index == len(line) {
			break
		}
		start := index
		quoted := false
		for index < len(line) {
			switch line[index] {
			case '"':
				quoted = !quoted
			case ' ', '\t':
				if !quoted {
					goto fieldEnd
				}
			}
			index++
		}
	fieldEnd:
		value := strings.Trim(line[start:index], "\"")
		if value == "" {
			return nil, fmt.Errorf("empty field")
		}
		fields = append(fields, value)
	}
	return fields, nil
}

func parseKeyValues(fields []string) (map[string]string, error) {
	values := make(map[string]string, len(fields))
	for _, field := range fields {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			return nil, fmt.Errorf("invalid key/value %q", field)
		}
		value := strings.Trim(parts[1], "\"")
		if value == "" {
			return nil, fmt.Errorf("empty value for %q", parts[0])
		}
		values[parts[0]] = value
	}
	return values, nil
}

func parseInt(values map[string]string, key string) (int, error) {
	value, ok := values[key]
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}
	number, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q", key, value)
	}
	return number, nil
}

func splitList(value string) []string {
	if value == "" || value == "-" {
		return []string{}
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" && part != "-" {
			result = append(result, part)
		}
	}
	return result
}

func LoadContract(path string) (Contract, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Contract{}, "", err
	}
	var contract Contract
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&contract); err != nil {
		return Contract{}, "", fmt.Errorf("decode contract: %w", err)
	}
	return contract, DigestBytes(raw), nil
}

func LoadFixture(path string, source Source) (Fixture, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Fixture{}, "", err
	}
	var fixture Fixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		return Fixture{}, "", fmt.Errorf("decode fixture: %w", err)
	}
	if len(fixture.Records) != len(source.Locks) {
		return Fixture{}, "", fmt.Errorf("fixture has %d records, expected %d", len(fixture.Records), len(source.Locks))
	}
	locks := make(map[string]Lock, len(source.Locks))
	for _, lock := range source.Locks {
		locks[lock.ID] = lock
	}
	seen := make(map[string]bool, len(fixture.Records))
	for _, record := range fixture.Records {
		lock, ok := locks[record.LockID]
		if !ok || seen[record.LockID] {
			return Fixture{}, "", fmt.Errorf("fixture lock authority is invalid for %q", record.LockID)
		}
		seen[record.LockID] = true
		if record.HTTPStatus == 200 && record.ObservedCoordinate == nil {
			return Fixture{}, "", fmt.Errorf("successful fixture response lacks coordinate for %q", record.LockID)
		}
		if record.HTTPStatus == 200 && record.ObservedDigest == nil {
			return Fixture{}, "", fmt.Errorf("successful fixture response lacks digest for %q", record.LockID)
		}
		if record.BodyMode == "" || lock.Behavior == "" {
			return Fixture{}, "", fmt.Errorf("fixture behavior is incomplete for %q", record.LockID)
		}
	}
	fixture.Digest = DigestBytes(raw)
	return fixture, fixture.Digest, nil
}

func LoadCasesFixture(path string, source Source) (CasesFixture, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return CasesFixture{}, "", err
	}
	var fixture CasesFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		return CasesFixture{}, "", fmt.Errorf("decode cases fixture: %w", err)
	}
	if len(fixture.Cases) != len(source.Cases) {
		return CasesFixture{}, "", fmt.Errorf("case fixture has %d cases, expected %d", len(fixture.Cases), len(source.Cases))
	}
	declared := make(map[string]CaseDecl, len(source.Cases))
	for _, item := range source.Cases {
		declared[item.ID] = item
	}
	seen := make(map[string]bool, len(fixture.Cases))
	for _, item := range fixture.Cases {
		decl, ok := declared[item.ID]
		if !ok || seen[item.ID] || decl.Expected == "" {
			return CasesFixture{}, "", fmt.Errorf("case fixture authority is invalid for %q", item.ID)
		}
		seen[item.ID] = true
	}
	fixture.Digest = DigestBytes(raw)
	return fixture, fixture.Digest, nil
}

func Validate(source Source, contract Contract) error {
	if err := validateSource(source, contract); err != nil {
		return err
	}
	if contract.RequiredOutputs == nil || len(contract.RequiredOutputs) != len(ArtifactFiles) {
		return fmt.Errorf("contract required output inventory is incomplete")
	}
	for index, name := range ArtifactFiles {
		if contract.RequiredOutputs[index] != name {
			return fmt.Errorf("required output %d is %q, expected %q", index, contract.RequiredOutputs[index], name)
		}
	}
	if strings.Join(contract.StatePrecedence, ">") != StateRefuted+">"+StateUnknown+">"+StateClosed || contract.UnknownFieldCount != 6 || contract.CrossProjectRequiredGates != 0 || contract.GoVersion != "1.27" {
		return fmt.Errorf("contract guardrail mismatch")
	}
	return validateDAG(source)
}

func validateDAG(source Source) error {
	known := make(map[string]bool, len(source.Locks))
	indegree := make(map[string]int, len(source.Locks))
	children := make(map[string][]string, len(source.Locks))
	for _, lock := range source.Locks {
		known[lock.ID] = true
		indegree[lock.ID] = len(lock.Dependencies)
		for _, dep := range lock.Dependencies {
			if !known[dep] {
				// All declared locks are parsed before validation; defer the exact
				// check until the complete set is available below.
			}
			children[dep] = append(children[dep], lock.ID)
		}
	}
	for _, lock := range source.Locks {
		for _, dep := range lock.Dependencies {
			if !known[dep] {
				return fmt.Errorf("lock %q depends on undeclared lock %q", lock.ID, dep)
			}
		}
	}
	queue := []string{}
	for id, degree := range indegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}
	processed := 0
	for len(queue) > 0 {
		sort.Strings(queue)
		id := queue[0]
		queue = queue[1:]
		processed++
		for _, child := range children[id] {
			indegree[child]--
			if indegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	if processed != len(source.Locks) {
		return fmt.Errorf("proof lock graph contains a cycle")
	}
	canonical := append([]string(nil), source.CanonicalOrder...)
	sorted := append([]string(nil), canonical...)
	sort.Strings(sorted)
	for index := range canonical {
		if canonical[index] != sorted[index] {
			return fmt.Errorf("canonical order must be fixed lock ID order")
		}
	}
	return nil
}

func lockMap(source Source) map[string]Lock {
	result := make(map[string]Lock, len(source.Locks))
	for _, lock := range source.Locks {
		result[lock.ID] = lock
	}
	return result
}

func fixtureMap(fixture Fixture) map[string]FixtureRecord {
	result := make(map[string]FixtureRecord, len(fixture.Records))
	for _, record := range fixture.Records {
		result[record.LockID] = record
	}
	return result
}

func caseFixtureMap(fixture CasesFixture) map[string]CaseFixture {
	result := make(map[string]CaseFixture, len(fixture.Cases))
	for _, item := range fixture.Cases {
		result[item.ID] = item
	}
	return result
}
