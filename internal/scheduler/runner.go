package scheduler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type fixtureResponse struct {
	LockID     string `json:"lock_id"`
	Coordinate string `json:"coordinate"`
	Digest     string `json:"digest"`
}

type fixtureServer struct {
	server *httptest.Server
}

func newFixtureServer(source Source, fixture Fixture) *fixtureServer {
	locks := lockMap(source)
	records := fixtureMap(fixture)
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		lockID := path.Base(request.URL.Path)
		lock, ok := locks[lockID]
		if !ok {
			http.NotFound(writer, request)
			return
		}
		record := records[lockID]
		if lock.LatencyMS > 0 {
			time.Sleep(time.Duration(lock.LatencyMS) * time.Millisecond)
		}
		switch lock.Behavior {
		case "missing":
			writer.WriteHeader(http.StatusNotFound)
			return
		case "timeout":
			// The extra delay is intentional: the client deadline, rather than a
			// guessed zero duration, determines the UNKNOWN result.
			time.Sleep(80 * time.Millisecond)
			writer.WriteHeader(http.StatusGatewayTimeout)
			return
		case "rate-limit":
			writer.WriteHeader(http.StatusTooManyRequests)
			return
		case "ambiguous":
			writer.WriteHeader(http.StatusMultipleChoices)
			return
		case "success", "reused", "digest-mismatch", "coordinate-contradiction":
			if record.HTTPStatus != 200 {
				writer.WriteHeader(record.HTTPStatus)
				return
			}
			response := fixtureResponse{LockID: lockID}
			if record.ObservedCoordinate != nil {
				response.Coordinate = *record.ObservedCoordinate
			}
			if record.ObservedDigest != nil {
				response.Digest = *record.ObservedDigest
			}
			writer.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(response)
		default:
			writer.WriteHeader(http.StatusInternalServerError)
		}
	})
	return &fixtureServer{server: httptest.NewServer(handler)}
}

func (server *fixtureServer) close() {
	server.server.Close()
}

type inflightMeter struct {
	current atomic.Int32
	peak    atomic.Int32
}

func (meter *inflightMeter) begin() {
	current := meter.current.Add(1)
	for {
		peak := meter.peak.Load()
		if current <= peak || meter.peak.CompareAndSwap(peak, current) {
			return
		}
	}
}

func (meter *inflightMeter) end() {
	meter.current.Add(-1)
}

func (meter *inflightMeter) max() int {
	return int(meter.peak.Load())
}

type runOptions struct {
	mode        string
	concurrency int
}

func RunPair(source Source, fixture Fixture) (RunResult, RunResult, PairComparison, error) {
	server := newFixtureServer(source, fixture)
	defer server.close()
	baseline, err := run(source, fixture, server, runOptions{mode: "sequential", concurrency: 1})
	if err != nil {
		return RunResult{}, RunResult{}, PairComparison{}, err
	}
	candidate, err := run(source, fixture, server, runOptions{mode: "bounded-parallel", concurrency: source.ConcurrencyBound})
	if err != nil {
		return RunResult{}, RunResult{}, PairComparison{}, err
	}
	return baseline, candidate, ComparePair(baseline, candidate), nil
}

func ReplayPair(source Source, fixture Fixture) (ReplayReceipt, error) {
	server := newFixtureServer(source, fixture)
	defer server.close()
	first, err := run(source, fixture, server, runOptions{mode: "bounded-parallel", concurrency: source.ConcurrencyBound})
	if err != nil {
		return ReplayReceipt{}, err
	}
	second, err := run(source, fixture, server, runOptions{mode: "bounded-parallel", concurrency: source.ConcurrencyBound})
	if err != nil {
		return ReplayReceipt{}, err
	}
	deterministic := first.SemanticRoot == second.SemanticRoot
	decision := StateUnknown
	reason := "REPLAY_ROOT_MISMATCH"
	if deterministic {
		decision = StateClosed
		reason = "CANONICAL_EVIDENCE_REPLAY_EXACT"
	}
	return ReplayReceipt{
		Schema: Schema, ContractID: ContractID, FirstSemanticRoot: first.SemanticRoot,
		SecondSemanticRoot: second.SemanticRoot, Deterministic: deterministic,
		ReplayCount: 2, Decision: decision, Reason: reason,
	}, nil
}

func run(source Source, fixture Fixture, server *fixtureServer, options runOptions) (RunResult, error) {
	if options.concurrency < 1 || options.concurrency > source.ConcurrencyBound {
		return RunResult{}, fmt.Errorf("requested concurrency %d is outside source bound %d", options.concurrency, source.ConcurrencyBound)
	}
	started := time.Now()
	locks := lockMap(source)
	records := fixtureMap(fixture)
	evidence := make([]LockEvidence, len(source.CanonicalOrder))
	meter := &inflightMeter{}
	client := &http.Client{Timeout: 60 * time.Millisecond}
	if options.mode == "sequential" {
		for index, id := range source.CanonicalOrder {
			lock := locks[id]
			evidence[index] = fetchLock(client, server, lock, records[id], source.Retry, meter)
		}
	} else {
		waves, err := buildWaves(source)
		if err != nil {
			return RunResult{}, err
		}
		indices := make(map[string]int, len(source.CanonicalOrder))
		for index, id := range source.CanonicalOrder {
			indices[id] = index
		}
		for _, wave := range waves {
			var group sync.WaitGroup
			semaphore := make(chan struct{}, options.concurrency)
			for _, id := range wave {
				id := id
				group.Add(1)
				go func() {
					defer group.Done()
					semaphore <- struct{}{}
					defer func() { <-semaphore }()
					evidence[indices[id]] = fetchLock(client, server, locks[id], records[id], source.Retry, meter)
				}()
			}
			group.Wait()
		}
	}
	canonical, err := canonicalEvidence(evidence)
	if err != nil {
		return RunResult{}, err
	}
	metrics := summarizeMetrics(evidence, time.Since(started), meter.max())
	return RunResult{Mode: options.mode, Evidence: evidence, Metrics: metrics, SemanticRoot: DigestBytes(canonical), Schedule: buildSchedule(source)}, nil
}

func buildWaves(source Source) ([][]string, error) {
	locks := lockMap(source)
	waveByID := make(map[string]int, len(source.Locks))
	maxWave := 0
	for _, id := range source.CanonicalOrder {
		lock := locks[id]
		wave := 0
		for _, dependency := range lock.Dependencies {
			dependencyWave, ok := waveByID[dependency]
			if !ok {
				return nil, fmt.Errorf("canonical order is not dependency-respecting at %q", id)
			}
			if dependencyWave+1 > wave {
				wave = dependencyWave + 1
			}
		}
		waveByID[id] = wave
		if wave > maxWave {
			maxWave = wave
		}
	}
	waves := make([][]string, maxWave+1)
	for _, id := range source.CanonicalOrder {
		wave := waveByID[id]
		waves[wave] = append(waves[wave], id)
	}
	for index := range waves {
		sort.Strings(waves[index])
	}
	return waves, nil
}

func buildSchedule(source Source) []ScheduleItem {
	waves, _ := buildWaves(source)
	waveByID := map[string]int{}
	for wave, ids := range waves {
		for _, id := range ids {
			waveByID[id] = wave
		}
	}
	locks := lockMap(source)
	result := make([]ScheduleItem, 0, len(source.CanonicalOrder))
	for index, id := range source.CanonicalOrder {
		result = append(result, ScheduleItem{Ordinal: index + 1, LockID: id, Dependencies: append([]string(nil), locks[id].Dependencies...), Wave: waveByID[id] + 1})
	}
	return result
}

func fetchLock(client *http.Client, server *fixtureServer, lock Lock, record FixtureRecord, retry RetryPolicy, meter *inflightMeter) LockEvidence {
	if lock.Behavior == "reused" {
		digest := lock.Digest
		return LockEvidence{LockID: lock.ID, Coordinate: lock.Coordinate, Status: StateClosed, Digest: &digest, Reused: true}
	}
	maxAttempts := retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var lastUnknown *Unknown
	var requestCount int
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		requestCount++
		meter.begin()
		response, err := client.Get(server.server.URL + "/locks/" + lock.ID)
		meter.end()
		if err != nil {
			lastUnknown = unknownFor(lock.ID, "HTTP_TIMEOUT", "TRANSIENT_TIMEOUT", "RETRY_LOCK_FETCH", "timeout")
			if attempt < maxAttempts {
				time.Sleep(time.Duration(retry.BackoffMS) * time.Millisecond)
				continue
			}
			return LockEvidence{LockID: lock.ID, Coordinate: lock.Coordinate, Status: StateUnknown, Attempts: attempt, RequestCount: requestCount, Unknown: lastUnknown}
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			lastUnknown = unknownFor(lock.ID, "HTTP_BODY_READ_FAILED", "AMBIGUOUS_HTTP", "RESTORE_LOCK_EVIDENCE", "body")
			return LockEvidence{LockID: lock.ID, Coordinate: lock.Coordinate, Status: StateUnknown, Attempts: attempt, RequestCount: requestCount, Unknown: lastUnknown}
		}
		if response.StatusCode != http.StatusOK {
			unknown := unknownForStatus(lock.ID, response.StatusCode)
			if isRetryable(response.StatusCode, retry) && attempt < maxAttempts {
				time.Sleep(time.Duration(retry.BackoffMS) * time.Millisecond)
				lastUnknown = unknown
				continue
			}
			return LockEvidence{LockID: lock.ID, Coordinate: lock.Coordinate, Status: StateUnknown, Attempts: attempt, RequestCount: requestCount, Unknown: unknown}
		}
		var parsed fixtureResponse
		if err := json.Unmarshal(body, &parsed); err != nil || parsed.LockID != lock.ID || parsed.Coordinate == "" || parsed.Digest == "" {
			unknown := unknownFor(lock.ID, "HTTP_BODY_AMBIGUOUS", "AMBIGUOUS_HTTP", "RESTORE_LOCK_EVIDENCE", "body")
			return LockEvidence{LockID: lock.ID, Coordinate: lock.Coordinate, Status: StateUnknown, Attempts: attempt, RequestCount: requestCount, Unknown: unknown}
		}
		if parsed.Coordinate != lock.Coordinate {
			counterexample := &Counterexample{Kind: "COORDINATE_CONTRADICTION", Field: "coordinate", Expected: lock.Coordinate, Observed: parsed.Coordinate}
			digest := parsed.Digest
			return LockEvidence{LockID: lock.ID, Coordinate: lock.Coordinate, Status: StateRefuted, Digest: &digest, Attempts: attempt, RequestCount: requestCount, Counterexample: counterexample}
		}
		if parsed.Digest != lock.Digest {
			counterexample := &Counterexample{Kind: "KNOWN_DIGEST_MISMATCH", Field: "digest", Expected: lock.Digest, Observed: parsed.Digest}
			digest := parsed.Digest
			return LockEvidence{LockID: lock.ID, Coordinate: lock.Coordinate, Status: StateRefuted, Digest: &digest, Attempts: attempt, RequestCount: requestCount, Counterexample: counterexample}
		}
		digest := parsed.Digest
		return LockEvidence{LockID: lock.ID, Coordinate: lock.Coordinate, Status: StateClosed, Digest: &digest, Attempts: attempt, RequestCount: requestCount}
	}
	return LockEvidence{LockID: lock.ID, Coordinate: lock.Coordinate, Status: StateUnknown, RequestCount: requestCount, Unknown: lastUnknown}
}

func isRetryable(status int, retry RetryPolicy) bool {
	for _, candidate := range retry.RetryableStatuses {
		if candidate == status {
			return true
		}
	}
	return false
}

func unknownForStatus(lockID string, status int) *Unknown {
	reason := "HTTP_STATUS_UNKNOWN"
	class := "AMBIGUOUS_HTTP"
	next := "RESTORE_LOCK_EVIDENCE"
	blocked := "http-status"
	switch status {
	case http.StatusNotFound:
		reason, class, next, blocked = "HTTP_404_MISSING", "DIRECT_MISSING", "RESTORE_MISSING_LOCK_EVIDENCE", "missing"
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		reason, class, next, blocked = "HTTP_TIMEOUT", "TRANSIENT_TIMEOUT", "RETRY_LOCK_FETCH", "timeout"
	case http.StatusTooManyRequests:
		reason, class, next, blocked = "HTTP_429_RATE_LIMIT", "RATE_LIMIT", "RETRY_AFTER_RATE_LIMIT", "rate-limit"
	case http.StatusMultipleChoices:
		reason, class, next, blocked = "HTTP_3XX_AMBIGUOUS", "AMBIGUOUS_HTTP", "DISAMBIGUATE_HTTP_RESPONSE", "ambiguous-http"
	}
	return unknownFor(lockID, reason, class, next, blocked)
}

func unknownFor(lockID, reason, class, next, blocked string) *Unknown {
	return &Unknown{Stage: "FETCH", Step: "READ_HTTP_EVIDENCE", Reason: reason, UnknownClass: class, NextOperation: next, BlockedBy: []string{"lock:" + lockID, blocked}}
}

func summarizeMetrics(evidence []LockEvidence, elapsed time.Duration, maxInFlight int) Metrics {
	metrics := Metrics{WallMS: elapsed.Milliseconds(), PeakRSSKib: peakRSSKib(), MaxInFlight: maxInFlight}
	for _, item := range evidence {
		metrics.Requests += item.RequestCount
		if item.Reused {
			metrics.Reused++
		}
		switch item.Status {
		case StateClosed:
			metrics.Completed++
		case StateUnknown:
			metrics.Unknown++
		case StateRefuted:
			metrics.Refuted++
		}
	}
	return metrics
}

func ComparePair(baseline, candidate RunResult) PairComparison {
	comparison := PairComparison{Schema: Schema, Baseline: baseline.Metrics, Candidate: candidate.Metrics, BaselineFinalVerdict: FinalVerdict(baseline.Evidence), CandidateFinalVerdict: FinalVerdict(candidate.Evidence), BaselineSemanticRoot: baseline.SemanticRoot, CandidateSemanticRoot: candidate.SemanticRoot}
	comparison.PerLockStatusExact, comparison.DigestExact, comparison.UnknownFieldsExact, comparison.CounterexamplesExact = compareEvidence(baseline.Evidence, candidate.Evidence)
	comparison.FinalVerdictExact = comparison.BaselineFinalVerdict == comparison.CandidateFinalVerdict
	comparison.FinalSemanticRootExact = baseline.SemanticRoot == candidate.SemanticRoot
	comparison.AllCriticalFieldsExact = comparison.PerLockStatusExact && comparison.DigestExact && comparison.UnknownFieldsExact && comparison.CounterexamplesExact && comparison.FinalVerdictExact && comparison.FinalSemanticRootExact
	comparison.WallReductionActual = candidate.Metrics.WallMS < baseline.Metrics.WallMS
	if !comparison.AllCriticalFieldsExact {
		comparison.Verdict = StateRefuted
		comparison.Reason = "CONCURRENCY_CHANGED_CANONICAL_EVIDENCE"
		comparison.Counterexample = firstEvidenceDifference(baseline.Evidence, candidate.Evidence)
		if comparison.Counterexample == nil {
			comparison.Counterexample = &Counterexample{Kind: "SEMANTIC_ROOT_MISMATCH", Field: "semantic_root", Expected: baseline.SemanticRoot, Observed: candidate.SemanticRoot}
		}
	} else if !comparison.WallReductionActual {
		comparison.Verdict = StateUnknown
		comparison.Reason = "ACTUAL_WALL_REDUCTION_NOT_DEMONSTRATED"
		comparison.Unknown = &Unknown{Stage: "MEASURE", Step: "COMPARE_EXACT_PAIR", Reason: comparison.Reason, UnknownClass: "INSUFFICIENT_PERFORMANCE_EVIDENCE", NextOperation: "REPEAT_SAME_CI_JOB_PAIR", BlockedBy: []string{"wall_ms"}}
	} else {
		comparison.Verdict = StateClosed
		comparison.Reason = "EXACT_CANONICAL_PAIR_AND_ACTUAL_WALL_REDUCTION"
	}
	return comparison
}

func compareEvidence(left, right []LockEvidence) (bool, bool, bool, bool) {
	if len(left) != len(right) {
		return false, false, false, false
	}
	statusExact, digestExact, unknownExact, counterexampleExact := true, true, true, true
	for index := range left {
		if left[index].LockID != right[index].LockID || left[index].Status != right[index].Status {
			statusExact = false
		}
		if !equalStringPointer(left[index].Digest, right[index].Digest) {
			digestExact = false
		}
		if !equalUnknown(left[index].Unknown, right[index].Unknown) {
			unknownExact = false
		}
		if !equalCounterexample(left[index].Counterexample, right[index].Counterexample) {
			counterexampleExact = false
		}
	}
	return statusExact, digestExact, unknownExact, counterexampleExact
}

func firstEvidenceDifference(left, right []LockEvidence) *Counterexample {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	for index := 0; index < limit; index++ {
		if left[index].Status != right[index].Status {
			return &Counterexample{Kind: "PER_LOCK_STATUS_DIFFERENCE", Field: "status", Expected: left[index].Status, Observed: right[index].Status}
		}
		if !equalStringPointer(left[index].Digest, right[index].Digest) {
			return &Counterexample{Kind: "PER_LOCK_DIGEST_DIFFERENCE", Field: "digest", Expected: pointerValue(left[index].Digest), Observed: pointerValue(right[index].Digest)}
		}
	}
	return nil
}

func equalStringPointer(left, right *string) bool {
	return pointerValue(left) == pointerValue(right)
}

func pointerValue(value *string) string {
	if value == nil {
		return "<null>"
	}
	return *value
}

func equalUnknown(left, right *Unknown) bool {
	leftRaw, _ := json.Marshal(left)
	rightRaw, _ := json.Marshal(right)
	return string(leftRaw) == string(rightRaw)
}

func equalCounterexample(left, right *Counterexample) bool {
	leftRaw, _ := json.Marshal(left)
	rightRaw, _ := json.Marshal(right)
	return string(leftRaw) == string(rightRaw)
}

func EvaluateCanonicalCases(source Source, fixture CasesFixture) ([]CaseResult, error) {
	items := caseFixtureMap(fixture)
	results := make([]CaseResult, 0, len(source.Cases))
	for _, declaration := range source.Cases {
		item, ok := items[declaration.ID]
		if !ok {
			return nil, fmt.Errorf("missing canonical case fixture %q", declaration.ID)
		}
		result := evaluateCase(declaration, item)
		if result.Decision != declaration.Expected {
			return nil, fmt.Errorf("case %q expected %s, got %s", declaration.ID, declaration.Expected, result.Decision)
		}
		results = append(results, result)
	}
	return results, nil
}

func evaluateCase(declaration CaseDecl, item CaseFixture) CaseResult {
	result := CaseResult{Ordinal: declaration.Ordinal, CaseID: declaration.ID, Expected: declaration.Expected, Decision: StateUnknown, Reason: "CASE_NOT_EVALUATED"}
	if item.Kind == "race-divergence" {
		result.Decision = StateRefuted
		result.Reason = "CONCURRENCY_RACE_CHANGED_RESULT"
		result.Counterexample = &Counterexample{Kind: "RACE_DIVERGENCE", Field: item.RaceField, Expected: item.RaceBaseline, Observed: item.RaceCandidate}
		return result
	}
	if item.HTTPStatus != http.StatusOK || item.Kind == "missing" || item.Kind == "timeout" || item.Kind == "rate-limit" || item.Kind == "ambiguous" {
		result.Decision = StateUnknown
		result.Reason = strings.ToUpper(strings.ReplaceAll(item.Kind, "-", "_")) + "_HTTP_EVIDENCE"
		result.Unknown = &Unknown{Stage: "CASE", Step: "READ_HTTP_EVIDENCE", Reason: result.Reason, UnknownClass: strings.ToUpper(strings.ReplaceAll(item.Kind, "-", "_")), NextOperation: "RESTORE_CASE_EVIDENCE", BlockedBy: []string{"case:" + declaration.ID}}
		return result
	}
	if item.ObservedCoordinate == nil || *item.ObservedCoordinate != item.ExpectedCoordinate {
		result.Decision = StateRefuted
		result.Reason = "COORDINATE_CONTRADICTION"
		result.Counterexample = &Counterexample{Kind: "COORDINATE_CONTRADICTION", Field: "coordinate", Expected: item.ExpectedCoordinate, Observed: pointerValue(item.ObservedCoordinate)}
		return result
	}
	if item.ObservedDigest == nil || *item.ObservedDigest != item.ExpectedDigest {
		result.Decision = StateRefuted
		result.Reason = "KNOWN_DIGEST_MISMATCH"
		result.Counterexample = &Counterexample{Kind: "KNOWN_DIGEST_MISMATCH", Field: "digest", Expected: item.ExpectedDigest, Observed: pointerValue(item.ObservedDigest)}
		return result
	}
	result.Decision = StateClosed
	result.Reason = "CASE_EVIDENCE_EXACT"
	return result
}

func Distribution(results []CaseResult) map[string]int {
	distribution := map[string]int{StateClosed: 0, StateUnknown: 0, StateRefuted: 0}
	for _, result := range results {
		distribution[result.Decision]++
	}
	return distribution
}

func FinalVerdict(evidence []LockEvidence) string {
	return aggregateEvidence(evidence)
}

func HTTPStatusName(status int) string {
	if status == 0 {
		return "timeout"
	}
	return strconv.Itoa(status)
}
