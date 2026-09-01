package scheduler

import (
	"encoding/json"
	"fmt"
	"sort"
)

const (
	Schema       = "gooo/deterministic-proof-fetch-scheduler/v1"
	ContractID   = "deterministic-proof-fetch-scheduler-v1"
	StateClosed  = "CLOSED"
	StateUnknown = "UNKNOWN"
	StateRefuted = "REFUTED"
)

var ArtifactFiles = []string{
	"schedule.json",
	"per-lock-evidence.ndjson",
	"replay-receipt.json",
	"exact-pair-comparison.json",
	"generated-worker.go",
	"canonical-cases.json",
	"human-report.md",
}

type Authority struct {
	RepositoryWrites          int `json:"repository_writes"`
	InputRepositoryWrites     int `json:"input_repository_writes"`
	LocalTestExecutions       int `json:"local_test_executions"`
	CrossProjectRequiredGates int `json:"cross_project_required_gates"`
	AutomaticCommit           int `json:"automatic_commit"`
	AutomaticPush             int `json:"automatic_push"`
	AutomaticMerge            int `json:"automatic_merge"`
	AutomaticTag              int `json:"automatic_tag"`
	AutomaticRelease          int `json:"automatic_release"`
}

type Guardrail struct {
	ForbiddenLocalValidation []string `json:"forbidden_local_validation_commands"`
	OperationalRefuted       bool     `json:"operational_refuted"`
	FailureDeletesForbidden  bool     `json:"failure_deletes_forbidden"`
	RuntimeAuthority         Authority `json:"runtime_authority"`
}

type RetryPolicy struct {
	MaxAttempts       int   `json:"max_attempts"`
	BackoffMS         int   `json:"backoff_ms"`
	RetryableStatuses []int `json:"retryable_statuses"`
}

type Lock struct {
	ID           string   `json:"id"`
	Coordinate   string   `json:"coordinate"`
	Digest       string   `json:"digest"`
	Dependencies []string `json:"dependencies"`
	Behavior     string   `json:"behavior"`
	LatencyMS    int      `json:"latency_ms"`
}

type CaseDecl struct {
	Ordinal int    `json:"ordinal"`
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Expected string `json:"expected"`
}

type Source struct {
	Schema            string       `json:"schema"`
	Version           string       `json:"version"`
	Authority         Authority    `json:"authority"`
	Guardrail         Guardrail    `json:"guardrail"`
	ConcurrencyBound  int          `json:"concurrency_bound"`
	Retry             RetryPolicy  `json:"retry_policy"`
	CanonicalOrder    []string     `json:"canonical_order"`
	Locks             []Lock       `json:"locks"`
	Cases             []CaseDecl   `json:"cases"`
	SourceDigest      string       `json:"source_digest"`
}

type Contract struct {
	Schema                    string   `json:"schema"`
	ContractID                string   `json:"contract_id"`
	Version                   string   `json:"version"`
	FixedLockCount            int      `json:"fixed_lock_count"`
	FixedCaseCount            int      `json:"fixed_case_count"`
	RequiredOutputs           []string `json:"required_outputs"`
	StatePrecedence           []string `json:"state_precedence"`
	UnknownFieldCount         int      `json:"unknown_field_count"`
	GoVersion                 string   `json:"go_version"`
	CrossProjectRequiredGates int      `json:"cross_project_required_gates"`
	ImmutableReleaseRequired  bool     `json:"immutable_release_required"`
}

type FixtureRecord struct {
	LockID              string  `json:"lock_id"`
	HTTPStatus          int     `json:"http_status"`
	ObservedCoordinate  *string `json:"observed_coordinate"`
	ObservedDigest      *string `json:"observed_digest"`
	BodyMode            string  `json:"body_mode"`
}

type Fixture struct {
	Schema  string          `json:"schema"`
	Version string          `json:"version"`
	Records []FixtureRecord `json:"records"`
	Digest  string          `json:"fixture_digest,omitempty"`
}

type CaseFixture struct {
	ID                 string  `json:"id"`
	Kind               string  `json:"kind"`
	HTTPStatus         int     `json:"http_status"`
	ExpectedCoordinate string  `json:"expected_coordinate"`
	ObservedCoordinate *string `json:"observed_coordinate"`
	ExpectedDigest     string  `json:"expected_digest"`
	ObservedDigest     *string `json:"observed_digest"`
	RaceField          string  `json:"race_field"`
	RaceBaseline       string  `json:"race_baseline"`
	RaceCandidate      string  `json:"race_candidate"`
}

type CasesFixture struct {
	Schema  string         `json:"schema"`
	Version string         `json:"version"`
	Cases   []CaseFixture  `json:"cases"`
	Digest  string         `json:"cases_digest,omitempty"`
}

type Unknown struct {
	Stage         string   `json:"stage"`
	Step          string   `json:"step"`
	Reason        string   `json:"reason"`
	UnknownClass  string   `json:"unknown_class"`
	NextOperation string   `json:"next_operation"`
	BlockedBy     []string `json:"blocked_by"`
}

func (u Unknown) Complete() bool {
	return u.Stage != "" && u.Step != "" && u.Reason != "" && u.UnknownClass != "" && u.NextOperation != "" && len(u.BlockedBy) > 0
}

type Counterexample struct {
	Kind     string `json:"kind"`
	Field    string `json:"field"`
	Expected string `json:"expected"`
	Observed string `json:"observed"`
}

type LockEvidence struct {
	LockID          string         `json:"lock_id"`
	Coordinate      string         `json:"coordinate"`
	Status          string         `json:"status"`
	Digest          *string        `json:"digest"`
	Attempts        int            `json:"attempts"`
	RequestCount    int            `json:"request_count"`
	Reused          bool           `json:"reused"`
	Unknown         *Unknown       `json:"unknown"`
	Counterexample  *Counterexample `json:"counterexample"`
}

type Metrics struct {
	WallMS       int64 `json:"wall_ms"`
	PeakRSSKib   int64 `json:"peak_rss_kib"`
	Requests     int   `json:"requests"`
	MaxInFlight  int   `json:"max_in_flight"`
	Completed    int   `json:"completed"`
	Reused       int   `json:"reused"`
	Unknown      int   `json:"unknown"`
	Refuted      int   `json:"refuted"`
}

type RunResult struct {
	Mode          string         `json:"mode"`
	Evidence      []LockEvidence `json:"evidence"`
	Metrics       Metrics        `json:"metrics"`
	SemanticRoot  string         `json:"semantic_root"`
	Schedule      []ScheduleItem `json:"schedule"`
}

type ScheduleItem struct {
	Ordinal      int      `json:"ordinal"`
	LockID       string   `json:"lock_id"`
	Dependencies []string `json:"dependencies"`
	Wave         int      `json:"wave"`
}

type ScheduleReceipt struct {
	Schema           string         `json:"schema"`
	ContractID       string         `json:"contract_id"`
	SourceDigest     string         `json:"source_digest"`
	ContractDigest   string         `json:"contract_digest"`
	FixtureDigest    string         `json:"fixture_digest"`
	ConcurrencyBound int            `json:"concurrency_bound"`
	RetryPolicy      RetryPolicy    `json:"retry_policy"`
	CanonicalOrder   []string       `json:"canonical_order"`
	DependencyEdges  []DependencyEdge `json:"dependency_edges"`
	Baseline         Metrics        `json:"baseline"`
	Candidate        Metrics        `json:"candidate"`
	CandidateRoot    string         `json:"candidate_semantic_root"`
}

type DependencyEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type PairComparison struct {
	Schema                   string          `json:"schema"`
	Baseline                 Metrics         `json:"baseline"`
	Candidate                Metrics        `json:"candidate"`
	BaselineFinalVerdict     string          `json:"baseline_final_verdict"`
	CandidateFinalVerdict    string          `json:"candidate_final_verdict"`
	FinalVerdictExact        bool            `json:"final_verdict_exact"`
	Verdict                  string          `json:"verdict"`
	Reason                   string          `json:"reason"`
	PerLockStatusExact       bool            `json:"per_lock_status_exact"`
	DigestExact              bool            `json:"digest_exact"`
	UnknownFieldsExact       bool            `json:"unknown_fields_exact"`
	CounterexamplesExact     bool            `json:"counterexamples_exact"`
	FinalSemanticRootExact   bool            `json:"final_semantic_root_exact"`
	AllCriticalFieldsExact   bool            `json:"all_critical_fields_exact"`
	WallReductionActual      bool            `json:"wall_reduction_actual"`
	BaselineSemanticRoot     string          `json:"baseline_semantic_root"`
	CandidateSemanticRoot    string          `json:"candidate_semantic_root"`
	Counterexample           *Counterexample `json:"counterexample"`
	Unknown                  *Unknown        `json:"unknown"`
}

type ReplayReceipt struct {
	Schema                 string `json:"schema"`
	ContractID             string `json:"contract_id"`
	FirstSemanticRoot      string `json:"first_semantic_root"`
	SecondSemanticRoot     string `json:"second_semantic_root"`
	Deterministic          bool   `json:"deterministic"`
	ReplayCount            int    `json:"replay_count"`
	Decision               string `json:"decision"`
	Reason                 string `json:"reason"`
}

type CaseResult struct {
	Ordinal        int            `json:"ordinal"`
	CaseID         string         `json:"case_id"`
	Expected       string         `json:"expected"`
	Decision       string         `json:"decision"`
	Reason         string         `json:"reason"`
	Unknown        *Unknown       `json:"unknown"`
	Counterexample *Counterexample `json:"counterexample"`
}

type StageMetric struct {
	Stage       string `json:"stage"`
	WallMS      int64  `json:"wall_ms"`
	PeakRSSKib  int64  `json:"peak_rss_kib"`
}

type TestMetrics struct {
	Total    int `json:"total"`
	Selected int `json:"selected"`
	Executed int `json:"executed"`
	Reused   int `json:"reused"`
	Failed   int `json:"failed"`
	Unknown  int `json:"unknown"`
}

type Inventory struct {
	GoFiles             int `json:"go_files"`
	GoooFiles           int `json:"gooo_files"`
	GoPhysicalLines     int `json:"go_physical_lines"`
	GoooPhysicalLines   int `json:"gooo_physical_lines"`
	Directories         int `json:"directories"`
	Files               int `json:"files"`
	GeneratedArtifacts  int `json:"generated_artifacts"`
	GeneratedBytes      int64 `json:"generated_bytes"`
	RootReadmeExcluded  bool `json:"root_readme_excluded"`
}

type OutputDigest struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type ConformanceReport struct {
	Schema          string          `json:"schema"`
	ContractID      string          `json:"contract_id"`
	SourceDigest    string          `json:"source_digest"`
	ContractDigest  string          `json:"contract_digest"`
	FixtureDigest   string          `json:"fixture_digest"`
	CasesFixtureDigest string       `json:"cases_fixture_digest"`
	Cases           []CaseResult    `json:"cases"`
	CaseDistribution map[string]int `json:"case_distribution"`
	ExactPair       PairComparison  `json:"exact_pair"`
	Stages          []StageMetric   `json:"stages"`
	Tests           TestMetrics     `json:"tests"`
	Inventory       Inventory       `json:"inventory"`
	Outputs         []OutputDigest  `json:"outputs"`
	Authority       Authority       `json:"authority"`
	RuntimeAuthority Authority      `json:"runtime_authority"`
	Decision        string          `json:"decision"`
	Reason          string          `json:"reason"`
}

func precedence(status string) int {
	switch status {
	case StateRefuted:
		return 3
	case StateUnknown:
		return 2
	case StateClosed:
		return 1
	default:
		return 4
	}
}

func aggregateEvidence(evidence []LockEvidence) string {
	decision := StateClosed
	for _, item := range evidence {
		if precedence(item.Status) > precedence(decision) {
			decision = item.Status
		}
	}
	return decision
}

func canonicalEvidence(evidence []LockEvidence) ([]byte, error) {
	ordered := append([]LockEvidence(nil), evidence...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].LockID < ordered[j].LockID })
	return json.Marshal(ordered)
}

func validateSource(source Source, contract Contract) error {
	if source.Schema != Schema || source.Version != "v1" {
		return fmt.Errorf("invalid source schema or version")
	}
	if contract.Schema != Schema || contract.ContractID != ContractID || contract.Version != "v1" {
		return fmt.Errorf("invalid contract identity")
	}
	if len(source.Locks) != contract.FixedLockCount || len(source.Locks) != 53 {
		return fmt.Errorf("expected fixed 53-lock denominator, got %d", len(source.Locks))
	}
	if len(source.Cases) != contract.FixedCaseCount || len(source.Cases) != 9 {
		return fmt.Errorf("expected fixed 9-case denominator, got %d", len(source.Cases))
	}
	if source.ConcurrencyBound < 1 || source.ConcurrencyBound > 64 {
		return fmt.Errorf("concurrency bound must be bounded")
	}
	if source.Retry.MaxAttempts < 1 || source.Retry.MaxAttempts > 3 {
		return fmt.Errorf("retry policy is outside bounded range")
	}
	if len(source.CanonicalOrder) != len(source.Locks) {
		return fmt.Errorf("canonical order must contain every lock exactly once")
	}
	seen := make(map[string]bool, len(source.Locks))
	for _, lock := range source.Locks {
		if seen[lock.ID] {
			return fmt.Errorf("duplicate lock authority %q", lock.ID)
		}
		seen[lock.ID] = true
		if lock.ID == "" || lock.Coordinate == "" || lock.Digest == "" || lock.LatencyMS < 0 {
			return fmt.Errorf("incomplete lock declaration %q", lock.ID)
		}
	}
	for _, id := range source.CanonicalOrder {
		if !seen[id] {
			return fmt.Errorf("canonical order references undeclared lock %q", id)
		}
	}
	if source.Authority.RepositoryWrites != 0 || source.Authority.InputRepositoryWrites != 0 || source.Authority.LocalTestExecutions != 0 || source.Authority.CrossProjectRequiredGates != 0 || source.Authority.AutomaticCommit != 0 || source.Authority.AutomaticPush != 0 || source.Authority.AutomaticMerge != 0 || source.Authority.AutomaticTag != 0 || source.Authority.AutomaticRelease != 0 {
		return fmt.Errorf("semantic runtime authority must be zero")
	}
	if !source.Guardrail.OperationalRefuted || !source.Guardrail.FailureDeletesForbidden {
		return fmt.Errorf("operational guardrails are incomplete")
	}
	return nil
}
