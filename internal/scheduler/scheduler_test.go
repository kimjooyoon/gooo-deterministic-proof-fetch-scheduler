package scheduler

import (
	"fmt"
	"testing"
)

func TestUnknownEvidenceHasNullDigestAndSixFields(t *testing.T) {
	result := evaluateCase(CaseDecl{Ordinal: 1, ID: "unknown", Expected: StateUnknown}, CaseFixture{ID: "unknown", Kind: "timeout", HTTPStatus: 0})
	if result.Decision != StateUnknown || result.Unknown == nil || !result.Unknown.Complete() {
		t.Fatalf("expected complete UNKNOWN tuple, got %#v", result)
	}
	if result.Counterexample != nil {
		t.Fatal("UNKNOWN must not carry a REFUTED counterexample")
	}
}

func TestPairDifferenceIsRefuted(t *testing.T) {
	digest := "sha256:1"
	left := RunResult{Evidence: []LockEvidence{{LockID: "lock-001", Status: StateClosed, Digest: &digest}}, SemanticRoot: "sha256:left"}
	right := RunResult{Evidence: []LockEvidence{{LockID: "lock-001", Status: StateUnknown}}, SemanticRoot: "sha256:right"}
	comparison := ComparePair(left, right)
	if comparison.Verdict != StateRefuted || comparison.Counterexample == nil {
		t.Fatalf("expected fail-closed REFUTED comparison, got %#v", comparison)
	}
}

func TestPrecedence(t *testing.T) {
	evidence := []LockEvidence{{LockID: "lock-001", Status: StateClosed}, {LockID: "lock-002", Status: StateUnknown}, {LockID: "lock-003", Status: StateRefuted}}
	if got := FinalVerdict(evidence); got != StateRefuted {
		t.Fatalf("expected REFUTED precedence, got %s", got)
	}
}

func TestValidateSourceRejectsNonCanonicalLockOrder(t *testing.T) {
	source := Source{
		Schema: Schema, Version: "v1", ConcurrencyBound: 1,
		Retry: RetryPolicy{MaxAttempts: 1},
		CanonicalOrder: make([]string, 53), Locks: make([]Lock, 53), Cases: make([]CaseDecl, 9),
		Guardrail: Guardrail{OperationalRefuted: true, FailureDeletesForbidden: true},
	}
	contract := Contract{Schema: Schema, ContractID: ContractID, Version: "v1", FixedLockCount: 53, FixedCaseCount: 9}
	for index := range source.Locks {
		id := fmt.Sprintf("lock-%03d", index)
		source.Locks[index] = Lock{ID: id, Coordinate: id, Digest: "digest"}
		source.CanonicalOrder[index] = id
	}
	source.CanonicalOrder[52] = source.CanonicalOrder[51]

	if err := validateSource(source, contract); err == nil {
		t.Fatal("expected duplicate canonical order to be rejected")
	}
}
