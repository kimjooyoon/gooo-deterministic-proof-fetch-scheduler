package scheduler

import "testing"

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
