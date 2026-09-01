package scheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type CIMetrics struct {
	Stages []StageMetric `json:"stages"`
	Tests  TestMetrics   `json:"tests"`
}

func WriteOutputs(root, outputDir string, source Source, contract Contract, contractDigest, fixtureDigest, casesDigest string, cases []CaseResult, baseline, candidate RunResult, comparison PairComparison, replay ReplayReceipt, ci CIMetrics) (ConformanceReport, error) {
	if !filepath.IsAbs(outputDir) {
		return ConformanceReport{}, fmt.Errorf("output directory must be an absolute caller-owned path")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return ConformanceReport{}, err
	}
	schedule := ScheduleReceipt{
		Schema: Schema, ContractID: ContractID, SourceDigest: source.SourceDigest, ContractDigest: contractDigest,
		FixtureDigest: fixtureDigest, ConcurrencyBound: source.ConcurrencyBound, RetryPolicy: source.Retry,
		CanonicalOrder: append([]string(nil), source.CanonicalOrder...), DependencyEdges: dependencyEdges(source),
		Baseline: baseline.Metrics, Candidate: candidate.Metrics, CandidateRoot: candidate.SemanticRoot,
	}
	if err := writeJSON(filepath.Join(outputDir, "schedule.json"), schedule); err != nil {
		return ConformanceReport{}, err
	}
	if err := writeNDJSON(filepath.Join(outputDir, "per-lock-evidence.ndjson"), candidate.Evidence); err != nil {
		return ConformanceReport{}, err
	}
	if err := writeJSON(filepath.Join(outputDir, "replay-receipt.json"), replay); err != nil {
		return ConformanceReport{}, err
	}
	if err := writeJSON(filepath.Join(outputDir, "exact-pair-comparison.json"), comparison); err != nil {
		return ConformanceReport{}, err
	}
	if err := os.WriteFile(filepath.Join(outputDir, "generated-worker.go"), GenerateWorker(source), 0o644); err != nil {
		return ConformanceReport{}, err
	}
	if err := writeJSON(filepath.Join(outputDir, "canonical-cases.json"), cases); err != nil {
		return ConformanceReport{}, err
	}
	outputDigests, err := digestOutputs(outputDir, ArtifactFiles[:len(ArtifactFiles)-1])
	if err != nil {
		return ConformanceReport{}, err
	}
	if err := os.WriteFile(filepath.Join(outputDir, "human-report.md"), []byte(renderHumanReport(source, contract, contractDigest, fixtureDigest, casesDigest, cases, baseline, candidate, comparison, replay, outputDigests)), 0o644); err != nil {
		return ConformanceReport{}, err
	}
	outputDigests, err = digestOutputs(outputDir, ArtifactFiles)
	if err != nil {
		return ConformanceReport{}, err
	}
	inventory, err := MeasureInventory(root, outputDir)
	if err != nil {
		return ConformanceReport{}, err
	}
	hasConformanceStage := false
	for _, stage := range ci.Stages {
		if stage.Stage == "conformance" {
			hasConformanceStage = true
			break
		}
	}
	if !hasConformanceStage {
		ci.Stages = append(ci.Stages, StageMetric{Stage: "conformance", WallMS: 0, PeakRSSKib: peakRSSKib()})
	}
	decision := StateUnknown
	reason := "CONFORMANCE_NOT_CLOSED"
	if comparison.Verdict == StateClosed && replay.Deterministic && Distribution(cases)[StateClosed] == 3 && Distribution(cases)[StateUnknown] == 3 && Distribution(cases)[StateRefuted] == 3 {
		decision = StateClosed
		reason = "FIXED_9_CASES_AND_EXACT_53_LOCK_PAIR"
	}
	report := ConformanceReport{
		Schema: Schema, ContractID: ContractID, SourceDigest: source.SourceDigest, ContractDigest: contractDigest, FixtureDigest: fixtureDigest, CasesFixtureDigest: casesDigest,
		Cases: cases, CaseDistribution: Distribution(cases), ExactPair: comparison, Stages: ci.Stages, Tests: ci.Tests,
		Inventory: inventory, Outputs: outputDigests, Authority: source.Authority, RuntimeAuthority: source.Guardrail.RuntimeAuthority,
		Decision: decision, Reason: reason,
	}
	if err := writeJSON(filepath.Join(outputDir, "conformance-report.json"), report); err != nil {
		return ConformanceReport{}, err
	}
	return report, nil
}

func dependencyEdges(source Source) []DependencyEdge {
	result := []DependencyEdge{}
	for _, lock := range source.Locks {
		for _, dependency := range lock.Dependencies {
			result = append(result, DependencyEdge{From: lock.ID, To: dependency})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].From == result[j].From {
			return result[i].To < result[j].To
		}
		return result[i].From < result[j].From
	})
	return result
}

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func writeNDJSON(path string, evidence []LockEvidence) error {
	ordered := append([]LockEvidence(nil), evidence...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].LockID < ordered[j].LockID })
	var buffer bytes.Buffer
	for _, item := range ordered {
		raw, err := json.Marshal(item)
		if err != nil {
			return err
		}
		buffer.Write(raw)
		buffer.WriteByte('\n')
	}
	return os.WriteFile(path, buffer.Bytes(), 0o644)
}

func digestOutputs(outputDir string, names []string) ([]OutputDigest, error) {
	result := make([]OutputDigest, 0, len(names))
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(outputDir, name))
		if err != nil {
			return nil, err
		}
		result = append(result, OutputDigest{Name: name, SHA256: DigestBytes(raw), Bytes: int64(len(raw))})
	}
	return result, nil
}

func MeasureInventory(root, outputDir string) (Inventory, error) {
	var inventory Inventory
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return fs.SkipDir
			}
			if path != root {
				inventory.Directories++
			}
			return nil
		}
		if path == filepath.Join(root, "README.md") {
			inventory.RootReadmeExcluded = true
			return nil
		}
		inventory.Files++
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		lines := physicalLines(raw)
		switch filepath.Ext(path) {
		case ".go":
			inventory.GoFiles++
			inventory.GoPhysicalLines += lines
		case ".gooo":
			inventory.GoooFiles++
			inventory.GoooPhysicalLines += lines
		}
		return nil
	}); err != nil {
		return Inventory{}, err
	}
	for _, name := range ArtifactFiles {
		if raw, err := os.ReadFile(filepath.Join(outputDir, name)); err == nil {
			inventory.GeneratedArtifacts++
			inventory.GeneratedBytes += int64(len(raw))
		}
	}
	return inventory, nil
}

func physicalLines(raw []byte) int {
	if len(raw) == 0 {
		return 0
	}
	count := bytes.Count(raw, []byte{'\n'})
	if raw[len(raw)-1] != '\n' {
		count++
	}
	return count
}

func maxStageWall(stages []StageMetric) int64 {
	var result int64
	for _, stage := range stages {
		if stage.WallMS > result {
			result = stage.WallMS
		}
	}
	return result
}

func renderHumanReport(source Source, contract Contract, contractDigest, fixtureDigest, casesDigest string, cases []CaseResult, baseline, candidate RunResult, comparison PairComparison, replay ReplayReceipt, outputs []OutputDigest) string {
	var builder strings.Builder
	builder.WriteString("# Deterministic proof-fetch scheduler evidence\n\n")
	builder.WriteString("The authoritative semantic input is `examples/proof-fetch-scheduler/main.gooo`. Go parses, schedules, fetches the deterministic local fixture, generates the worker, and evaluates evidence. The JSON contract is an identity and output-shape check.\n\n")
	fmt.Fprintf(&builder, "- Source digest: `%s`\n- Contract digest: `%s`\n- Fixture digest: `%s`\n- Cases fixture digest: `%s`\n- Fixed denominator: 53 locks, 9 canonical cases\n- Declared concurrency bound: %d\n- Aggregate baseline lock verdict: `%s`\n- Aggregate candidate lock verdict: `%s`\n- Scheduler pair verdict: `%s` (%s)\n- Replay: `%s` (%s)\n\n", source.SourceDigest, contractDigest, fixtureDigest, casesDigest, source.ConcurrencyBound, FinalVerdict(baseline.Evidence), FinalVerdict(candidate.Evidence), comparison.Verdict, comparison.Reason, replay.Decision, replay.Reason)
	builder.WriteString("## Exact 53-lock pair\n\n")
	builder.WriteString("| metric | sequential baseline | bounded-parallel candidate |\n|---|---:|---:|\n")
	fmt.Fprintf(&builder, "| wall_ms | %d | %d |\n| peak_rss_kib | %d | %d |\n| requests | %d | %d |\n| max_in_flight | %d | %d |\n| completed | %d | %d |\n| reused | %d | %d |\n| unknown | %d | %d |\n| refuted | %d | %d |\n\n", baseline.Metrics.WallMS, candidate.Metrics.WallMS, baseline.Metrics.PeakRSSKib, candidate.Metrics.PeakRSSKib, baseline.Metrics.Requests, candidate.Metrics.Requests, baseline.Metrics.MaxInFlight, candidate.Metrics.MaxInFlight, baseline.Metrics.Completed, candidate.Metrics.Completed, baseline.Metrics.Reused, candidate.Metrics.Reused, baseline.Metrics.Unknown, candidate.Metrics.Unknown, baseline.Metrics.Refuted, candidate.Metrics.Refuted)
	fmt.Fprintf(&builder, "Critical exactness: status=%t, digest=%t, UNKNOWN six fields=%t, REFUTED counterexamples=%t, final verdict=%t, final semantic root=%t, actual wall reduction=%t.\n\n", comparison.PerLockStatusExact, comparison.DigestExact, comparison.UnknownFieldsExact, comparison.CounterexamplesExact, comparison.FinalVerdictExact, comparison.FinalSemanticRootExact, comparison.WallReductionActual)
	builder.WriteString("## Canonical cases\n\n| ordinal | case | expected | observed | reason |\n|---:|---|---|---|---|\n")
	for _, item := range cases {
		fmt.Fprintf(&builder, "| %d | `%s` | `%s` | `%s` | `%s` |\n", item.Ordinal, item.CaseID, item.Expected, item.Decision, item.Reason)
	}
	builder.WriteString("\nState precedence is `REFUTED > UNKNOWN > CLOSED`. Missing, timeout, rate-limit, and ambiguous HTTP evidence stays `digest: null` and `UNKNOWN`; no zero or success estimate is produced. Known digest mismatch, coordinate contradiction, and race divergence carry a counterexample and are `REFUTED`.\n\n")
	builder.WriteString("## Authority and guardrails\n\n")
	builder.WriteString("All semantic and runtime authority counters are zero: repository writes, input-repository writes, local test executions, cross-project required gates, automatic commit/push/merge/tag/release. Failed runs, tags, and releases are never deleted or recreated. The optional public-release check is read-only and is not a required cross-project gate.\n\n")
	builder.WriteString("## Output digests\n\n| artifact | bytes | SHA-256 |\n|---|---:|---|\n")
	for _, output := range outputs {
		fmt.Fprintf(&builder, "| `%s` | %d | `%s` |\n", output.Name, output.Bytes, output.SHA256)
	}
	builder.WriteString("\n")
	return builder.String()
}

func ReadCIMetrics(path string) (CIMetrics, error) {
	if path == "" {
		return CIMetrics{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return CIMetrics{}, err
	}
	var metrics CIMetrics
	if err := json.Unmarshal(raw, &metrics); err != nil {
		return CIMetrics{}, err
	}
	return metrics, nil
}

func stageNow(stage string, started time.Time) StageMetric {
	return StageMetric{Stage: stage, WallMS: time.Since(started).Milliseconds(), PeakRSSKib: peakRSSKib()}
}
