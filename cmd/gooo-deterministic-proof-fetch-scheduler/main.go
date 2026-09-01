package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kimjooyoon/gooo-deterministic-proof-fetch-scheduler/internal/scheduler"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "compile":
		err = compile(os.Args[2:])
	case "integration":
		err = integration(os.Args[2:])
	case "conformance", "measure":
		err = conformance(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: gooo-deterministic-proof-fetch-scheduler <compile|integration|conformance> [flags]")
}

type commonFlags struct {
	root         string
	source       string
	contract     string
	fixture      string
	casesFixture string
	outputDir    string
	ciMetrics    string
}

func addCommonFlags(set *flag.FlagSet) *commonFlags {
	values := &commonFlags{}
	set.StringVar(&values.root, "root", ".", "repository root used for read-only inventory")
	set.StringVar(&values.source, "source", "examples/proof-fetch-scheduler/main.gooo", "authoritative .gooo source")
	set.StringVar(&values.contract, "contract", "contracts/scheduler-v1.json", "transport contract")
	set.StringVar(&values.fixture, "fixture", "fixtures/fixed-lock-fixture.json", "fixed local lock fixture")
	set.StringVar(&values.casesFixture, "cases-fixture", "fixtures/canonical-cases.json", "fixed canonical case fixture")
	set.StringVar(&values.outputDir, "output-dir", "", "absolute caller-owned output directory")
	set.StringVar(&values.ciMetrics, "ci-metrics", "", "optional CI stage metrics JSON")
	return values
}

func compile(args []string) error {
	set := flag.NewFlagSet("compile", flag.ContinueOnError)
	values := addCommonFlags(set)
	if err := set.Parse(args); err != nil {
		return err
	}
	if !filepath.IsAbs(values.outputDir) {
		return fmt.Errorf("--output-dir must be an absolute caller-owned path")
	}
	source, contract, contractDigest, _, _, err := loadInputs(*values)
	if err != nil {
		return err
	}
	if err := scheduler.Validate(source, contract); err != nil {
		return err
	}
	if err := os.MkdirAll(values.outputDir, 0o755); err != nil {
		return err
	}
	semanticIR := struct {
		Schema         string           `json:"schema"`
		SourceDigest   string           `json:"source_digest"`
		ContractDigest string           `json:"contract_digest"`
		Concurrency    int              `json:"concurrency_bound"`
		CanonicalOrder []string         `json:"canonical_order"`
		Locks          []scheduler.Lock `json:"locks"`
	}{scheduler.Schema, source.SourceDigest, contractDigest, source.ConcurrencyBound, source.CanonicalOrder, source.Locks}
	if err := writeJSON(filepath.Join(values.outputDir, "semantic-ir.json"), semanticIR); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(values.outputDir, "generated-worker.go"), scheduler.GenerateWorker(source), 0o644); err != nil {
		return err
	}
	return printJSON(map[string]any{"decision": scheduler.StateClosed, "source_digest": source.SourceDigest, "contract_digest": contractDigest})
}

func integration(args []string) error {
	set := flag.NewFlagSet("integration", flag.ContinueOnError)
	values := addCommonFlags(set)
	if err := set.Parse(args); err != nil {
		return err
	}
	source, contract, contractDigest, fixture, fixtureDigest, err := loadInputs(*values)
	if err != nil {
		return err
	}
	if err := scheduler.Validate(source, contract); err != nil {
		return err
	}
	started := time.Now()
	baseline, candidate, comparison, err := scheduler.RunPair(source, fixture)
	if err != nil {
		return err
	}
	result := map[string]any{
		"schema": scheduler.Schema, "contract_id": scheduler.ContractID,
		"source_digest": source.SourceDigest, "contract_digest": contractDigest, "fixture_digest": fixtureDigest,
		"decision": comparison.Verdict, "reason": comparison.Reason, "baseline": baseline.Metrics,
		"candidate": candidate.Metrics, "exact_pair": comparison.AllCriticalFieldsExact,
		"wall_ms": time.Since(started).Milliseconds(), "public_network_required": false,
	}
	if values.outputDir != "" {
		if !filepath.IsAbs(values.outputDir) {
			return fmt.Errorf("--output-dir must be an absolute caller-owned path")
		}
		if err := os.MkdirAll(values.outputDir, 0o755); err != nil {
			return err
		}
		if err := writeJSON(filepath.Join(values.outputDir, "integration.json"), result); err != nil {
			return err
		}
	}
	return printJSON(result)
}

func conformance(args []string) error {
	set := flag.NewFlagSet("conformance", flag.ContinueOnError)
	values := addCommonFlags(set)
	if err := set.Parse(args); err != nil {
		return err
	}
	if !filepath.IsAbs(values.outputDir) {
		return fmt.Errorf("--output-dir must be an absolute caller-owned path")
	}
	source, contract, contractDigest, fixture, fixtureDigest, err := loadInputs(*values)
	if err != nil {
		return err
	}
	casesFixture, casesDigest, err := scheduler.LoadCasesFixture(values.casesFixture, source)
	if err != nil {
		return err
	}
	if err := scheduler.Validate(source, contract); err != nil {
		return err
	}
	started := time.Now()
	baseline, candidate, comparison, err := scheduler.RunPair(source, fixture)
	if err != nil {
		return err
	}
	replay, err := scheduler.ReplayPair(source, fixture)
	if err != nil {
		return err
	}
	cases, err := scheduler.EvaluateCanonicalCases(source, casesFixture)
	if err != nil {
		return err
	}
	ci, err := scheduler.ReadCIMetrics(values.ciMetrics)
	if err != nil {
		return err
	}
	ci.Stages = append(ci.Stages, scheduler.StageMetric{Stage: "conformance", WallMS: time.Since(started).Milliseconds(), PeakRSSKib: scheduler.CurrentPeakRSSKib()})
	if ci.Tests.Total == 0 {
		ci.Tests.Total = len(source.Cases)
		ci.Tests.Selected = len(source.Cases)
		ci.Tests.Executed = len(source.Cases)
	}
	report, err := scheduler.WriteOutputs(values.root, values.outputDir, source, contract, contractDigest, fixtureDigest, casesDigest, cases, baseline, candidate, comparison, replay, ci)
	if err != nil {
		return err
	}
	return printJSON(report)
}

func loadInputs(values commonFlags) (scheduler.Source, scheduler.Contract, string, scheduler.Fixture, string, error) {
	source, err := scheduler.ParseSource(values.source)
	if err != nil {
		return scheduler.Source{}, scheduler.Contract{}, "", scheduler.Fixture{}, "", err
	}
	contract, contractDigest, err := scheduler.LoadContract(values.contract)
	if err != nil {
		return scheduler.Source{}, scheduler.Contract{}, "", scheduler.Fixture{}, "", err
	}
	fixture, fixtureDigest, err := scheduler.LoadFixture(values.fixture, source)
	if err != nil {
		return scheduler.Source{}, scheduler.Contract{}, "", scheduler.Fixture{}, "", err
	}
	return source, contract, contractDigest, fixture, fixtureDigest, nil
}

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func printJSON(value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Println(string(raw))
	return err
}
