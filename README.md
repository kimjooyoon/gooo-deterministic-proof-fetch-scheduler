# Deterministic proof-fetch scheduler

This repository is a narrow, falsifiable implementation of bounded-parallel
fetching for a shared immutable proof-lock ledger. The semantic authority is
`examples/proof-fetch-scheduler/main.gooo`: it declares the 53-lock DAG,
immutable coordinates and digests, dependency edges, retry policy, concurrency
bound, canonical lock-ID order, authority counters, and operational guardrails.
Go is the parser, scheduler, local fixture evaluator, generator, and evidence
writer. The JSON contract carries identity and output-shape requirements; it
does not redefine the `.gooo` meaning.

## What is proven

The candidate uses at most eight in-flight local HTTP requests and emits
evidence in fixed `lock-001` through `lock-053` order regardless of completion
order. The sequential baseline and bounded-parallel candidate run against the
same source, contract, 53-record fixture, local fixture server, retry policy,
and Go job. `CLOSED` requires exact per-lock status, digest, all six UNKNOWN
fields, all REFUTED counterexamples, final semantic root, and an actual lower
candidate `wall_ms`. A race or semantic difference is `REFUTED`; missing,
timeout, rate-limit, and ambiguous HTTP evidence is `digest: null` plus a
complete `UNKNOWN` tuple. The precedence is `REFUTED > UNKNOWN > CLOSED`.

The fixed case denominator is nine cases: three `CLOSED`, three `UNKNOWN`, and
three `REFUTED`. The 53-lock fixture intentionally contains reusable locks,
missing evidence, timeout, rate limit, and a known digest mismatch. It is
served by an in-process deterministic HTTP server, so conformance does not
make 106 public requests. One optional read-only public-release check is
available after publishing; the required cross-project gate remains zero.

## Commands

All output paths must be absolute caller-owned directories. These commands do
not modify the input repository.

```sh
go run ./cmd/gooo-deterministic-proof-fetch-scheduler compile \
  --source examples/proof-fetch-scheduler/main.gooo \
  --contract contracts/scheduler-v1.json \
  --output-dir /tmp/gooo-scheduler-compile

go run ./cmd/gooo-deterministic-proof-fetch-scheduler conformance \
  --root . \
  --source examples/proof-fetch-scheduler/main.gooo \
  --contract contracts/scheduler-v1.json \
  --fixture fixtures/fixed-lock-fixture.json \
  --cases-fixture fixtures/canonical-cases.json \
  --output-dir /tmp/gooo-scheduler-conformance
```

The conformance output contains `schedule.json`, canonical per-lock evidence
NDJSON, a replay receipt, exact-pair comparison, generated worker source,
canonical case results, `conformance-report.json`, and a human report. The
report also records stage wall time and peak RSS, test totals, selected,
executed, reused, failed, and unknown counts, Go/.gooo physical lines,
directories/files, generated artifact count/bytes, and all zero runtime
authority counters.

## Delivery boundary

GitHub Actions is the only validation and release authority. The workflow uses
Go 1.27, runs the stage evidence script, uploads the exact output directory,
and refuses to delete or recreate failed runs, tags, or releases. The release
workflow verifies an annotated tag points to the exact main commit, builds a
draft release with evidence assets, publishes it once, and checks the immutable
release response.
