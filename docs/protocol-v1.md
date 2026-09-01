# Protocol v1

## Authority

`main.gooo` is the only semantic authority. Every lock has one declared ID,
immutable coordinate, expected digest, behavior, latency, and dependency list.
The canonical order is lexicographic lock ID order and is used only for
emission and comparison; scheduling may complete work in any order within the
declared bound.

The contract fixes the denominator and artifact names. It is deliberately
validated as a transport identity, so changing a semantic rule in JSON cannot
silently change evaluation.

## Evidence state machine

Each fetch returns one of:

- `CLOSED`: HTTP evidence is present and coordinate plus digest match.
- `UNKNOWN`: evidence is missing or not safely interpretable. The digest is
  null and the record carries `stage`, `step`, `reason`, `unknown_class`,
  `next_operation`, and `blocked_by`.
- `REFUTED`: known evidence contradicts an immutable coordinate/digest, or a
  pair comparison exposes a race/semantic difference. A counterexample is
  required.

Aggregate state uses `REFUTED > UNKNOWN > CLOSED`. The scheduler claim is
separate from the aggregate lock verdict: a pair can be `CLOSED` as an exact
safe scheduling equivalence while the fixed fixture still correctly reports a
refuted lock.

## Exact pair

The baseline and candidate share the fixture server and pinned inputs in one
job. The comparison checks status, digest, UNKNOWN six-field tuples,
REFUTED counterexamples, and the canonical semantic root. Performance is
closed only when candidate wall time is strictly lower. RSS and all request
counters remain integer measurements and are never used to infer a semantic
success.
