#!/usr/bin/env bash
set -euo pipefail

repo_root="$(pwd)"
evidence_dir="${RUNNER_TEMP}/gooo-deterministic-proof-fetch-scheduler"
mkdir -p "${evidence_dir}"
stages_file="${evidence_dir}/stages.ndjson"
: > "${stages_file}"

wall_ms() {
  awk '{ printf "%d", ($1 * 1000) + 0.5 }' "$1"
}

measure_stage() {
  stage_name="$1"
  shift
  timing_file="${evidence_dir}/${stage_name}.time"
  /usr/bin/time -f '%e %M' -o "${timing_file}" "$@"
  read -r seconds rss_kib < "${timing_file}"
  jq -cn --arg stage "${stage_name}" --argjson wall "$(wall_ms "${timing_file}")" --argjson rss "${rss_kib}" '{stage:$stage,wall_ms:$wall,peak_rss_kib:$rss}' >> "${stages_file}"
}

measure_stage build go build ./...

run_tests() {
  go test -json ./... > "${evidence_dir}/go-test.json"
}
measure_stage test run_tests

measure_stage vet go vet ./...

measure_stage format bash -c 'test -z "$(gofmt -d $(git ls-files "*.go"))"'

measure_stage semantic-audit bash scripts/semantic-audit.sh

measure_stage compile go run ./cmd/gooo-deterministic-proof-fetch-scheduler compile \
  --root "${repo_root}" \
  --source examples/proof-fetch-scheduler/main.gooo \
  --contract contracts/scheduler-v1.json \
  --fixture fixtures/fixed-lock-fixture.json \
  --output-dir "${evidence_dir}/compile"

measure_stage integration go run ./cmd/gooo-deterministic-proof-fetch-scheduler integration \
  --root "${repo_root}" \
  --source examples/proof-fetch-scheduler/main.gooo \
  --contract contracts/scheduler-v1.json \
  --fixture fixtures/fixed-lock-fixture.json \
  --output-dir "${evidence_dir}/integration"

test_count="$(jq -s '[.[] | select(.Action == "pass" and (.Test // "") != "")] | length' "${evidence_dir}/go-test.json")"
jq -n --slurpfile stages "${stages_file}" --argjson total "${test_count}" '{stages:$stages,tests:{total:$total,selected:$total,executed:$total,reused:0,failed:0,unknown:0}}' > "${evidence_dir}/ci-metrics.json"

measure_stage conformance go run ./cmd/gooo-deterministic-proof-fetch-scheduler conformance \
  --root "${repo_root}" \
  --source examples/proof-fetch-scheduler/main.gooo \
  --contract contracts/scheduler-v1.json \
  --fixture fixtures/fixed-lock-fixture.json \
  --cases-fixture fixtures/canonical-cases.json \
  --ci-metrics "${evidence_dir}/ci-metrics.json" \
  --output-dir "${evidence_dir}/conformance"

jq -n --slurpfile stages "${stages_file}" --argjson total "${test_count}" '{stages:$stages,tests:{total:$total,selected:$total,executed:$total,reused:0,failed:0,unknown:0}}' > "${evidence_dir}/ci-metrics-final.json"

jq -e '.decision == "CLOSED" and .case_distribution.CLOSED == 3 and .case_distribution.UNKNOWN == 3 and .case_distribution.REFUTED == 3 and .exact_pair.all_critical_fields_exact == true and .authority.repository_writes == 0 and .authority.cross_project_required_gates == 0' "${evidence_dir}/conformance/conformance-report.json" >/dev/null
cp "${evidence_dir}/conformance/human-report.md" "${evidence_dir}/human-report.md"
cat "${evidence_dir}/conformance/human-report.md" >> "${GITHUB_STEP_SUMMARY}"
echo "${evidence_dir}"
