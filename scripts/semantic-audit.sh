#!/usr/bin/env bash
set -euo pipefail

source_file="examples/proof-fetch-scheduler/main.gooo"
contract_file="contracts/scheduler-v1.json"

test -f "${source_file}"
test "$(rg -c '^lock ' "${source_file}")" = 53
test "$(rg -c '^case ' "${source_file}")" = 9
test "$(rg -c '^canonical ' "${source_file}")" = 1
test "$(rg -c '^concurrency ' "${source_file}")" = 1
test "$(rg -c '^retry ' "${source_file}")" = 1
jq -e '.fixed_lock_count == 53 and .fixed_case_count == 9 and .cross_project_required_gates == 0 and .unknown_field_count == 6 and .immutable_release_required == true' "${contract_file}" >/dev/null
! rg -n 'github\.com|api\.github|curl |wget |go test|go build|go vet|gofmt' "internal" "examples" "fixtures" >/dev/null
echo "semantic audit: .gooo authority, fixed denominator, bounded policy, and local-only fixture boundary present"
