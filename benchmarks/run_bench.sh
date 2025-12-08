#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${REPO_ROOT}/bin"
DATA_REL="${1:-fixtures/ecommerce_1m.csv}"
DATA_PATH="${REPO_ROOT}/${DATA_REL}"

if [[ ! -f "${DATA_PATH}" ]]; then
  echo "error: dataset ${DATA_REL} not found. Generate it via scripts/gen_ecommerce_fixture.sh" >&2
  exit 1
fi

if ! command -v duckdb >/dev/null 2>&1; then
  echo "error: duckdb CLI not found in PATH." >&2
  exit 1
fi

mkdir -p "${BIN_DIR}"
GOCACHE="${GOCACHE:-${REPO_ROOT}/.gocache}" go build -o "${BIN_DIR}/sieswi" ./cmd/sieswi

RESULTS_DIR="${REPO_ROOT}/benchmarks/results"
mkdir -p "${RESULTS_DIR}"
RESULT_FILE="${RESULTS_DIR}/latest.txt"
echo "# Benchmark $(date -u +"%Y-%m-%dT%H:%M:%SZ")" > "${RESULT_FILE}"
echo "# Dataset: ${DATA_REL}" >> "${RESULT_FILE}"
echo >> "${RESULT_FILE}"

if /usr/bin/time -l true >/dev/null 2>&1; then
  TIME_CMD=(/usr/bin/time -l)
elif command -v /usr/bin/time >/dev/null 2>&1; then
  TIME_CMD=(/usr/bin/time -v)
else
  TIME_CMD=(time -p)
fi

declare -a CASES=(
  "ttfr_limit1|order_id|country = 'UK'|1"
  "country_eq|order_id,price_minor,total_minor|country = 'UK'|5"
  "price_gt|order_id,total_minor|price_minor > 2000|1000"
  "high_selectivity|order_id,total_minor|order_id = 'ORD000500000'|1"
)

expand_query() {
  local columns="$1"
  local predicate="$2"
  local limit="$3"
  local filePath="$4"
  printf 'SELECT %s FROM "%s" WHERE %s LIMIT %s' "${columns}" "${filePath}" "${predicate}" "${limit}"
}

expand_duckdb_query() {
  local columns="$1"
  local predicate="$2"
  local limit="$3"
  local filePath="$4"
  local outFile="$5"
  printf "COPY (SELECT %s FROM read_csv_auto('%s', HEADER=TRUE) WHERE %s LIMIT %s) TO '%s' (FORMAT CSV, HEADER TRUE);" \
    "${columns}" "${filePath}" "${predicate}" "${limit}" "${outFile}"
}

run_with_metrics() {
  local label="$1"
  local engine="$2"
  local outfile="$3"
  shift 3
  echo "## ${engine} - ${label}" >> "${RESULT_FILE}"
  if [[ "${engine}" == "sieswi" ]]; then
    "${TIME_CMD[@]}" "$@" > "${outfile}" 2>>"${RESULT_FILE}"
  else
    "${TIME_CMD[@]}" "$@" 2>>"${RESULT_FILE}"
  fi
  echo >> "${RESULT_FILE}"
}

TEMP_DIR="${RESULTS_DIR}/temp"
mkdir -p "${TEMP_DIR}"

echo >> "${RESULT_FILE}"
echo "# Output Verification" >> "${RESULT_FILE}"
echo >> "${RESULT_FILE}"

for case in "${CASES[@]}"; do
  IFS='|' read -r label columns predicate limit <<< "${case}"

  SIESWI_OUT="${TEMP_DIR}/sieswi_${label}.csv"
  DUCKDB_OUT="${TEMP_DIR}/duckdb_${label}.csv"

  printf -v sieswi_sql '%s' "$(expand_query "${columns}" "${predicate}" "${limit}" "${DATA_PATH}")"
  printf -v duckdb_sql '%s' "$(expand_duckdb_query "${columns}" "${predicate}" "${limit}" "${DATA_PATH}" "${DUCKDB_OUT}")"

  printf -v sieswi_cmd '%q %q' "${BIN_DIR}/sieswi" "${sieswi_sql}"
  run_with_metrics "${label}" "sieswi" "${SIESWI_OUT}" bash -c "${sieswi_cmd}"

  printf -v duck_cmd '%q -c %q' "$(command -v duckdb)" "${duckdb_sql}"
  run_with_metrics "${label}" "duckdb" "${DUCKDB_OUT}" bash -c "${duck_cmd}"

  # Compare outputs
  echo "## ${label} - output comparison" >> "${RESULT_FILE}"
  if diff -q "${SIESWI_OUT}" "${DUCKDB_OUT}" >/dev/null 2>&1; then
    echo "✓ Outputs are identical" >> "${RESULT_FILE}"
  else
    echo "✗ Outputs differ:" >> "${RESULT_FILE}"
    echo "  sieswi rows: $(wc -l < "${SIESWI_OUT}")" >> "${RESULT_FILE}"
    echo "  duckdb rows: $(wc -l < "${DUCKDB_OUT}")" >> "${RESULT_FILE}"
    echo "  First 5 lines diff:" >> "${RESULT_FILE}"
    diff "${SIESWI_OUT}" "${DUCKDB_OUT}" | head -20 >> "${RESULT_FILE}" 2>&1 || true
  fi
  echo >> "${RESULT_FILE}"
done

rm -rf "${TEMP_DIR}"

echo "Benchmark results written to ${RESULT_FILE}"
