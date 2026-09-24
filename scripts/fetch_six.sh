#!/usr/bin/env bash
#
# Fetch the latest 10-K for the six assignment companies as PDFs.
#
#   ./scripts/fetch_six.sh
#   BASE_URL=http://api:8000 OUT_DIR=/tmp/pdfs ./scripts/fetch_six.sh
#
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
OUT_DIR="${OUT_DIR:-./pdfs}"
TIMEOUT="${TIMEOUT:-300}"

# Alphabet files as GOOGL; Goldman Sachs as GS.
TICKERS="AAPL META GOOGL AMZN NFLX GS"

# Pull one string field out of a JSON object. Good enough here because the
# responses are flat and the keys we want appear once; anything more
# structured would mean depending on jq.
field() { grep -o "\"$1\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" | head -1 | sed 's/.*"\([^"]*\)"$/\1/'; }

curl -sf -o /dev/null "${BASE_URL}/healthz" || {
    echo "error: no service at ${BASE_URL} (try: docker compose up --build)" >&2
    exit 1
}

mkdir -p "${OUT_DIR}"
ok=0

# Each ticker is submitted and waited on in turn. The workers still run them
# concurrently, so by the time the first PDF is downloaded the rest are
# usually done or close to it.
for ticker in ${TICKERS}; do
    body=$(curl -s -X POST "${BASE_URL}/reports" \
        -H "Content-Type: application/json" \
        -d "{\"ticker\": \"${ticker}\", \"form\": \"10-K\"}")
    task_id=$(printf '%s' "${body}" | field task_id)
    deadline=$(( $(date +%s) + TIMEOUT ))

    # A report already on disk comes back completed immediately, with no
    # task id to poll.
    while :; do
        state=$(printf '%s' "${body}" | field state)
        case "${state}" in
            completed|failed) break ;;
        esac
        if [ -z "${task_id}" ] || [ "$(date +%s)" -ge "${deadline}" ]; then
            state="${state:-error}"
            break
        fi
        sleep 3
        body=$(curl -s "${BASE_URL}/tasks/${task_id}")
    done

    if [ "${state}" = "completed" ]; then
        url=$(printf '%s' "${body}" | field url)
        if [ -n "${url}" ] && curl -sf -o "${OUT_DIR}/${ticker}-10-K.pdf" "${BASE_URL}${url}"; then
            ok=$(( ok + 1 ))
            printf '  %-6s done  -> %s\n' "${ticker}" "${OUT_DIR}/${ticker}-10-K.pdf"
            continue
        fi
        printf '  %-6s completed but artifact download failed\n' "${ticker}"
    else
        printf '  %-6s %s\n' "${ticker}" "${state}"
    fi
done

echo
echo "${ok}/6 PDFs in ${OUT_DIR}"
[ "${ok}" -eq 6 ]
