#!/usr/bin/env bash
#
# Request the latest 10-K for 100 tickers.
#
#   ./scripts/request_tickers.sh
#   BASE_URL=http://api:8000 ./scripts/request_tickers.sh
#
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
FORM="${FORM:-10-K}"

TICKERS=(
    AAPL  MSFT  GOOGL AMZN  NVDA  META  TSLA  JPM   V     UNH
    XOM   JNJ   WMT   MA    PG    AVGO  HD    CVX   MRK   ABBV
    COST  PEP   ADBE  KO    CSCO  CRM   TMO   MCD   ACN   BAC
    LLY   ABT   DHR   NFLX  AMD   TXN   WFC   DIS   VZ    CMCSA
    INTC  PM    NKE   INTU  COP   NEE   RTX   UNP   BMY   QCOM
    HON   UPS   LOW   ORCL  IBM   SBUX  CAT   GS    BA    DE
    AMGN  BLK   ELV   PLD   LMT   SPGI  SYK   GILD  MDT   ADP
    TJX   CVS   MDLZ  AXP   C     MRSH  VRTX  CI    ZTS   SCHW
    MO    SO    DUK   BDX   CB    ITW   NOW   EQIX  APD   PGR
    CL    MU    AON   WM    SHW   FDX   NSC   EMR   GM    F
)

for ticker in "${TICKERS[@]}"; do
    code=$(curl -s -o /tmp/resp.$$ -w '%{http_code}' \
        -X POST "${BASE_URL}/reports" \
        -H "Content-Type: application/json" \
        -d "{\"ticker\": \"${ticker}\", \"form\": \"${FORM}\"}")

    # 202 queued | 200 already available | 404 unknown ticker | 429 queue full
    printf '%-6s %s %s\n' "${ticker}" "${code}" "$(cat /tmp/resp.$$)"
done

rm -f /tmp/resp.$$
