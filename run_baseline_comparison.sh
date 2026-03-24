#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
cd /home/jagrit/pgrx

get_rss() {
    local pid=$(pgrep -f "beam.smp" 2>/dev/null | head -1)
    [ -z "$pid" ] && echo "0" && return
    awk '/VmRSS/{print $2}' /proc/$pid/status 2>/dev/null || echo "0"
}

echo "============================================"
echo "  pgrx Baseline Memory: Dev vs Production"
echo "============================================"
echo ""

# --- Dev mode ---
pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
sleep 6
DEV_RSS=$(get_rss)
echo "DEV MODE (mix run, 12 schedulers):"
echo "  RSS: ${DEV_RSS} KB ($(( DEV_RSS / 1024 )) MB)"
echo "  Threads: $(ls /proc/$(pgrep -f beam.smp | head -1)/task/ 2>/dev/null | wc -l)"
# Quick test
cd /home/jagrit/pgrx/native/cli
./target/release/pgrx -c "SELECT 1;" 2>&1 | grep -q "1 rows" && echo "  Status: OK" || echo "  Status: FAIL"
cd /home/jagrit/pgrx

pkill -9 -f beam.smp 2>/dev/null; sleep 2

# --- Dev mode with reduced schedulers ---
elixir --erl "+S 1:1 +SDcpu 2 +SDio 1 +sbwt none +stbt none" -S mix run --no-halt &
sleep 6
DEV2_RSS=$(get_rss)
echo ""
echo "DEV MODE (reduced schedulers +S 1:1):"
echo "  RSS: ${DEV2_RSS} KB ($(( DEV2_RSS / 1024 )) MB)"
echo "  Threads: $(ls /proc/$(pgrep -f beam.smp | head -1)/task/ 2>/dev/null | wc -l)"
cd /home/jagrit/pgrx/native/cli
./target/release/pgrx -c "SELECT 1;" 2>&1 | grep -q "1 rows" && echo "  Status: OK" || echo "  Status: FAIL"
cd /home/jagrit/pgrx

pkill -9 -f beam.smp 2>/dev/null; sleep 2

# --- Production release ---
_build/prod/rel/pgrx/bin/pgrx daemon
sleep 6
PROD_RSS=$(get_rss)
echo ""
echo "PRODUCTION RELEASE (stripped beams, minimal schedulers):"
echo "  RSS: ${PROD_RSS} KB ($(( PROD_RSS / 1024 )) MB)"
echo "  Threads: $(ls /proc/$(pgrep -f beam.smp | head -1)/task/ 2>/dev/null | wc -l)"
cd /home/jagrit/pgrx/native/cli
./target/release/pgrx -c "SELECT 1;" 2>&1 | grep -q "1 rows" && echo "  Status: OK" || echo "  Status: FAIL"
cd /home/jagrit/pgrx

# Test with 50 connections
./target/release/pgrx --memtest 50 2>&1 | grep -E "BASELINE|ACTIVE|HIBERNATED|CLOSED"

_build/prod/rel/pgrx/bin/pgrx stop 2>/dev/null; sleep 2

# --- Summary ---
echo ""
echo "============================================"
echo "  SUMMARY"
echo "============================================"
echo "  Dev (12 schedulers):    ${DEV_RSS} KB ($(( DEV_RSS / 1024 )) MB)"
echo "  Dev (1 scheduler):      ${DEV2_RSS} KB ($(( DEV2_RSS / 1024 )) MB)"
echo "  Production release:     ${PROD_RSS} KB ($(( PROD_RSS / 1024 )) MB)"
echo "  Savings: $(( DEV_RSS - PROD_RSS )) KB ($(( (DEV_RSS - PROD_RSS) / 1024 )) MB)"
echo "============================================"
