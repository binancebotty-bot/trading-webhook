#!/usr/bin/env bash
set -euo pipefail

# cd to script directory (WALLET FINDER/)
cd "$(dirname "$0")"

echo
echo "============================================"
echo "  WALLET FINDER — Stopping Both Dashboards"
echo "============================================"
echo

stop_by_pid_file() {
    local pid_file="$1"
    local label="$2"
    if [ -f "$pid_file" ]; then
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Killing PID $pid ($label)..."
            kill "$pid" 2>/dev/null || true
            sleep 1
            # Force kill if still alive
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$pid_file"
        echo "  $label stopped."
    fi
}

stop_by_port() {
    local port="$1"
    local label="$2"
    # Windows: use netstat + taskkill
    local pid
    pid=$(netstat -ano 2>/dev/null | grep ":$port" | grep LISTENING | awk '{print $NF}' | head -1) || true
    if [ -n "$pid" ]; then
        echo "Killing PID $pid on port $port ($label)..."
        cmd //c "taskkill /f /pid $pid" 2>/dev/null || true
        echo "  $label stopped."
    fi
}

# Try PID files first, then fall back to port scan
stop_by_pid_file .proving_engine.pid "Proving Engine"
stop_by_pid_file .live_copy.pid "Live Copy Dashboard"

# Belt and suspenders: kill anything still on these ports
stop_by_port 8012 "Proving Engine (port scan)"
stop_by_port 8014 "Live Copy Dashboard (port scan)"

echo
echo "Both servers stopped."
echo
