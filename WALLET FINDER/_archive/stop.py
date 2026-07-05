"""
stop.py — WALLET FINDER shutdown

Stops the WALLET FINDER proving dashboard.
"""
import os
import signal
from pathlib import Path

HERE = Path(__file__).resolve().parent
PID_FILE = HERE / ".wallet_finder_dashboard.pid"


def stop():
    if not PID_FILE.exists():
        print("WALLET FINDER dashboard is not running (no PID file).")
        return

    pid_str = PID_FILE.read_text().strip()
    try:
        pid = int(pid_str)
    except ValueError:
        print(f"Invalid PID file: {pid_str}")
        PID_FILE.unlink(missing_ok=True)
        return

    # Windows: use taskkill for graceful shutdown; Unix: SIGTERM
    try:
        if os.name == 'nt':
            import subprocess
            subprocess.run(['taskkill', '/PID', str(pid), '/T', '/F'], capture_output=True, timeout=10)
        else:
            os.kill(pid, signal.SIGTERM)
        print(f"Stopping PID {pid}...")
        import time
        for _ in range(10):
            try:
                os.kill(pid, 0)
                time.sleep(0.5)
            except (ProcessLookupError, OSError):
                break
        PID_FILE.unlink(missing_ok=True)
        print("WALLET FINDER dashboard stopped.")
    except ProcessLookupError:
        print(f"PID {pid} not found. Removing stale PID file.")
        PID_FILE.unlink(missing_ok=True)
    except OSError as e:
        print(f"Failed to stop process: {e}")


if __name__ == "__main__":
    stop()
