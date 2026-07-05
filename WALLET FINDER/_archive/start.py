"""
start.py — WALLET FINDER launcher

Starts the WALLET FINDER proving dashboard on port 8012.
Does NOT start background scanners (run those separately).
"""
import subprocess
import sys
import os
from pathlib import Path

HERE = Path(__file__).resolve().parent
PID_FILE = HERE / ".wallet_finder_dashboard.pid"
PORT = 8012


def start():
    if PID_FILE.exists():
        pid = PID_FILE.read_text().strip()
        try:
            pid_int = int(pid)
            import signal
            os.kill(pid_int, 0)
            print(f"WALLET FINDER dashboard is already running (PID {pid_int}).")
            print(f"Visit http://localhost:{PORT}/")
            return
        except (ValueError, ProcessLookupError, OSError):
            PID_FILE.unlink(missing_ok=True)

    print(f"Starting WALLET FINDER dashboard on port {PORT}...")
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  RATE LIMIT WARNING                                     ║")
    print("║  This dashboard is READ-ONLY (no HL API calls).         ║")
    print("║  Scanner scripts (1WalletFinder, stage2 scanner,        ║")
    print("║  universe_builder) hit the SAME HL API as the live      ║")
    print("║  copy engine. Do NOT run scanners simultaneously        ║")
    print("║  with the live copy engine unless you have verified     ║")
    print("║  your rate limit headroom.                              ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    # Use uvicorn to serve the FastAPI app
    cmd = [
        sys.executable, "-m", "uvicorn",
        "app:app",
        "--host", "0.0.0.0",
        "--port", str(PORT),
        "--log-level", "info",
    ]

    process = subprocess.Popen(
        cmd,
        cwd=str(HERE),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    # Write PID
    PID_FILE.write_text(str(process.pid))

    print(f"WALLET FINDER dashboard started (PID {process.pid}).")
    print(f"Visit http://localhost:{PORT}/")
    print(f"To stop: python stop.py")
    print(f"PID file: {PID_FILE}")


if __name__ == "__main__":
    start()
