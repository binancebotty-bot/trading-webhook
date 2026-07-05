import subprocess, sys, os

script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "2hl_Stage1_Filter.py")
log_out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stage1_stdout.log")
log_err = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stage1_stderr.log")

with open(log_out, "w") as stdout_f, open(log_err, "w") as stderr_f:
    proc = subprocess.Popen(
        [sys.executable, script],
        stdout=stdout_f,
        stderr=stderr_f,
        cwd=os.path.dirname(os.path.abspath(__file__))
    )
    print(f"Stage 1 PID: {proc.pid}")
    proc.wait()
    print(f"Stage 1 finished with exit code: {proc.returncode}")
