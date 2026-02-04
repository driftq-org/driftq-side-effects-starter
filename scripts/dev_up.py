import argparse
import subprocess
import sys
import time
from urllib.request import urlopen, Request


def check(url: str, timeout_s: float = 2.0):
    req = Request(url, headers={"User-Agent": "dev_up.py"})
    with urlopen(req, timeout=timeout_s) as r:
        return r.read().decode("utf-8")


def wait_for(url: str, *, timeout_s: int = 60):
    started = time.time()
    last_err = None
    while time.time() - started < timeout_s:
        try:
            _ = check(url, timeout_s=2.0)
            return True
        except Exception as e:
            last_err = e
            time.sleep(1.0)

    raise RuntimeError(f"timeout waiting for {url}: {last_err}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--detached", action="store_true", help="Run docker compose in detached mode")
    ap.add_argument("--build", action="store_true", default=True, help="Build images (default: true)")
    args = ap.parse_args()

    cmd = ["docker", "compose", "up"]
    if args.build:
        cmd.append("--build")
    if args.detached:
        cmd.append("-d")

    subprocess.check_call(cmd)

    # Wait for driftq + api
    wait_for("http://localhost:8080/v1/healthz", timeout_s=90)
    wait_for("http://localhost:8000/healthz", timeout_s=90)

    print("\n✅ Stack is up. Open:")
    print("  - API docs:      http://localhost:8000/docs")
    print("  - API health:    http://localhost:8000/healthz")
    print("  - DriftQ health: http://localhost:8080/v1/healthz")
    print("\nNext:")
    print("  make run   # retry demo (safe)")
    print("  make fail  # crash demo (redelivery + exactly-once)")
    print("  make worker-logs")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"command failed: {e}", file=sys.stderr)
        sys.exit(e.returncode)
