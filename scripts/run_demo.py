import argparse
import json
import time
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError


API = "http://localhost:8000"


def post_json(url: str, payload: dict):
    body = json.dumps(payload).encode("utf-8")
    req = Request(url, data=body, method="POST", headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode("utf-8"))


def get_json(url: str):
    req = Request(url, headers={"User-Agent": "run_demo.py"})
    with urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode("utf-8"))


def wait_for_effect(business_key: str, *, timeout_s: int = 60):
    started = time.time()
    while time.time() - started < timeout_s:
        try:
            data = get_json(f"{API}/debug/side-effects?limit=50")
            for item in data.get("items", []):
                if item.get("business_key") == business_key and item.get("status") == "done":
                    return item
        except Exception:
            pass
        time.sleep(1.0)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["run", "crash"], required=True)
    args = ap.parse_args()

    business_key = f"order-{int(time.time())}"

    if args.mode == "run":
        payload = {
            "business_key": business_key,
            "amount": 42.0,
            "fail_before_effect_n": 1,
            "fail_mode": "none",
            "max_attempts": 5,
        }
        print("🟢 Running retry demo (1 failure before side effect, then success).")
    else:
        payload = {
            "business_key": business_key,
            "amount": 42.0,
            "fail_before_effect_n": 0,
            "fail_mode": "crash_after_effect_before_ack",
            "max_attempts": 5,
        }
        print("🧨 Running crash demo (side effect done, then worker crashes before ack).")

    out = post_json(f"{API}/runs", payload)
    run_id = out["run_id"]

    print(f"✅ run_id = {run_id}")
    print(f"🔎 Stream events (SSE): curl -N {API}/runs/{run_id}/events?client_id=cli")
    print("📜 Worker logs: docker compose logs -f worker")
    print("🔍 Side effects DB: http://localhost:8000/debug/side-effects")
    print("🗂️ Artifacts list:  http://localhost:8000/debug/artifacts")

    item = wait_for_effect(business_key, timeout_s=90)
    if item:
        print("✅ Side effect is DONE (exactly-once marker written).")
        print(f"   effect_id: {item.get('effect_id')}")
        print(f"   artifact:  {item.get('artifact_path')}")
    else:
        print("⚠️ Timed out waiting for side effect to reach DONE. Check worker logs.")


if __name__ == "__main__":
    main()
