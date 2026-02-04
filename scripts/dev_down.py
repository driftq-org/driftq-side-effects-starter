import argparse
import subprocess
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wipe", action="store_true", help="Also delete volumes (WAL + sqlite store)")
    ap.add_argument("--prune-images", action="store_true", help="Also remove built images for this stack")
    ap.add_argument("--yes", action="store_true", help="Skip confirmation prompt for destructive actions")
    args = ap.parse_args()

    cmd = ["docker", "compose", "down"]

    if args.wipe:
        if not args.yes:
            ans = input("This will remove volumes (WAL + sqlite store). Are you sure? [y/N] ").strip().lower()
            if ans not in ("y", "yes"):
                print("aborted")
                return
        cmd.append("-v")

    subprocess.check_call(cmd)

    if args.prune_images:
        # remove images built from local Dockerfiles (api + worker)
        # This is best-effort and safe to ignore if it fails.
        try:
            subprocess.check_call(["docker", "image", "prune", "-f"])
        except Exception:
            pass

    print("✅ Stack is down.")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"command failed: {e}", file=sys.stderr)
        sys.exit(e.returncode)
