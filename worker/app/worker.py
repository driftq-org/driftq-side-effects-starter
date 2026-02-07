import asyncio
import json
import os
import random
import sys
import time
from typing import Any, Dict, Optional

from .driftq_client import DriftQClient
from .side_effects_store import SideEffectStore

COMMANDS_TOPIC = "sidefx.commands"
DLQ_TOPIC = "sidefx.dlq"
EVENTS_PREFIX = "sidefx.events."

LOG_LEVEL = os.getenv("LOG_LEVEL", "info").lower()


def _now_ms() -> int:
    return int(time.time() * 1000)


def log(msg: str) -> None:
    if LOG_LEVEL in ("debug", "info"):
        print(msg, flush=True)


async def emit(driftq: DriftQClient, events_topic: str, evt: Dict[str, Any], *, idem: Optional[str] = None) -> None:
    try:
        await driftq.produce(events_topic, evt, idempotency_key=idem)
    except Exception as e:
        # For a demo worker, we don't want event publishing to kill the whole step
        log(f"[warn] failed to emit event to {events_topic}: {e}")


def effect_id_for(run_id: str, step_id: str, business_key: str) -> str:
    # This is the stable key that makes the side effect exactly-once
    return f"{run_id}:{step_id}:{business_key}"


async def handle_command(driftq: DriftQClient, store: SideEffectStore, cmd: Dict[str, Any]) -> None:
    run_id = str(cmd.get("run_id"))
    events_topic = str(cmd.get("events_topic") or f"{EVENTS_PREFIX}{run_id}")
    step_id = str(cmd.get("step_id") or "charge_card")
    business_key = str(cmd.get("business_key") or "missing")
    amount = float(cmd.get("amount") or 0)
    attempt = int(cmd.get("attempt") or 0)
    # max_attempts = int(cmd.get("max_attempts") or 5)
    fail_before_effect_n = int(cmd.get("fail_before_effect_n") or 0)
    fail_mode = str(cmd.get("fail_mode") or "none")

    await driftq.ensure_topic(events_topic)

    await emit(
        driftq,
        events_topic,
        {"ts": _now_ms(), "type": "step.started", "run_id": run_id, "step_id": step_id, "attempt": attempt},
        idem=f"evt:{run_id}:{step_id}:started:a{attempt}",
    )

    # Simulate transient failure BEFORE side effect (safe retry scenario)
    if attempt < fail_before_effect_n:
        await emit(
            driftq,
            events_topic,
            {
                "ts": _now_ms(),
                "type": "step.failed",
                "run_id": run_id,
                "step_id": step_id,
                "attempt": attempt,
                "reason": "forced_failure_before_side_effect",
            },
            idem=f"evt:{run_id}:{step_id}:failed_before:a{attempt}",
        )
        raise RuntimeError("forced failure before side effect")

    # --- Exactly-once side effect ---
    eid = effect_id_for(run_id, step_id, business_key)
    status = store.get_status(eid)
    if status and status[0] == "done":
        await emit(
            driftq,
            events_topic,
            {
                "ts": _now_ms(),
                "type": "side_effect.skipped",
                "run_id": run_id,
                "step_id": step_id,
                "business_key": business_key,
                "effect_id": eid,
                "reason": "already_done",
            },
            idem=f"evt:{run_id}:{step_id}:effect:skipped"
        )
    else:
        inserted = store.mark_in_progress_if_new(
            eid,
            run_id=run_id,
            step_id=step_id,
            business_key=business_key,
            payload=cmd
        )
        if not inserted:
            # Someone else / previous attempt created the row (could be in_progress)
            # If the artifact already exists, we can safely finalize the record as done
            artifacts_dir = os.getenv("ARTIFACTS_DIR", "/data/artifacts")
            tickets_dir = os.path.join(artifacts_dir, "tickets")
            os.makedirs(tickets_dir, exist_ok=True)
            artifact_path = os.path.join(tickets_dir, f"ticket_{business_key}.json")
            if os.path.exists(artifact_path):
                store.mark_done(eid, artifact_path=artifact_path)
                await emit(
                    driftq,
                    events_topic,
                    {"ts": _now_ms(), "type": "side_effect.healed", "run_id": run_id, "effect_id": eid},
                    idem=f"evt:{run_id}:{step_id}:effect:healed",
                )
            else:
                await emit(
                    driftq,
                    events_topic,
                    {
                        "ts": _now_ms(),
                        "type": "side_effect.skipped",
                        "run_id": run_id,
                        "step_id": step_id,
                        "business_key": business_key,
                        "effect_id": eid,
                        "reason": "already_in_progress",
                    },
                    idem=f"evt:{run_id}:{step_id}:effect:skipped_in_progress"
                )
        else:
            await emit(
                driftq,
                events_topic,
                {
                    "ts": _now_ms(),
                    "type": "side_effect.executing",
                    "run_id": run_id,
                    "step_id": step_id,
                    "business_key": business_key,
                    "effect_id": eid,
                    "amount": amount
                },
                idem=f"evt:{run_id}:{step_id}:effect:exec"
            )

            # Simulate the external side effect as writing a "ticket" file (proof artifact).
            artifacts_dir = os.getenv("ARTIFACTS_DIR", "/data/artifacts")
            tickets_dir = os.path.join(artifacts_dir, "tickets")
            os.makedirs(tickets_dir, exist_ok=True)
            artifact_path = os.path.join(tickets_dir, f"ticket_{business_key}.json")

            payload = {
                "created_ms": _now_ms(),
                "run_id": run_id,
                "step_id": step_id,
                "business_key": business_key,
                "amount": amount,
                "note": "fake external side effect (ticket/webhook/charge)"
            }

            # Create-only: will error if it already exists
            # That makes the artifact creation idempotent too
            try:
                with open(artifact_path, "x", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
            except FileExistsError:
                pass

            store.mark_done(eid, artifact_path=artifact_path)

            await emit(
                driftq,
                events_topic,
                {
                    "ts": _now_ms(),
                    "type": "side_effect.done",
                    "run_id": run_id,
                    "step_id": step_id,
                    "business_key": business_key,
                    "effect_id": eid,
                    "artifact_path": artifact_path,
                },
                idem=f"evt:{run_id}:{step_id}:effect:done"
            )

            # Chaos mode: crash AFTER side effect but BEFORE ack
            if fail_mode == "crash_after_effect_before_ack":
                await emit(
                    driftq,
                    events_topic,
                    {
                        "ts": _now_ms(),
                        "type": "chaos.crash_now",
                        "run_id": run_id,
                        "note": "crashing worker on purpose after side effect, before ack"
                    },
                    idem=f"evt:{run_id}:{step_id}:chaos:crash"
                )
                log("[chaos] crashing worker NOW (after side effect, before ack)")
                os._exit(137)

    await emit(
        driftq,
        events_topic,
        {"ts": _now_ms(), "type": "step.completed", "run_id": run_id, "step_id": step_id, "attempt": attempt},
        idem=f"evt:{run_id}:{step_id}:completed:a{attempt}"
    )
    await emit(driftq, events_topic, {"ts": _now_ms(), "type": "run.completed", "run_id": run_id}, idem=f"evt:{run_id}:completed")


async def main() -> None:
    driftq = DriftQClient()
    store = SideEffectStore(os.getenv("SIDEFX_DB_PATH", "/data/side_effects.sqlite"))
    store.ensure_schema()

    group = os.getenv("WORKER_GROUP", "sidefx-worker")
    lease_ms = 30_000

    # make sure base topics exist
    await driftq.ensure_topic(COMMANDS_TOPIC)
    await driftq.ensure_topic(DLQ_TOPIC)

    log(f"[worker] starting consume loop: topic={COMMANDS_TOPIC} group={group}")

    try:
        async for msg in driftq.consume_stream(topic=COMMANDS_TOPIC, group=group, lease_ms=lease_ms, timeout_s=60.0):
            cmd = driftq.extract_value(msg)
            if not isinstance(cmd, dict):
                # bad message; ack and move on
                try:
                    await driftq.ack(topic=COMMANDS_TOPIC, group=group, msg=msg)
                except Exception:
                    pass
                continue

            run_id = str(cmd.get("run_id") or "unknown")
            attempt = int(cmd.get("attempt") or 0)
            max_attempts = int(cmd.get("max_attempts") or 5)
            events_topic = str(cmd.get("events_topic") or f"{EVENTS_PREFIX}{run_id}")
            step_id = str(cmd.get("step_id") or "charge_card")
            business_key = str(cmd.get("business_key") or "missing")

            try:
                await handle_command(driftq, store, cmd)

                # success -> ack
                try:
                    await driftq.ack(topic=COMMANDS_TOPIC, group=group, msg=msg)
                except Exception:
                    pass

            except Exception as e:
                # failed -> schedule retry OR DLQ
                next_attempt = attempt + 1
                backoff_s = min(2 ** attempt, 10) + random.random()  # quick + dirty
                await emit(
                    driftq,
                    events_topic,
                    {
                        "ts": _now_ms(),
                        "type": "retry.considered",
                        "run_id": run_id,
                        "step_id": step_id,
                        "attempt": attempt,
                        "next_attempt": next_attempt,
                        "max_attempts": max_attempts,
                        "error": str(e),
                        "backoff_s": round(backoff_s, 2)
                    },
                    idem=f"evt:{run_id}:{step_id}:retry:considered:a{attempt}"
                )

                if next_attempt >= max_attempts:
                    # DLQ record + ack original
                    dlq = {
                        "ts": _now_ms(),
                        "type": "sidefx.dlq",
                        "run_id": run_id,
                        "step_id": step_id,
                        "business_key": business_key,
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                        "error": str(e),
                        "command": cmd
                    }

                    try:
                        await driftq.produce(DLQ_TOPIC, dlq, idempotency_key=f"dlq:{run_id}:{step_id}:{business_key}")
                    except Exception as pe:
                        log(f"[warn] failed to write DLQ: {pe}")

                    await emit(
                        driftq,
                        events_topic,
                        {"ts": _now_ms(), "type": "run.dlq", "run_id": run_id, "step_id": step_id, "error": str(e)},
                        idem=f"evt:{run_id}:{step_id}:dlq",
                    )

                    try:
                        await driftq.ack(topic=COMMANDS_TOPIC, group=group, msg=msg)
                    except Exception:
                        pass
                    continue

                # schedule retry (new command message), then ack original
                retry_cmd = dict(cmd)
                retry_cmd["attempt"] = next_attempt
                retry_cmd["ts"] = _now_ms()

                try:
                    await driftq.produce(
                        COMMANDS_TOPIC,
                        retry_cmd,
                        idempotency_key=f"cmd:{run_id}:{step_id}:{business_key}:a{next_attempt}"
                    )
                except Exception as pe:
                    log(f"[warn] failed to schedule retry produce: {pe}")

                await emit(
                    driftq,
                    events_topic,
                    {"ts": _now_ms(), "type": "retry.scheduled", "run_id": run_id, "step_id": step_id, "attempt": next_attempt},
                    idem=f"evt:{run_id}:{step_id}:retry:scheduled:a{next_attempt}",
                )

                try:
                    await driftq.ack(topic=COMMANDS_TOPIC, group=group, msg=msg)
                except Exception:
                    pass

    except asyncio.CancelledError:
        raise
    finally:
        await driftq.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
