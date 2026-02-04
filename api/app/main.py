import asyncio
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .driftq_client import DriftQClient
from .side_effects_store import SideEffectStore

COMMANDS_TOPIC = "sidefx.commands"
DLQ_TOPIC = "sidefx.dlq"
EVENTS_PREFIX = "sidefx.events."


class RunCreateRequest(BaseModel):
    business_key: str = Field(..., description="Your real-world key (order_id / payment_intent / ticket_id).")
    amount: float = 49.99
    # How many attempts should fail BEFORE we do the side effect (to prove retries are safe)
    fail_before_effect_n: int = 0
    # Chaos mode:
    # - none: normal
    # - crash_after_effect_before_ack: do the side effect, then crash the worker before ack (forces redelivery)
    fail_mode: str = Field(default="none")
    max_attempts: int = 5


class RunCreateResponse(BaseModel):
    run_id: str
    events_topic: str


# tiny in-memory run registry (demo only)
RUNS: Dict[str, Dict[str, Any]] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.driftq = DriftQClient()
    db_path = os.getenv("SIDEFX_DB_PATH", "/data/side_effects.sqlite")
    app.state.store = SideEffectStore(db_path)
    app.state.store.ensure_schema()

    try:
        yield
    finally:
        await app.state.driftq.close()


app = FastAPI(title="driftq-side-effects-starter API", lifespan=lifespan)

# CORS only matters if you bolt a UI on top (this repo doesn't ship a UI by default)
allow_origins = os.getenv("ALLOW_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in allow_origins if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


async def _ensure_topic(topic: str) -> None:
    try:
        await app.state.driftq.ensure_topic(topic)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"driftq.ensure_topic({topic}) failed: {e}")


async def _produce(topic: str, value: Dict[str, Any], *, idem: Optional[str] = None) -> None:
    try:
        await app.state.driftq.produce(topic, value, idempotency_key=idem)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"driftq.produce(topic={topic}) failed: {e}")


@app.get("/healthz")
async def healthz():
    try:
        driftq_health = await app.state.driftq.healthz()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"driftq unhealthy: {e}")
    return {"ok": True, "driftq": driftq_health}


@app.post("/runs", response_model=RunCreateResponse)
async def create_run(req: RunCreateRequest):
    run_id = uuid.uuid4().hex
    events_topic = f"{EVENTS_PREFIX}{run_id}"

    RUNS[run_id] = {
        "run_id": run_id,
        "business_key": req.business_key,
        "amount": req.amount,
        "fail_before_effect_n": req.fail_before_effect_n,
        "fail_mode": req.fail_mode,
        "max_attempts": req.max_attempts,
        "created_ms": _now_ms()
    }

    await _ensure_topic(COMMANDS_TOPIC)
    await _ensure_topic(DLQ_TOPIC)
    await _ensure_topic(events_topic)

    now_ms = _now_ms()

    await _produce(
        events_topic,
        {"ts": now_ms, "type": "run.created", "run_id": run_id, "business_key": req.business_key, "amount": req.amount},
        idem=f"evt:{run_id}:created",
    )

    # This is the command the worker will process.
    cmd = {
        "ts": now_ms,
        "type": "run.command",
        "run_id": run_id,
        "events_topic": events_topic,
        "step_id": "charge_card",
        "business_key": req.business_key,
        "amount": req.amount,
        "attempt": 0,
        "max_attempts": int(req.max_attempts),
        "fail_before_effect_n": int(req.fail_before_effect_n),
        "fail_mode": req.fail_mode
    }

    await _produce(
        COMMANDS_TOPIC,
        cmd,
        # attempt is part of the command idempotency key because retries are new messages
        idem=f"cmd:{run_id}:charge_card:{req.business_key}:a0"
    )

    await _produce(
        events_topic,
        {"ts": _now_ms(), "type": "command.enqueued", "run_id": run_id, "attempt": 0},
        idem=f"evt:{run_id}:enq:a0"
    )

    return RunCreateResponse(run_id=run_id, events_topic=events_topic)


@app.get("/runs/{run_id}/events")
async def stream_run_events(run_id: str, request: Request):
    meta = RUNS.get(run_id)
    if not meta:
        raise HTTPException(status_code=404, detail="run not found")

    events_topic = f"{EVENTS_PREFIX}{run_id}"
    client_id = (request.query_params.get("client_id") or "default")[:32]
    group = f"events-{run_id}-{client_id}"

    async def event_gen():
        try:
            yield f"data: {json.dumps({'type': 'sse.connected', 'run_id': run_id})}\n\n"
            async for msg in app.state.driftq.consume_stream(topic=events_topic, group=group, lease_ms=30_000, timeout_s=60.0):
                if await request.is_disconnected():
                    break

                evt = app.state.driftq.extract_value(msg)
                if isinstance(evt, dict):
                    yield f"data: {json.dumps(evt)}\n\n"

                try:
                    await app.state.driftq.ack(topic=events_topic, group=group, msg=msg)
                except Exception:
                    # best-effort ack for the demo
                    pass
        except (asyncio.CancelledError, GeneratorExit):
            return

    resp = StreamingResponse(event_gen(), media_type="text/event-stream")
    resp.headers["Cache-Control"] = "no-cache"
    resp.headers["X-Accel-Buffering"] = "no"
    resp.headers["Connection"] = "keep-alive"
    return resp


@app.get("/debug/side-effects")
async def debug_side_effects(limit: int = 50):
    rows = app.state.store.list_effects(limit=limit)
    return {
        "count": len(rows),
        "items": [
            {
                "effect_id": r.effect_id,
                "run_id": r.run_id,
                "step_id": r.step_id,
                "business_key": r.business_key,
                "status": r.status,
                "artifact_path": r.artifact_path,
                "created_ms": r.created_ms,
                "updated_ms": r.updated_ms,
                "payload": json.loads(r.payload_json)
            }

            for r in rows
        ]
    }


@app.get("/debug/artifacts")
async def debug_artifacts():
    artifacts_dir = os.getenv("ARTIFACTS_DIR", "/data/artifacts")
    tickets_dir = os.path.join(artifacts_dir, "tickets")
    items: list[dict] = []

    if os.path.isdir(tickets_dir):
        for name in sorted(os.listdir(tickets_dir))[-50:]:
            p = os.path.join(tickets_dir, name)
            if os.path.isfile(p):
                items.append({"name": name, "bytes": os.path.getsize(p)})

    return {"tickets_dir": tickets_dir, "count": len(items), "items": items}


@app.get("/debug/dlq")
async def debug_dlq(limit: int = 5):
    # quick peek without fancy indexing
    group = f"debug-dlq-{uuid.uuid4().hex[:8]}"
    items: list[dict] = []

    async for msg in app.state.driftq.consume_stream(topic=DLQ_TOPIC, group=group, lease_ms=30_000, timeout_s=3.0):
        payload = app.state.driftq.extract_value(msg)
        if isinstance(payload, dict):
            items.append(payload)
        if len(items) >= limit:
            break

    return {"count": len(items), "items": items}
