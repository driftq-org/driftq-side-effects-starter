# DriftQ Side-Effects Starter (idempotency + safe retries) 🧨✅

This repo is a **production-ish** starter that proves a real claim:

> **Retries won’t duplicate side effects.**
> Even if the worker crashes mid-step and DriftQ redelivers the message.

It’s meant for engineers who already have a Next.js + FastAPI app and are thinking:
**"ok cool… but how do I stop double-charging / double-webhooking when retries happen?"**

✅ That’s what this repo shows.

---

## What this template proves (in plain English)

- **DriftQ** is the durable middle layer (commands + events live there)
- **Worker** is where the "real work" happens (LLM calls, webhooks, DB writes, Jira tickets, etc.)
- The worker can crash or retry… and **your side effect still happens exactly once**
- We keep a **SQLite side-effect store** that records "already done" so duplicates get skipped

---

## Quickstart (one command) 🔥

From repo root:

```bash
python scripts/dev_up.py --detached
```

When it’s up:

- **API docs:** http://localhost:8000/docs
- **DriftQ health:** http://localhost:8080/v1/healthz

Logs:

```bash
docker compose logs -f worker
```

Stop:

```bash
python scripts/dev_down.py
```

Wipe everything (including DriftQ WAL + SQLite store):

```bash
python scripts/dev_down.py --wipe --yes
```

---

## The two demos (do these in order)

### 1) Happy path with a real retry (no chaos)

This forces **one failure BEFORE the side effect**, then succeeds on retry.

```bash
make run
```

What you should see in worker logs:
- attempt 0: fails (before side effect)
- attempt 1: side effect runs, store marked **done**, run completes

Then hit debug endpoints:
- side effect rows: http://localhost:8000/debug/side-effects
- artifacts list: http://localhost:8000/debug/artifacts

### 2) Chaos mode: crash mid-step (the scary one)

This is the "killer demo".

It does the side effect, then the worker **crashes on purpose before ack**.
DriftQ redelivers the command. The worker comes back up.
And the side effect does **NOT** run twice.

```bash
make fail
```

Again: check worker logs and the debug endpoints.
You should still see **one** side effect row + **one** artifact file.

---

## How the idempotency story works

### Stable idempotency key (the whole point)

We use a stable key like:

```
effect_id = run_id:step_id:business_key
```

Examples:
- `run_id` = the run
- `step_id` = `"charge_card"` (or `"send_webhook"` / `"create_ticket"`)
- `business_key` = your real-world key (order_id / payment_intent / ticket_id)

The worker stores that in SQLite. If the message comes again:
- row exists → we skip the side effect
- row doesn’t exist → we run it and mark done

### Why we do *not* put this logic inside the API

Because that turns into a mess fast:
- ad-hoc retries everywhere
- partial side effects
- "did it run?" becomes a DB archaeology expedition
- then you reinvent a workflow engine inside your request handlers 😬

This repo keeps the API thin and puts durable behavior in:
- **DriftQ** (commands/events + redelivery)
- **worker** (retries + idempotent side effects)
- **SQLite store** (exactly-once markers)

---

## Where the "proof artifact" lives

We write a "ticket file" (fake webhook/ticket) under `/data/artifacts/` (Docker volume).
And we record a row in `/data/side_effects.sqlite`.

So you get:
- **DB proof** (authoritative)
- **file proof** (easy to eyeball)

---

## "How do I integrate this into my app?"

Read: `docs/INTEGRATE_IN_YOUR_APP.md`
It’s the playbook for bolting this onto a real FastAPI + Next.js codebase.

---

## DriftQ-Core link (for the real engine)

This starter is intentionally focused and tiny-ish.
For the actual engine + roadmap + latest changes:

https://github.com/driftq-org/DriftQ-Core
