# Integrate DriftQ into your app (Template B: exactly-once side effects) 🧩

You watched the side-effects demo and thought:

> "Ok cool… but how do I add this to *my* app without wrecking everything?"

This is the practical answer.

Heads up: this repo is a focused starter. For the full DriftQ engine + latest updates:
https://github.com/driftq-org/DriftQ-Core

---

## The mental model (don’t overthink it)

You’re adding:
- **DriftQ** (durable commands/events)
- **one worker** (runs the actual work)
- **one tiny idempotency store** (SQLite is enough to start)

Your "client" can be anything: UI, cron job, webhook handler, CLI, etc.

```
Client/UI/cron  ->  API (FastAPI)  ->  DriftQ (commands topic)
                                 ->  DriftQ (events topic)   (optional)
                                              ^
                                              |
                                          worker(s)
                               (LLMs / webhooks / DB / payments)
```

---

## What "exactly-once side effects" actually means here

Real world: retries happen. Crashes happen. Networks lie.

A "side effect" is anything you *really* don’t want duplicated:
- charge a card
- send a webhook
- create a Jira ticket
- send an email
- call a third-party API
- write an irreversible DB row (or anything expensive)

Exactly-once side effects means:
- The step might be *attempted* multiple times
- But the **external action** should only happen once per business key

---

## The core trick (copy/paste into your brain)

Use a stable key for the side effect:

```
effect_id = run_id:step_id:business_key
```

Store it in a table with a UNIQUE constraint.

Then your worker does:
1) "Have I already done this effect_id?"
2) If yes → skip the side effect
3) If no → do it, then mark done

That’s it.

---

## Minimal integration checklist

### 1) Run DriftQ somewhere reachable
Your API + worker must be able to hit DriftQ.

Env var you’ll use everywhere:
- `DRIFTQ_HTTP_URL=http://driftq:8080` (inside docker-compose network)
- or your cluster/service URL in prod

### 2) Add a tiny DriftQ client wrapper in your API
You only need a handful of calls:
- ensure_topic (or create topic once at boot)
- produce (commands/events)
- consume (optional, if you stream events back to clients via SSE)
- ack / nack (depending on your consume style)

Keep it boring. Don’t build a "framework" here.

### 3) Add an endpoint that enqueues work (don’t do it inline)
Example flow:
- `POST /runs` returns a `run_id`
- It produces a command message to a topic like `sidefx.commands`

Your API should be "start run + publish command", not "do the whole job."

### 4) Add a worker that consumes commands and executes steps
That worker is where you put the real work:
- LLM calls
- external API calls
- DB writes
- "workflow steps"

### 5) Add the side-effect store (idempotency)
SQLite is fine to start.

In prod you can move to Postgres/Redis/etc. The pattern stays the same:
**a UNIQUE key that proves "already done".**

---

## Common gotchas (read these or suffer later)

- **Don’t ack before you finish the side effect.**
  If you ack early and crash after, you just "confirmed success" without actually doing the work.

- **Don’t rely on "at-most-once".**
  You *will* lose work during restarts and you won’t notice until customers complain.

- **Treat retries as a feature.**
  But make them safe with idempotency.

- **Pick the right business key.**
  Order ID / payment intent / ticket ID — something you can explain to humans.

- **Don’t make your API the workflow engine.**
  Keep request handlers thin. Push durable behavior into DriftQ + worker.

---

## If you want the full engine

This repo is a focused starter to prove one killer thing.

DriftQ-Core is the real engine:
https://github.com/driftq-org/DriftQ-Core
