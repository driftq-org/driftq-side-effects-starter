.PHONY: up down wipe logs api-logs worker-logs driftq-logs run fail chaos-kill-worker

up:
	python scripts/dev_up.py --detached

down:
	python scripts/dev_down.py

wipe:
	python scripts/dev_down.py --wipe --yes

logs:
	docker compose logs -f

api-logs:
	docker compose logs -f api

worker-logs:
	docker compose logs -f worker

driftq-logs:
	docker compose logs -f driftq

# Happy-path demo: one transient failure before side-effect, then success (no duplicates)
run:
	python scripts/run_demo.py --mode run

# Chaos demo: worker crashes AFTER doing the side-effect but BEFORE ack'ing the message.
# The message gets redelivered, but the side-effect store makes it exactly-once.
fail:
	python scripts/run_demo.py --mode crash

# Manual chaos: kill the worker container (SIGKILL). Useful if you want to do it by hand mid-run.
chaos-kill-worker:
	docker compose kill -s SIGKILL worker
