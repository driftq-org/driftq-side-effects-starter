import json
import os
from typing import Any, AsyncIterator, Dict, Optional

import httpx


class DriftQClient:
    def __init__(self) -> None:
        self.base = os.getenv("DRIFTQ_HTTP_URL", "http://localhost:8080").rstrip("/")

        self.owner = (
            os.getenv("DRIFTQ_OWNER")
            or os.getenv("HOSTNAME")
            or os.getenv("COMPUTERNAME")
            or "worker"
        )

        self._http = httpx.AsyncClient(timeout=httpx.Timeout(10.0))

    async def close(self) -> None:
        await self._http.aclose()

    async def healthz(self) -> Dict[str, Any]:
        r = await self._http.get(f"{self.base}/v1/healthz")
        r.raise_for_status()
        return r.json()

    async def ensure_topic(self, topic: str, partitions: int = 1) -> None:
        r = await self._http.post(
            f"{self.base}/v1/topics",
            params={"name": topic, "partitions": partitions}
        )

        if r.status_code not in (200, 201, 409):
            r.raise_for_status()

    async def produce(
        self,
        topic: str,
        value: Any,
        *,
        idempotency_key: Optional[str] = None,
        retry_max_attempts: Optional[int] = None,
        retry_backoff_ms: Optional[int] = None,
        retry_max_backoff_ms: Optional[int] = None,
        retry_jitter_ms: Optional[int] = None
    ) -> None:
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))

        params: Dict[str, Any] = {"topic": topic, "value": value}
        if idempotency_key:
            params["idempotency_key"] = idempotency_key

        if retry_max_attempts is not None:
            params["retry_max_attempts"] = retry_max_attempts

        if retry_backoff_ms is not None:
            params["retry_backoff_ms"] = retry_backoff_ms

        if retry_max_backoff_ms is not None:
            params["retry_max_backoff_ms"] = retry_max_backoff_ms

        if retry_jitter_ms is not None:
            params["retry_jitter_ms"] = retry_jitter_ms

        r = await self._http.post(f"{self.base}/v1/produce", params=params)
        r.raise_for_status()

    async def consume_stream(
        self,
        *,
        topic: str,
        group: str,
        lease_ms: int = 30_000,
        timeout_s: float = 60.0
    ) -> AsyncIterator[Dict[str, Any]]:
        # GET /v1/consume?topic=t&group=g&owner=o&lease_ms=5000  (NDJSON stream)
        # We'll reconnect forever if the server drops the stream
        params = {"topic": topic, "group": group, "owner": self.owner, "lease_ms": lease_ms}

        while True:
            async with self._http.stream(
                "GET",
                f"{self.base}/v1/consume",
                params=params,
                timeout=httpx.Timeout(timeout_s),
            ) as r:
                r.raise_for_status()

                async for line in r.aiter_lines():
                    if not line:
                        continue
                    # Each line is a JSON object (NDJSON)
                    yield json.loads(line)

    async def ack(self, *, topic: str, group: str, msg: Dict[str, Any]) -> None:
        partition = msg.get("partition")
        offset = msg.get("offset")
        if partition is None or offset is None:
            raise ValueError(f"Cannot ack message missing partition/offset: {msg}")

        r = await self._http.post(
            f"{self.base}/v1/ack",
            params={
                "topic": topic,
                "group": group,
                "owner": self.owner,
                "partition": partition,
                "offset": offset
            }
        )
        if r.status_code not in (200, 204, 409):
            r.raise_for_status()

    def extract_value(self, msg: Dict[str, Any]) -> Any:
        v = msg.get("value")
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return v
        return v
