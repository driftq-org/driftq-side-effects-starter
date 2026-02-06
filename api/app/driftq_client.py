import json
import os
import socket
import time
from typing import Any, AsyncIterator, Dict, Optional

import httpx


class DriftQClient:
    def __init__(self) -> None:
        self.base = os.getenv("DRIFTQ_HTTP_URL", "http://localhost:8080").rstrip("/")
        # stable per-container default; you can also set DRIFTQ_OWNER explicitly if you want
        self.owner = os.getenv("DRIFTQ_OWNER") or socket.gethostname()

        # IMPORTANT: for streaming, don't use a tiny read timeout globally
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0, read=None, write=10.0, pool=10.0))

    async def close(self) -> None:
        await self._http.aclose()

    async def healthz(self) -> Dict[str, Any]:
        r = await self._http.get(f"{self.base}/v1/healthz")
        r.raise_for_status()
        return r.json()

    async def list_topics(self) -> Dict[str, Any]:
        r = await self._http.get(f"{self.base}/v1/topics")
        r.raise_for_status()
        return r.json()

    async def ensure_topic(self, topic: str, partitions: int = 1) -> None:
        topics = await self.list_topics()
        names = {t.get("name") for t in topics.get("topics", []) if isinstance(t, dict)}
        if topic in names:
            return

        r = await self._http.post(
            f"{self.base}/v1/topics",
            params={"name": topic, "partitions": str(partitions)}
        )

        if r.status_code not in (200, 201, 204, 409):
            r.raise_for_status()

    def _value_to_string(self, value: Any) -> str:
        if value is None:
            return ""

        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")

        if isinstance(value, str):
            return value
        # JSON for dict/list/etc
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)

    async def produce(
        self,
        topic: str,
        value: Any,
        *,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        params = {
            "topic": topic,
            "value": self._value_to_string(value)
        }

        if idempotency_key:
            params["idempotency_key"] = idempotency_key

        r = await self._http.post(f"{self.base}/v1/produce", params=params)

        if r.status_code >= 400:
            raise RuntimeError(f"DriftQ /v1/produce failed {r.status_code}: {r.text}")

        return r.json()

    async def consume_stream(
        self,
        *,
        topic: str,
        group: str,
        lease_ms: int = 30_000,
        timeout_s: Optional[float] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        DriftQ consumes as NDJSON stream (one JSON per line). Reconnect on EOF
        """
        params = {"topic": topic, "group": group, "owner": self.owner, "lease_ms": str(lease_ms)}

        start = time.time()
        while True:
            if timeout_s is not None and (time.time() - start) > timeout_s:
                return

            # read timeout == how long we’ll wait for the next line
            per_req_timeout = httpx.Timeout(connect=10.0, read=timeout_s, write=10.0, pool=10.0) if timeout_s else None

            try:
                async with self._http.stream(
                    "GET",
                    f"{self.base}/v1/consume",
                    params=params,
                    timeout=per_req_timeout,
                ) as r:
                    r.raise_for_status()
                    async for line in r.aiter_lines():
                        if not line.strip():
                            continue
                        yield json.loads(line)

            except httpx.ReadTimeout:
                return

    async def ack(self, *, topic: str, group: str, msg: Dict[str, Any]) -> None:
        partition = msg.get("partition")
        offset = msg.get("offset")
        if partition is None or offset is None:
            raise RuntimeError(f"Cannot ack: msg missing partition/offset: {msg}")

        r = await self._http.post(
            f"{self.base}/v1/ack",
            params={
                "topic": topic,
                "group": group,
                "owner": self.owner,
                "partition": str(partition),
                "offset": str(offset)
            }
        )

        # 409 can happen if lease is lost so treat as "not fatal" for demo
        if r.status_code not in (200, 204, 409):
            raise RuntimeError(f"DriftQ /v1/ack failed {r.status_code}: {r.text}")

    def extract_value(self, msg: Dict[str, Any]) -> Any:
        v = msg.get("value")
        if isinstance(v, str):
            s = v.strip()
            if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
                try:
                    return json.loads(s)
                except Exception:
                    return v

        return v
