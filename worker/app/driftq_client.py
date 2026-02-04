import os
from typing import Any, AsyncIterator, Dict, Optional

import httpx


class DriftQClient:
    def __init__(self) -> None:
        self.base = os.getenv("DRIFTQ_HTTP_URL", "http://localhost:8080").rstrip("/")
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(10.0))

    async def close(self) -> None:
        await self._http.aclose()

    async def healthz(self) -> Dict[str, Any]:
        r = await self._http.get(f"{self.base}/v1/healthz")
        r.raise_for_status()
        return r.json()

    async def ensure_topic(self, topic: str) -> None:
        r = await self._http.post(f"{self.base}/v1/topics", json={"name": topic})
        if r.status_code not in (200, 201, 409):
            r.raise_for_status()

    async def produce(self, topic: str, value: Dict[str, Any], *, idempotency_key: Optional[str] = None) -> None:
        payload: Dict[str, Any] = {"topic": topic, "value": value}
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key
        r = await self._http.post(f"{self.base}/v1/produce", json=payload)
        r.raise_for_status()

    async def consume_stream(
        self,
        *,
        topic: str,
        group: str,
        lease_ms: int = 30_000,
        timeout_s: float = 60.0
    ) -> AsyncIterator[Dict[str, Any]]:
        while True:
            r = await self._http.get(
                f"{self.base}/v1/consume",
                params={"topic": topic, "group": group, "lease_ms": lease_ms},
                timeout=httpx.Timeout(timeout_s)
            )

            r.raise_for_status()
            msg = r.json()
            if not msg:
                continue
            yield msg

    async def ack(self, *, topic: str, group: str, msg: Dict[str, Any]) -> None:
        r = await self._http.post(f"{self.base}/v1/ack", json={"topic": topic, "group": group, "msg": msg})
        if r.status_code not in (200, 204, 409):
            r.raise_for_status()

    def extract_value(self, msg: Dict[str, Any]) -> Any:
        return msg.get("value")
