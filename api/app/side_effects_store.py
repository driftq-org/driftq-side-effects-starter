import os
import sqlite3
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class SideEffectRow:
    effect_id: str
    run_id: str
    step_id: str
    business_key: str
    status: str
    artifact_path: Optional[str]
    created_ms: int
    updated_ms: int
    payload_json: str


class SideEffectStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    def _conn(self) -> sqlite3.Connection:
        # check_same_thread=False because FastAPI is async / multi-thread friendly (we only do small reads here)
        c = sqlite3.connect(self.db_path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c

    def ensure_schema(self) -> None:
        with self._conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS side_effects (
                  effect_id TEXT PRIMARY KEY,
                  run_id TEXT NOT NULL,
                  step_id TEXT NOT NULL,
                  business_key TEXT NOT NULL,
                  status TEXT NOT NULL,
                  artifact_path TEXT,
                  created_ms INTEGER NOT NULL,
                  updated_ms INTEGER NOT NULL,
                  payload_json TEXT NOT NULL
                );
                """
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_side_effects_run_id ON side_effects(run_id);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_side_effects_business_key ON side_effects(business_key);")

    def list_effects(self, limit: int = 50) -> List[SideEffectRow]:
        self.ensure_schema()
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM side_effects ORDER BY updated_ms DESC LIMIT ?;",
                (int(limit),)
            ).fetchall()
        out: List[SideEffectRow] = []
        for r in rows:
            out.append(
                SideEffectRow(
                    effect_id=r["effect_id"],
                    run_id=r["run_id"],
                    step_id=r["step_id"],
                    business_key=r["business_key"],
                    status=r["status"],
                    artifact_path=r["artifact_path"],
                    created_ms=int(r["created_ms"]),
                    updated_ms=int(r["updated_ms"]),
                    payload_json=r["payload_json"]
                )
            )
        return out

    def get_effect(self, effect_id: str) -> Optional[SideEffectRow]:
        self.ensure_schema()
        with self._conn() as c:
            r = c.execute("SELECT * FROM side_effects WHERE effect_id = ?;", (effect_id,)).fetchone()
        if not r:
            return None
        return SideEffectRow(
            effect_id=r["effect_id"],
            run_id=r["run_id"],
            step_id=r["step_id"],
            business_key=r["business_key"],
            status=r["status"],
            artifact_path=r["artifact_path"],
            created_ms=int(r["created_ms"]),
            updated_ms=int(r["updated_ms"]),
            payload_json=r["payload_json"]
        )
