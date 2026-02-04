import json
import os
import sqlite3
import time
from typing import Any, Dict, Optional, Tuple


class SideEffectStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.db_path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        # better concurrency behavior in dev
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
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

    def get_status(self, effect_id: str) -> Optional[Tuple[str, Optional[str]]]:
        self.ensure_schema()
        with self._conn() as c:
            r = c.execute("SELECT status, artifact_path FROM side_effects WHERE effect_id = ?;", (effect_id,)).fetchone()
        if not r:
            return None
        return str(r["status"]), (str(r["artifact_path"]) if r["artifact_path"] is not None else None)

    def mark_in_progress_if_new(self, effect_id: str, *, run_id: str, step_id: str, business_key: str, payload: Dict[str, Any]) -> bool:
        """
        Returns True if we inserted a new row (meaning: we should do the side effect now)
        Returns False if the row already exists (meaning: effect is already in progress or already done)
        """
        self.ensure_schema()
        now_ms = int(time.time() * 1000)
        payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        try:
            with self._conn() as c:
                c.execute(
                    """
                    INSERT INTO side_effects(effect_id, run_id, step_id, business_key, status, artifact_path, created_ms, updated_ms, payload_json)
                    VALUES(?, ?, ?, ?, 'in_progress', NULL, ?, ?, ?);
                    """,
                    (effect_id, run_id, step_id, business_key, now_ms, now_ms, payload_json)
                )

            return True
        except sqlite3.IntegrityError:
            return False

    def mark_done(self, effect_id: str, *, artifact_path: str) -> None:
        self.ensure_schema()
        now_ms = int(time.time() * 1000)
        with self._conn() as c:
            c.execute(
                "UPDATE side_effects SET status='done', artifact_path=?, updated_ms=? WHERE effect_id=?;",
                (artifact_path, now_ms, effect_id)
            )

    def mark_failed(self, effect_id: str, *, reason: str) -> None:
        self.ensure_schema()
        now_ms = int(time.time() * 1000)
        with self._conn() as c:
            c.execute(
                "UPDATE side_effects SET status='failed', updated_ms=? WHERE effect_id=?;",
                (now_ms, effect_id)
            )
