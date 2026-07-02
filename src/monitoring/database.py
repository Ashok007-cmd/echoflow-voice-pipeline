"""SQLite persistence layer for tracking voice assistant turn performance metrics.

Uses a single persistent connection with WAL mode for concurrent read performance
and a background write queue so DB writes never block the real-time pipeline.
"""

import logging
import os
import sqlite3
import threading
from queue import Empty, Queue
from typing import Optional

from src.pipeline.models import TurnMetrics

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = os.environ.get(
    "ECHOFLOW_DB_PATH",
    os.path.join(os.path.expanduser("~"), ".local", "share", "echoflow", "metrics.db"),
)

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS turn_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp REAL,
        turn_number INTEGER,
        asr_ms REAL,
        llm_ttft_ms REAL,
        llm_total_ms REAL,
        tts_ms REAL,
        audio_playback_ms REAL,
        end_to_end_ms REAL,
        asr_backend TEXT,
        llm_backend TEXT,
        tts_backend TEXT,
        llm_model TEXT,
        error TEXT,
        degradation_used INTEGER,
        user_text TEXT,
        assistant_text TEXT
    )
"""

_INSERT_TURN = """
    INSERT INTO turn_metrics (
        timestamp, turn_number, asr_ms, llm_ttft_ms, llm_total_ms,
        tts_ms, audio_playback_ms, end_to_end_ms, asr_backend,
        llm_backend, tts_backend, llm_model, error,
        degradation_used, user_text, assistant_text
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_SENTINEL = object()


class MetricsDatabase:
    """Manages the SQLite database for latency metrics storage.

    Writes are dispatched to a background thread so the real-time pipeline
    is never stalled by disk I/O. The connection is opened once and reused
    across all writes (WAL journal mode for safe concurrent reads).
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._queue: Queue = Queue()
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            name="metrics-db-writer",
            daemon=True,
        )
        self._writer_thread.start()

    def _writer_loop(self) -> None:
        """Background thread: dequeues metrics and writes them to SQLite."""
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute(_CREATE_TABLE)
            conn.commit()

            while True:
                try:
                    item = self._queue.get(timeout=1.0)
                except Empty:
                    continue

                if item is _SENTINEL:
                    break

                try:
                    conn.execute(_INSERT_TURN, item)
                    conn.commit()
                except sqlite3.Error as e:
                    logger.error(f"Failed to write metrics to database: {e}")
                finally:
                    self._queue.task_done()

        except Exception as e:
            logger.error(f"Metrics database writer error: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except sqlite3.Error as e:
                    logger.debug(f"Error closing metrics database connection: {e}")

    def save_turn_metrics(self, metrics: TurnMetrics) -> None:
        """Enqueue a TurnMetrics record for background write."""
        row = (
            metrics.timestamp,
            metrics.turn_number,
            metrics.asr_ms,
            metrics.llm_ttft_ms,
            metrics.llm_total_ms,
            metrics.tts_ms,
            metrics.audio_playback_ms,
            metrics.end_to_end_ms,
            metrics.asr_backend,
            metrics.llm_backend,
            metrics.tts_backend,
            metrics.llm_model,
            metrics.error,
            1 if metrics.degradation_used else 0,
            metrics.user_text,
            metrics.assistant_text,
        )
        self._queue.put(row)

    def close(self) -> None:
        """Flush pending writes and shut down the background thread."""
        self._queue.put(_SENTINEL)
        self._writer_thread.join(timeout=5.0)

    def query_recent(self, limit: int = 100) -> list[dict]:
        """Return the most recent `limit` rows (synchronous read for reporting)."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM turn_metrics ORDER BY id DESC LIMIT ?", (limit,)
            )
            rows = [dict(r) for r in cursor.fetchall()]
            conn.close()
            return list(reversed(rows))
        except sqlite3.Error as e:
            logger.error(f"Failed to query metrics database: {e}")
            return []
