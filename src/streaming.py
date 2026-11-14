"""Replay-safe event processor; Kafka is an optional integration, not a requirement."""
import json
import sqlite3
import time

class EventProcessor:
    def __init__(self, db_path):
        self.db = sqlite3.connect(db_path)
        self.db.execute("CREATE TABLE IF NOT EXISTS seen (event_id TEXT PRIMARY KEY)")
        self.db.execute("CREATE TABLE IF NOT EXISTS counts (category TEXT PRIMARY KEY, total INTEGER NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS dead_letters (event_json TEXT, reason TEXT)")
        self.db.commit()

    def process(self, event):
        try:
            if (not isinstance(event, dict) or not event.get("event_id")
                or not isinstance(event.get("category"), str) or not event["category"].strip()):
                raise ValueError("missing event_id/category")
            with self.db:
                cursor = self.db.execute("INSERT OR IGNORE INTO seen VALUES (?)", (str(event["event_id"]),))
                if not cursor.rowcount:
                    return "duplicate"
                self.db.execute("INSERT INTO counts VALUES (?,1) ON CONFLICT(category) DO UPDATE SET total=total+1", (event["category"],))
            return "processed"
        except (ValueError, TypeError) as error:
            with self.db:
                self.db.execute("INSERT INTO dead_letters VALUES (?,?)", (json.dumps(event), str(error)))
            return "dead_letter"

    def snapshot(self):
        return dict(self.db.execute("SELECT category,total FROM counts ORDER BY category").fetchall())

    def close(self):
        self.db.close()

def replay_jsonl(input_path, db_path):
    p = EventProcessor(db_path)
    start = time.perf_counter()
    status = {"processed": 0, "duplicate": 0, "dead_letter": 0}
    try:
        with open(input_path, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    event = {"raw": line.strip()}
                status[p.process(event)] += 1
        status["elapsed_seconds"] = round(time.perf_counter() - start, 6)
        status["aggregates"] = p.snapshot()
        return status
    finally:
        p.close()
