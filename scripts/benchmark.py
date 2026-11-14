"""Synthetic local benchmark; NOT Kafka, Spark or cloud throughput."""
import argparse
import json
import tempfile
import time
from pathlib import Path
from src.streaming import replay_jsonl

parser = argparse.ArgumentParser()
parser.add_argument("--events", type=int, default=10000)
args = parser.parse_args()
with tempfile.TemporaryDirectory() as d:
    source = Path(d) / "events.jsonl"
    with source.open("w") as output:
        for i in range(args.events):
            output.write(json.dumps({"event_id": str(i), "category": f"c{i%8}"}) + "\n")
    start = time.perf_counter()
    result = replay_jsonl(source, Path(d) / "state.db")
    elapsed = time.perf_counter() - start
    print(json.dumps({
        "synthetic_events": args.events,
        "elapsed_seconds": round(elapsed, 3),
        "events_per_second": round(args.events / elapsed, 1),
        "processor": result
    }, indent=2))
