from src.streaming import replay_jsonl
import json

print(json.dumps(replay_jsonl("data/events.jsonl", "stream.db"), indent=2))
