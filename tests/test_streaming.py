from src.streaming import EventProcessor, replay_jsonl

def test_dedup_and_replay(tmp_path):
    db = tmp_path / "events.db"
    p = EventProcessor(db)
    assert p.process({"event_id": "1", "category": "x"}) == "processed"
    assert p.process({"event_id": "1", "category": "x"}) == "duplicate"
    assert p.snapshot() == {"x": 1}
    p.close()
    p = EventProcessor(db)
    assert p.process({"event_id": "1", "category": "x"}) == "duplicate"
    p.close()

def test_dead_letter(tmp_path):
    p = EventProcessor(tmp_path / "x.db")
    assert p.process({"event_id": "1"}) == "dead_letter"
    assert p.snapshot() == {}
    p.close()

def test_fixture(tmp_path):
    r = replay_jsonl("data/events.jsonl", tmp_path / "a.db")
    assert (r["processed"], r["duplicate"], r["dead_letter"]) == (3, 1, 1)
    assert r["aggregates"] == {"billing": 2, "support": 1}
