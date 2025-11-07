from fastapi.testclient import TestClient
from src.app.main import app

def test_metrics_shape():
    c = TestClient(app)
    r = c.get("/metrics")
    assert r.status_code == 200
    body = r.json()
    assert {"last_load_at","last_load_inserted","last_load_status","last_error_message"} <= set(body.keys())

def test_daily_range_ok():
    c = TestClient(app)
    r = c.get("/orders/daily", params={"start": "2025-10-02", "end": "2025-10-03"})
    assert r.status_code == 200
    body = r.json()
    assert "daily" in body and isinstance(body["daily"], list)
