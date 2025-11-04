from fastapi.testclient import TestClient
from src.app.main import app

def test_health():
    c = TestClient(app)
    r = c.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

def test_daily_no_params():
    c = TestClient(app)
    r = c.get("/orders/daily")
    assert r.status_code == 200
    body = r.json()
    assert "daily" in body
    assert isinstance(body["daily"], list)
