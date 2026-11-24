from collections import defaultdict
from datetime import UTC, datetime
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from app import main
from app.generator import demo_events
from app.kafka_producer import PublicationError
from app.models import DeviceType, EventType


@pytest.fixture
def client(monkeypatch):
    class Publisher:
        def __init__(self):
            self.events = []

        def publish(self, event):
            self.events.append(event)

    monkeypatch.setattr(main, "EventPublisher", Publisher)
    with TestClient(main.app) as client:
        yield client


@pytest.fixture
def payload():
    return {
        "user_id": "user-1",
        "movie_id": "orbit",
        "event_type": "VIEW_PAUSED",
        "timestamp": "2026-04-01T14:23:02.123456+03:00",
        "device_type": "MOBILE",
        "session_id": "session-1",
        "progress_seconds": 300,
    }


def test_create_event_generates_uuid_and_normalizes_utc(client, payload):
    response = client.post("/events", json=payload)
    assert response.status_code == 201
    assert response.json()["status"] == "published"
    event, = client.app.state.publisher.events
    assert UUID(response.json()["event_id"]) == event.event_id
    assert event.avro_record()["timestamp"] == datetime(2026, 4, 1, 11, 23, 2, 123000, UTC)
    assert event.user_id == "user-1"


@pytest.mark.parametrize(("field", "bad_value"), [
    ("event_type", "PLAY"),
    ("device_type", "TOASTER"),
    ("event_id", "not-a-uuid"),
    ("progress_seconds", -1),
    ("progress_seconds", 2**31),
    ("timestamp", "2026-04-01T11:23:02"),
    ("timestamp", 1775042582),
    ("timestamp", "1900-01-01T00:00:00Z"),
    ("user_id", " "),
    ("session_id", ""),
])
def test_event_validation(client, payload, field, bad_value):
    payload[field] = bad_value
    response = client.post("/events", json=payload)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", field]
    assert not client.app.state.publisher.events


def test_publish_error_is_a_503(client, payload, monkeypatch):
    def fail(_event):
        raise PublicationError("Kafka unavailable")

    monkeypatch.setattr(client.app.state.publisher, "publish", fail)
    assert client.post("/events", json=payload).status_code == 503


def test_generator_is_deterministic_and_has_realistic_sessions():
    events = demo_events()
    assert events == demo_events()
    assert len({event.event_id for event in events}) == len(events)
    assert len({event.timestamp.date() for event in events}) == 9
    assert {event.device_type for event in events} == set(DeviceType)
    assert {event.event_type for event in events} == set(EventType)
    sessions = defaultdict(list)
    for event in events:
        sessions[event.session_id].append(event)
        if event.event_type == EventType.SEARCHED:
            assert event.movie_id == "__search__" and event.progress_seconds == 0
    assert len({event.user_id for event in events}) > 1
    assert len({event.movie_id for event in events if event.movie_id != "__search__"}) > 1
    for session in sessions.values():
        assert session[0].event_type == EventType.SEARCHED
        assert session[1].event_type == EventType.VIEW_STARTED
        assert [event.timestamp for event in session] == sorted(event.timestamp for event in session)
        assert [event.progress_seconds for event in session] == sorted(event.progress_seconds for event in session)
        for event in session:
            if event.event_type == EventType.VIEW_FINISHED:
                assert event.progress_seconds > session[1].progress_seconds
