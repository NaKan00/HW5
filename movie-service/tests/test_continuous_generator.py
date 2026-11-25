from datetime import UTC, datetime, timedelta
from threading import Event

import pytest
from fastapi.testclient import TestClient

from app import main
from app.continuous_generator import live_session
from app.models import DeviceType, EventType


@pytest.fixture
def publisher(monkeypatch):
    class RecordingPublisher:
        def __init__(self):
            self.events = []
            self.published = Event()

        def publish(self, event):
            self.events.append(event)
            self.published.set()

    publisher = RecordingPublisher()
    monkeypatch.setattr(main, "EventPublisher", lambda: publisher)
    return publisher


def test_start_is_idempotent_and_stop_joins_worker(publisher):
    with TestClient(main.app) as client:
        assert client.get("/generator/status").json() == {"running": False, "events_published": 0}

        started = client.post("/generator/start")
        assert started.status_code == 200
        assert started.json()["running"] is True
        generator = client.app.state.generator
        task = generator._task
        assert client.post("/generator/start").json()["running"] is True
        assert generator._task is task
        assert publisher.published.wait(timeout=3)
        assert client.get("/generator/status").json()["running"] is True

        stopped = client.post("/generator/stop").json()
        assert stopped == {"running": False, "events_published": len(publisher.events)}
        assert stopped["events_published"] > 0
        assert task.done()
        assert client.post("/generator/stop").json() == stopped
        assert client.get("/generator/status").json() == stopped


def test_shutdown_stops_active_worker(publisher):
    with TestClient(main.app) as client:
        assert client.post("/generator/start").json()["running"] is True
        assert publisher.published.wait(timeout=3)
        generator = client.app.state.generator
        task = generator._task

    assert task.done()
    assert generator.status() == {"running": False, "events_published": len(publisher.events)}


def test_live_sessions_use_current_utc_and_ordered_progress():
    earliest = datetime.now(UTC) - timedelta(milliseconds=1)
    sessions = [list(live_session(number)) for number in range(8)]
    latest = datetime.now(UTC)
    events = [event for session in sessions for event in session]
    assert len({event.user_id for event in events}) == 8
    assert len({event.movie_id for event in events if event.event_type != EventType.SEARCHED}) == 3
    assert {event.device_type for event in events} == set(DeviceType)
    assert {event.event_type for event in events} == set(EventType)
    assert len({event.session_id for event in events}) == len(sessions)

    for session in sessions:
        search, start, *remaining = session
        assert search.event_type == EventType.SEARCHED
        assert (search.movie_id, search.progress_seconds) == ("__search__", 0)
        assert start.event_type == EventType.VIEW_STARTED
        assert start.progress_seconds == 0
        viewing = [event for event in remaining if event.event_type != EventType.LIKED]
        assert [event.event_type for event in viewing] in (
            [EventType.VIEW_FINISHED],
            [EventType.VIEW_PAUSED, EventType.VIEW_RESUMED, EventType.VIEW_FINISHED],
        )
        assert viewing[-1].progress_seconds > start.progress_seconds
        progress = [event.progress_seconds for event in session]
        assert progress == sorted(progress)
        assert all(event.timestamp.tzinfo is UTC and earliest <= event.timestamp <= latest for event in session)
