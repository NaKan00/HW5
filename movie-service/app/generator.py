from datetime import UTC, date, datetime, time, timedelta
from uuid import UUID, uuid5

from .models import DeviceType, EventType, MovieEvent

DEMO_START = date(2026, 4, 1)
DEMO_NAMESPACE = UUID("64e3520e-361b-4de7-a8c3-5ff676430f70")


def session_events(
    user: str,
    session: str,
    movie: str,
    device: DeviceType,
    start: datetime,
    duration: int | None,
    like: bool,
):
    steps = [
        (-15, EventType.SEARCHED, 0),
        (0, EventType.VIEW_STARTED, 0),
        (300, EventType.VIEW_PAUSED, 300),
        (330, EventType.VIEW_RESUMED, 300),
    ]
    if duration:
        steps.append((duration + 30, EventType.VIEW_FINISHED, duration))
        if like:
            steps.append((duration + 45, EventType.LIKED, duration))
    for offset, event_type, progress in steps:
        yield MovieEvent(
            event_id=uuid5(DEMO_NAMESPACE, f"{session}/{event_type}"),
            user_id=user,
            movie_id="__search__" if event_type == EventType.SEARCHED else movie,
            event_type=event_type,
            timestamp=start + timedelta(seconds=offset),
            device_type=device,
            session_id=session,
            progress_seconds=progress,
        )


def demo_events(start: date = DEMO_START) -> list[MovieEvent]:
    dates = [
        (0, "a", 10),
        (1, "a", 5),
        (1, "b", 4),
        (2, "b", 2),
        (3, "c", 3),
        (4, "c", 2),
        (5, "c", 1),
        (6, "c", 1),
        (7, "a", 3),
        (8, "b", 1),
    ]
    events = []
    devices = list(DeviceType)
    for day, cohort, users in dates:
        for index in range(users):
            user = f"demo-{start}-{cohort}-{index}"
            session = f"{user}-{day}"
            movie, duration = ("orbit", 3600) if index < 6 else ("harbor", 5400)
            if day == 0 and index == 9:
                duration = None
            timestamp = datetime.combine(start + timedelta(days=day), time(12, index * 2), UTC)
            events.extend(session_events(user, session, movie, devices[index % 4], timestamp, duration, index % 2 == 0))
    return events
