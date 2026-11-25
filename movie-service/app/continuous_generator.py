import asyncio
import logging
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from uuid import uuid4

from .kafka_producer import PublicationError
from .models import DeviceType, EventType, MovieEvent

log = logging.getLogger(__name__)
EVENT_INTERVAL = 0.5
MOVIES = (("orbit", 3600), ("harbor", 5400), ("signal", 4200))


def live_session(number: int) -> Iterator[MovieEvent]:
    user = f"live-user-{number % 8 + 1}"
    session = f"live-session-{uuid4()}"
    movie, duration = MOVIES[number % len(MOVIES)]
    devices = list(DeviceType)
    device = devices[number % len(devices)]
    steps = [(EventType.SEARCHED, 0), (EventType.VIEW_STARTED, 0)]
    if number % 2 == 0:
        steps.extend([(EventType.VIEW_PAUSED, 300), (EventType.VIEW_RESUMED, 300)])
    steps.append((EventType.VIEW_FINISHED, duration))
    if number % 3 == 0:
        steps.append((EventType.LIKED, duration))

    for event_type, progress in steps:
        yield MovieEvent(
            user_id=user,
            movie_id="__search__" if event_type == EventType.SEARCHED else movie,
            event_type=event_type,
            timestamp=datetime.now(UTC),
            device_type=device,
            session_id=session,
            progress_seconds=progress,
        )


class ContinuousGenerator:
    def __init__(self, publish: Callable[[MovieEvent], None]):
        self._publish = publish
        self._stop = asyncio.Event()
        self._lifecycle = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._events_published = 0
        self._next_session = 0

    def status(self) -> dict[str, bool | int]:
        return {
            "running": self._task is not None and not self._task.done(),
            "events_published": self._events_published,
        }

    async def start(self) -> dict[str, bool | int]:
        async with self._lifecycle:
            if self._task is None or self._task.done():
                self._stop.clear()
                self._task = asyncio.create_task(self._run(), name="continuous-movie-generator")
            return self.status()

    async def stop(self) -> dict[str, bool | int]:
        async with self._lifecycle:
            self._stop.set()
            if self._task is not None:
                # Let an in-flight publication finish even if the caller disconnects.
                await asyncio.shield(self._task)
                self._task = None
            return self.status()

    async def _run(self) -> None:
        while not self._stop.is_set():
            events = live_session(self._next_session)
            self._next_session += 1
            for event in events:
                if self._stop.is_set():
                    return
                try:
                    await asyncio.to_thread(self._publish, event)
                except PublicationError as error:
                    log.warning("action=generator_publication_failed event_id=%s reason=%s", event.event_id, error)
                    await self._pace()
                    # A failed step can leave the session incomplete.
                    break
                self._events_published += 1
                await self._pace()

    async def _pace(self) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=EVENT_INTERVAL)
        except TimeoutError:
            pass
