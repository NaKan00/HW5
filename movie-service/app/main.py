import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request

from .continuous_generator import ContinuousGenerator
from .generator import demo_events
from .kafka_producer import EventPublisher, PublicationError
from .models import MovieEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.publisher = EventPublisher()
    app.state.generator = ContinuousGenerator(app.state.publisher.publish)
    try:
        yield
    finally:
        await app.state.generator.stop()


app = FastAPI(title="Cinema Movie Events", lifespan=lifespan)


def publish(request: Request, event: MovieEvent) -> None:
    try:
        request.app.state.publisher.publish(event)
    except PublicationError as error:
        log.error(json.dumps({"action": "publication_failed", "event_id": str(event.event_id), "reason": str(error)}))
        raise HTTPException(503, "Event could not be published. Retry with the same event_id.") from error


@app.get("/health")
def health():
    return {"status": "ready"}


@app.post("/events", status_code=201)
def create_event(event: MovieEvent, request: Request):
    publish(request, event)
    return {"event_id": str(event.event_id), "status": "published"}


@app.post("/generator/demo", status_code=201)
def generate_demo(request: Request):
    events = demo_events()
    for event in events:
        publish(request, event)
    return {
        "events_published": len(events),
        "first_date": min(event.timestamp.date() for event in events),
        "last_date": max(event.timestamp.date() for event in events),
        "dates": sorted({event.timestamp.date() for event in events}),
    }


@app.post("/generator/start")
async def start_generator(request: Request):
    return await request.app.state.generator.start()


@app.post("/generator/stop")
async def stop_generator(request: Request):
    return await request.app.state.generator.stop()


@app.get("/generator/status")
async def generator_status(request: Request):
    return request.app.state.generator.status()
