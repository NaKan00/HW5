import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import date

import clickhouse_connect
from fastapi import FastAPI, HTTPException, Request

from .clickhouse_queries import ClickHouseStore
from .config import CLICKHOUSE
from .exporter import Exporter, NoMetricsError, s3_client
from .scheduler import Analytics, run_scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = clickhouse_connect.get_client(**CLICKHOUSE)
    app.state.analytics = Analytics(ClickHouseStore(client), Exporter(s3_client()))
    stop = asyncio.Event()
    task = asyncio.create_task(run_scheduler(app.state.analytics, stop))
    try:
        yield
    finally:
        stop.set()
        await task
        client.close()


app = FastAPI(title="Cinema Analytics", lifespan=lifespan)


def checked_date(day: date) -> date:
    if not date(1970, 1, 1) <= day <= date(2149, 6, 6):
        raise HTTPException(422, "Date is outside the supported ClickHouse Date range")
    return day


@app.get("/health")
def health():
    return {"status": "ready"}


@app.post("/aggregate/{day}")
def aggregate(day: date, request: Request):
    try:
        return request.app.state.analytics.aggregate(checked_date(day))
    except HTTPException:
        raise
    except Exception as error:
        log.error("action=aggregation_failed date=%s error=%s", day, type(error).__name__)
        raise HTTPException(503, "Aggregation failed; retry this date") from error


@app.post("/export/{day}")
def export(day: date, request: Request):
    try:
        return request.app.state.analytics.export(checked_date(day))
    except HTTPException:
        raise
    except NoMetricsError as error:
        raise HTTPException(404, "Aggregate this date before exporting") from error
    except Exception as error:
        log.error("action=export_failed date=%s error=%s", day, type(error).__name__)
        raise HTTPException(503, "Export failed; retry this date") from error
