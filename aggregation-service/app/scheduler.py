import asyncio
import logging
from datetime import UTC, datetime
from threading import Lock
from time import monotonic

from .config import AGGREGATION_INTERVAL, EXPORT_HOUR
from .postgres_writer import metric_rows, write_metrics

log = logging.getLogger(__name__)


class Analytics:
    def __init__(self, store, exporter):
        self.store = store
        self.exporter = exporter
        self.lock = Lock()
        self.exported_on = {}

    def aggregate(self, day):
        with self.lock:
            start = monotonic()
            log.info("action=aggregation_started date=%s", day)
            result = self.store.calculate(day)
            calculated_at = datetime.now(UTC)
            self.store.write(day, result, calculated_at)
            rows = metric_rows(day, result, calculated_at)
            write_metrics(day, rows)
            duration = round((monotonic() - start) * 1000)
            log.info(
                "action=aggregation_complete date=%s events_processed=%s postgres_rows=%s duration_ms=%s",
                day, result.events_processed, len(rows), duration,
            )
            return {
                "date": day, "events_processed": result.events_processed,
                "metrics_written": len(rows), "duration_ms": duration,
            }

    def export(self, day):
        with self.lock:
            return self.exporter.export(day)

    def run_cycle(self):
        now = datetime.now(UTC)
        today = now.date()
        for day in self.store.dates():
            try:
                self.aggregate(day)
            except Exception as error:
                log.error("action=scheduled_aggregation_failed date=%s error=%s", day, type(error).__name__)
                continue
            if day < today and now.hour >= EXPORT_HOUR and self.exported_on.get(day) != today:
                try:
                    self.export(day)
                    self.exported_on[day] = today
                except Exception as error:
                    log.error("action=scheduled_export_failed date=%s error=%s", day, type(error).__name__)


async def run_scheduler(analytics: Analytics, stop: asyncio.Event):
    while not stop.is_set():
        try:
            await asyncio.to_thread(analytics.run_cycle)
        except Exception as error:
            log.error("action=scheduler_cycle_failed error=%s", type(error).__name__)
        try:
            await asyncio.wait_for(stop.wait(), timeout=AGGREGATION_INTERVAL)
        except TimeoutError:
            pass
