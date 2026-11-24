from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Annotated
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, field_validator


class EventType(StrEnum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(StrEnum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


Identifier = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
AvroInt = Annotated[int, Field(strict=True, ge=0, le=2**31 - 1)]
MIN_DATE = date(1970, 1, 1)
MAX_DATE = date(2149, 6, 6)  # ClickHouse Date


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID = Field(default_factory=uuid4)
    user_id: Identifier
    movie_id: Identifier
    event_type: EventType
    timestamp: AwareDatetime
    device_type: DeviceType
    session_id: Identifier
    progress_seconds: AvroInt

    @field_validator("timestamp", mode="before")
    @classmethod
    def require_datetime(cls, value):
        if not isinstance(value, (str, datetime)):
            raise ValueError("send an ISO 8601 datetime with a timezone")
        if isinstance(value, str) and "T" not in value.upper():
            raise ValueError("send an ISO 8601 datetime with a timezone")
        return value

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        utc = value.astimezone(UTC)
        if not MIN_DATE <= utc.date() <= MAX_DATE:
            raise ValueError(f"UTC date must be between {MIN_DATE} and {MAX_DATE}")
        return utc.replace(microsecond=utc.microsecond // 1000 * 1000)

    def avro_record(self) -> dict:
        record = self.model_dump(mode="json")
        record["timestamp"] = self.timestamp
        return record
