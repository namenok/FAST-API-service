from pydantic import BaseModel, UUID4, Field
from typing import Any, Optional, Dict
from datetime import datetime
from uuid import UUID


class EventCreate(BaseModel):
    event_id: UUID
    occurred_at: datetime
    user_id: str
    event_type: str
    properties: Optional[dict[str, Any]] = None

    class Config:
        orm_mode = True


class EventSchema(BaseModel):
    event_id: UUID4
    occurred_at: datetime
    user_id: str = Field(..., min_length=1)
    event_type: str = Field(..., min_length=1)
    properties: Optional[Dict] = {}
