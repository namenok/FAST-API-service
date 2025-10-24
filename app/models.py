from sqlalchemy import Column, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID, JSONB
from .database import Base
import uuid

class Event(Base):
    __tablename__ = "events"

    event_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    occurred_at = Column(TIMESTAMP, nullable=False)
    user_id = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    properties = Column(JSONB, nullable=True)



