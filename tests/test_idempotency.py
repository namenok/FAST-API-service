from app.models import Event
from datetime import datetime
import uuid

def test_event_idempotency(db_session):
    event_id = uuid.uuid4()
    event_data = {
        "event_id": event_id,
        "occurred_at": datetime.utcnow(),
        "user_id": "u1",
        "event_type": "click",
        "properties": {"a": 1}
    }

    e1 = Event(**event_data)
    db_session.add(e1)
    db_session.commit()

    e2 = Event(**event_data)
    db_session.add(e2)
    try:
        db_session.commit()
    except Exception as e:
        db_session.rollback()

    events = db_session.query(Event).filter_by(event_id=event_id).all()
    assert len(events) == 1
