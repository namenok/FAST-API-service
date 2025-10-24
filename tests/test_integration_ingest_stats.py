from datetime import datetime
import uuid
from app.models import Event

def test_ingest_and_get_dau(client, db_session):

    events = [
        Event(
            event_id=str(uuid.uuid4()),
            occurred_at=datetime.utcnow(),
            user_id="u1",
            event_type="login",
            properties={"page": "home"}
        ),
        Event(
            event_id=str(uuid.uuid4()),
            occurred_at=datetime.utcnow(),
            user_id="u2",
            event_type="login",
            properties={"page": "about"}
        ),
        Event(
            event_id=str(uuid.uuid4()),
            occurred_at=datetime.utcnow(),
            user_id="u1",
            event_type="click",
            properties={"btn": "ok"}
        )
    ]

    db_session.add_all(events)
    db_session.commit()

    today = datetime.utcnow().date().isoformat()
    resp = client.get(f"/stats/dau?from_date={today}&to_date={today}")
    assert resp.status_code == 200

    data = resp.json()
    print("Events in response:", data)
    assert isinstance(data, list)
    assert data[0]["dau"] == 2
