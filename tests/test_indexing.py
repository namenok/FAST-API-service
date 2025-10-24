from sqlalchemy import inspect

def test_event_id_unique_index_exists(db_session):
    inspector = inspect(db_session.bind)
    pk_constraint = inspector.get_pk_constraint("events") or {}
    constrained_columns = pk_constraint.get("constrained_columns", [])
    assert "event_id" in constrained_columns