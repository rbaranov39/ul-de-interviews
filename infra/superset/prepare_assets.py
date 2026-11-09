from uuid import UUID

from superset.app import create_app


CLICKHOUSE_DATABASE_NAME = "ClickHouse Analytics"
CLICKHOUSE_DATABASE_UUID = UUID("a5808f90-fc92-43b6-9c22-f766a723cd42")


app = create_app()

with app.app_context():
    from superset import db
    from superset.models.core import Database

    database = (
        db.session.query(Database)
        .filter_by(database_name=CLICKHOUSE_DATABASE_NAME)
        .one()
    )
    database.uuid = CLICKHOUSE_DATABASE_UUID
    db.session.commit()

print(
    "Prepared Superset assets for "
    f"{CLICKHOUSE_DATABASE_NAME} ({CLICKHOUSE_DATABASE_UUID})"
)
