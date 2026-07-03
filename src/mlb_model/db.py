from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import inspect, text
from sqlmodel import Session, SQLModel, create_engine

from mlb_model import models  # noqa: F401 — registers all tables on SQLModel.metadata
from mlb_model.config import get_settings


settings = get_settings()
engine = create_engine(settings.database_url, echo=False)


def _apply_additive_migrations(target_engine=None) -> None:
    """Add columns that exist on the models but not yet in the SQLite file.

    ``create_all`` only creates missing tables — it never alters existing
    ones. This keeps older local databases working when a new column (e.g.
    ``marketsnapshot.is_closing_line``) is added to a model. Only additive,
    nullable-or-defaulted column changes are supported.
    """
    target_engine = target_engine or engine
    inspector = inspect(target_engine)
    with target_engine.begin() as connection:
        for table in SQLModel.metadata.sorted_tables:
            if not inspector.has_table(table.name):
                continue
            existing = {column["name"] for column in inspector.get_columns(table.name)}
            for column in table.columns:
                if column.name in existing:
                    continue
                ddl = (
                    f'ALTER TABLE "{table.name}" ADD COLUMN "{column.name}" '
                    f"{column.type.compile(target_engine.dialect)}"
                )
                if not column.nullable:
                    default = "0" if str(column.type) in {"BOOLEAN", "INTEGER", "FLOAT", "NUMERIC"} else "''"
                    ddl += f" NOT NULL DEFAULT {default}"
                connection.execute(text(ddl))


def init_db(target_engine=None) -> None:
    target_engine = target_engine or engine
    SQLModel.metadata.create_all(target_engine)
    _apply_additive_migrations(target_engine)


@contextmanager
def session_scope() -> Session:
    with Session(engine) as session:
        yield session
