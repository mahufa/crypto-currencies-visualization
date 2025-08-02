from sqlalchemy import create_engine, Engine, Connection, Table

from .db_schema import metadata

_engine: Engine | None = None


def get_engine(db_url="sqlite:///cache.db") -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(db_url)
        metadata.create_all(_engine)
    return _engine


def get_connection() -> Connection:
    return get_engine().connect()


def get_table_or_throw(table_name: str) -> Table:
    table = metadata.tables.get(table_name)
    if table is None:
        raise ValueError(f"Table {table_name} does not exist in metadata.")
    return table
