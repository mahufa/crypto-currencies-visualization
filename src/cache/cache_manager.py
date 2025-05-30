from datetime import datetime

from sqlalchemy import select, func

from cache.db_manager import get_connection
from cache.db_schema import metadata, historical_data, ohlc_data
from cache.parsers import parse_historical, parse_ohlc
from project_utils import utc_from_cached_ts


class CacheManager:
    def __init__(self,  coin_id: str, currency_symbol: str, table_name: str):
        self.table = metadata.tables.get(table_name)
        if self.table is None:
            raise ValueError(f"Table {table_name} does not exist in metadata.")

        self.coin_id = coin_id
        self.currency_symbol = currency_symbol

    def __enter__(self):
        self.conn = get_connection()
        self.trans = self.conn.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.trans.rollback()
        else:
            self.trans.commit()
        self.conn.close()

    def _base_filter(self, stmt: select) -> select:
        return (stmt
                .where(self.table.c.coin_id == self.coin_id)
                .where(self.table.c.currency_symbol == self.currency_symbol))

    def last_dt(self) -> datetime | None:
        q = (select(func.max(self.table.c.timestamp))
             .select_from(self.table))
        last_ts = self.conn.execute(self._base_filter(q)).scalar()
        return utc_from_cached_ts(last_ts) if last_ts else None

    def fetch_local(self) -> list[dict]:
        q = (select(*[col for col in self.table.c if col.name not in ['coin_id', 'currency_symbol']])
                 .select_from(self.table))

        q_result =  self.conn.execute(self._base_filter(q))

        cols = q_result.keys()
        rows = q_result.fetchall()
        return [{col : val for col, val in zip(cols, row)} for row in rows]

    def upsert(self, raw_data: dict | list):
        normalized_data = self._normalize_data(raw_data)

        data_to_upsert = [{'coin_id': self.coin_id,
                           'currency_symbol': self.currency_symbol,
                           **row} for row in normalized_data]

        stmt = self.table.insert().prefix_with('OR REPLACE')
        self.conn.execute(stmt, data_to_upsert)

    def _normalize_data(self, raw_data: dict | list) -> list[dict]:
        if self.table is historical_data:
            return parse_historical(raw_data)
        elif self.table is ohlc_data:
            return parse_ohlc(raw_data)
        else:
            raise RuntimeError(f"No parser for table {self.table.name!r}")