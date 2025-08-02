from datetime import datetime

from sqlalchemy import select, func, Column, Table

from cache.db_manager import get_connection, get_table_or_throw
from cache.parsers import normalize_data
from project_utils import utc_from_cached_ts, CoinMetaData


class CacheManager:
    def __init__(
            self,
            coin_meta: CoinMetaData,
            table_name: str
    ):
        self._table = get_table_or_throw(table_name)
        self._coin_meta = coin_meta

    def __enter__(self):
        self._conn = get_connection()
        self._tran = self._conn.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._tran.rollback()
        else:
            self._tran.commit()
        self._conn.close()

    def last_dt(self) -> datetime | None:
        q = (select(func.max(self._table.c.timestamp))
             .select_from(self._table))
        last_ts = self._conn.execute(self._filter_using_coin_meta(q)).scalar()
        return utc_from_cached_ts(last_ts) if last_ts else None

    def fetch_local(self) -> list[dict]:
        q_result = self._get_cursor_with_table_data()

        cols = q_result.keys()
        rows = q_result.fetchall()
        return [{col: val for col, val in zip(cols, row)} for row in rows]

    def upsert(self, raw_data: dict | list):
        normalized_data = normalize_data(raw_data, self._table)
        data_to_upsert = self._prepare_for_upsert(normalized_data)

        stmt = self._table.insert().prefix_with('OR REPLACE') # SQLite only!
        self._conn.execute(stmt, data_to_upsert)

    def _get_cursor_with_table_data(self):
        column_query = (select
                            (*self._get_table_data_columns())
                        .select_from(self._table))
        filtering_query = self._filter_using_coin_meta(column_query)
        return self._conn.execute(filtering_query)

    def _get_table_data_columns(self) -> list[Column]:
        return [col for col in self._table.c if col.name not in [
                                'coin_id',
                                'currency_symbol']]

    def _filter_using_coin_meta(self, stmt: select) -> select:
        return (stmt
                .where(self._table.c.coin_id == self._coin_meta.coin_id)
                .where(self._table.c.currency_symbol == self._coin_meta.currency))

    def _prepare_for_upsert(self, normalized_data: list[dict]) -> list[dict]:
        return [{'coin_id': self._coin_meta.coin_id,
          'currency_symbol': self._coin_meta.currency,
          **row} for row in normalized_data]
