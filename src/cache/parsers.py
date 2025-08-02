from sqlalchemy import Table

from cache.db_schema import historical_data, ohlc_data


def normalize_data(raw_data: dict | list, table: Table) -> list[dict]:
    if table is historical_data:
        return _parse_historical(raw_data)
    elif table is ohlc_data:
        return _parse_ohlc(raw_data)
    else:
        raise RuntimeError(f"No parser for table {table.name!r}")


def _parse_historical(data: dict) -> list[dict]:
    if not data or 'prices' not in data:
        return []
    else:
        return [
            {
                'timestamp': ts,
                'price': pr,
                'market_cap': mc,
                'total_volume': tv
            } for
                (ts, pr),
                (_, mc),
                (_, tv)
            in zip(
                data['prices'],
                data['market_caps'],
                data['total_volumes']
            )
        ]


def _parse_ohlc(data: list) -> list[dict]:
    if not data:
        return []
    else:
        return [
            {
                'timestamp': ts,
                'open': op,
                'high': hi,
                'low': lo,
                'close': cl
            }
            for
                ts,
                op,
                hi,
                lo,
                cl
            in data
        ]
