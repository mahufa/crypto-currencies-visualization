from typing import NamedTuple, Self

import pandas as pd


class CoinMetaData(NamedTuple):
    coin_id: str
    currency: str

    @classmethod
    def from_ts_frame(cls, ts_frame: pd.DataFrame) -> Self:
        _check_attrs(ts_frame)
        return cls(*[ts_frame.attrs[field] for field in cls._fields])


def make_time_series_frame(data: any, coin_meta: CoinMetaData) -> pd.DataFrame:
    df = pd.DataFrame(data)
    df.attrs['coin_id'], df.attrs['currency'] = tuple(coin_meta)
    return df


def set_dt_index_using_ts_column(ts_frame: pd.DataFrame) -> pd.DataFrame:
    return (ts_frame
            .assign(timestamp=pd.to_datetime(ts_frame.timestamp, unit='ms').dt.floor('s'))
            .set_index('timestamp')
            .rename_axis('datetime'))


def _check_attrs(ts_frame: pd.DataFrame) -> None:
    missing = [attr for attr in CoinMetaData._fields if attr not in ts_frame.attrs]
    if missing:
        raise ValueError(f"Missing DataFrame attrs: {', '.join(missing)}")