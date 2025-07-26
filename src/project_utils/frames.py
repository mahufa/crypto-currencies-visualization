import pandas as pd


def make_time_series_frame(data: any, coin_id: str, currency_symbol: str) -> pd.DataFrame:
    df = pd.DataFrame(data)
    df.attrs['coin_id'] = coin_id
    df.attrs['currency_symbol'] = currency_symbol
    return df


def get_id_and_currency(ts_frame: pd.DataFrame) -> tuple[str, str]:
    _check_attrs(ts_frame)
    return ts_frame.attrs['coin_id'], ts_frame.attrs['currency_symbol']


def get_df_with_date_index_from_ts_column(ts_frame: pd.DataFrame) -> pd.DataFrame:
    return (ts_frame
            .assign(timestamp=pd.to_datetime(ts_frame.timestamp, unit='ms').dt.floor('s'))
            .set_index('timestamp')
            .rename_axis('datetime'))


def _check_attrs(ts_frame: pd.DataFrame) -> None:
    missing = [k for k in ('coin_id', 'currency_symbol') if k not in ts_frame.attrs]
    if missing:
        raise ValueError(f"Missing DataFrame attrs: {', '.join(missing)}")