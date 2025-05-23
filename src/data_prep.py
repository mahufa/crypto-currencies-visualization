import pandas as pd

from project_utils.frames import get_frame_attr


def set_date_index(ts_df: pd.DataFrame) -> pd.DataFrame:
    return (ts_df
                .assign(timestamp = pd.to_datetime(ts_df.timestamp, unit='ms').dt.floor('s'))
                .set_index('timestamp')
                .rename_axis('datetime'))


def resample_data(ts_frame: pd.DataFrame, freq : str='1D') -> pd.DataFrame:
    return ts_frame.resample(freq).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'total_volume': 'sum',
        'market_cap': 'last',
    })


def get_complete_session(hist_df: pd.DataFrame, ohlc_df: pd.DataFrame, freq : str='1D') -> pd.DataFrame:
    hist_coin, hist_currency = get_frame_attr(hist_df)
    ohlc_coin, ohlc_currency = get_frame_attr(ohlc_df)
    if (hist_coin, hist_currency) != (ohlc_coin, ohlc_currency):
        raise ValueError('hist_df and ohlc_df must have same coin_id and currency_symbol')

    sessions = (hist_df
                .merge(ohlc_df, on='timestamp', how='outer')
                .pipe(set_date_index)
                .pipe(resample_data, freq=freq)
                )

    sessions.attrs = hist_df.attrs

    return sessions