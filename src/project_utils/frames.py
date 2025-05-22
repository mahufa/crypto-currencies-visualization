import pandas as pd


def make_time_series_frame(data: any, coin_id: str, currency_symbol: str) -> pd.DataFrame:
    df = pd.DataFrame(data)
    df.attrs['coin_id'] = coin_id
    df.attrs['currency_symbol'] = currency_symbol
    return df

def get_frame_attr(ts_frame: pd.DataFrame) -> tuple[str,str]:
    missing = [k for k in ('coin_id', 'currency_symbol') if k not in ts_frame.attrs]
    if missing:
        raise ValueError(f"Missing DataFrame attrs: {', '.join(missing)}")
    return ts_frame.attrs['coin_id'], ts_frame.attrs['currency_symbol']