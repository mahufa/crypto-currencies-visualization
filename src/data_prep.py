import pandas as pd

from project_utils import (
    get_id_and_currency,
    get_df_with_date_index_from_ts_column
)


class OHLCSessionMaker:
    def __init__(self, hist_df: pd.DataFrame, ohlc_df: pd.DataFrame, freq: str = '1D'):
        self._validate_dfs(hist_df, ohlc_df)

        self.hist_df = hist_df
        self.ohlc_df = ohlc_df
        self.freq = freq

    def make_session(self) -> pd.DataFrame:
        sessions = (self.hist_df
                    .merge(self.ohlc_df, on='timestamp', how='outer')
                    .pipe(get_df_with_date_index_from_ts_column)
                    .pipe(self._resample_data)
                    )
        sessions.attrs = self.hist_df.attrs
        return sessions

    def _resample_data(self, ts_frame: pd.DataFrame) -> pd.DataFrame:
        return ts_frame.resample(self.freq).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'total_volume': 'sum',
            'market_cap': 'last',
        })

    @staticmethod
    def _validate_dfs(
            hist_df: pd.DataFrame,
            ohlc_df: pd.DataFrame,
    ) -> None:
        OHLCSessionMaker._check_if_any_df_empty(hist_df, ohlc_df)
        OHLCSessionMaker._check_if_dfs_have_the_same_attrs(hist_df, ohlc_df)

    @staticmethod
    def _check_if_dfs_have_the_same_attrs(
            hist_df: pd.DataFrame,
            ohlc_df: pd.DataFrame,
    ) -> None:
        hist_coin, hist_currency = get_id_and_currency(hist_df)
        ohlc_coin, ohlc_currency = get_id_and_currency(ohlc_df)

        if (hist_coin, hist_currency) != (ohlc_coin, ohlc_currency):
            raise ValueError('hist_df and ohlc_df must have same coin_id and currency_symbol')

    @staticmethod
    def _check_if_any_df_empty(
            hist_df: pd.DataFrame,
            ohlc_df: pd.DataFrame,
    ) -> None:
        if hist_df.empty or ohlc_df.empty:
            raise ValueError("Empty dataframes not supported")