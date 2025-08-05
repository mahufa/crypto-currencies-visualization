import pandas as pd

from project_utils import set_dt_index_using_ts_column, CoinMetaData

class OHLCSessionMaker:
    def __init__(
            self,
            hist_df: pd.DataFrame,
            ohlc_df: pd.DataFrame,
            freq: str = '1D'
    ):
        self._validate_dfs(hist_df, ohlc_df)

        self.hist_df = hist_df
        self.ohlc_df = ohlc_df
        self.freq = freq

    def make_session(self) -> pd.DataFrame:
        sessions = (
            self.hist_df
                    .merge(self.ohlc_df, on='timestamp', how='outer')
                    .pipe(set_dt_index_using_ts_column)
                    .pipe(self._resample_data)
                    .dropna()
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
        hist_meta = CoinMetaData.from_ts_frame(hist_df)
        ohlc_meta= CoinMetaData.from_ts_frame(ohlc_df)

        if hist_meta != ohlc_meta:
            raise ValueError('hist_df and ohlc_df must have same coin_id and currency_symbol')

    @staticmethod
    def _check_if_any_df_empty(
            hist_df: pd.DataFrame,
            ohlc_df: pd.DataFrame,
    ) -> None:
        if hist_df.empty or ohlc_df.empty:
            raise ValueError("Empty dataframes not supported")