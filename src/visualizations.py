import pandas as pd
import mplfinance as mpf
from matplotlib import pyplot as plt
from matplotlib.dates import DayLocator, DateFormatter

from project_utils import CoinMetaData, set_dt_index_using_ts_column


class TimeSeriesPlotter:
    def __init__(
            self,
            time_series_frame: pd.DataFrame,
            plot_size: tuple[int,int] = (10, 5),
    ):
        self._time_series_frame = time_series_frame.pipe(set_dt_index_using_ts_column)
        self._coin_meta = CoinMetaData.from_ts_frame(time_series_frame)
        self.plot_size = plot_size

    @staticmethod
    def _format_plot_date(ax: plt.Axes) -> None:
        ax.xaxis.set_major_locator(DayLocator(interval=3))
        ax.xaxis.set_major_formatter(DateFormatter('%m/%d'))
        ax.figure.autofmt_xdate()


class HistPlotter(TimeSeriesPlotter):
    def plot_price(self):
        return self._plot_by_columns(['price'])

    def plot_total_volume(self):
        return self._plot_by_columns(['total_volume'])

    def plot_market_cap(self):
        return self._plot_by_columns(['market_cap'])

    def _plot_by_columns(
            self,
            columns: list[str],
    ):
        title = self._plot_title(columns)

        ax = self._time_series_frame.plot(
            y=columns,
            figsize= self.plot_size,
            title=f'{self._coin_meta.coin_id}: {title}',
            xlabel = 'Date',
            ylabel = self._coin_meta.currency,
            grid = True,
        )

        self._format_plot_date(ax)

        return ax

    @staticmethod
    def _plot_title(plotted_columns: list[str]):
        if not plotted_columns:
            raise ValueError('Must have at least one column')

        return plotted_columns[0] if len(plotted_columns) == 1 \
            else ', '.join(plotted_columns)


class OHLCPlotter(TimeSeriesPlotter):
    _columns = ['open', 'high', 'low', 'close', 'total_volume']

    def __init__(
            self,
            ohlc_df: pd.DataFrame,
            plot_size = (10, 5),
    ):
        self._check_if_ohlc_df(ohlc_df)
        super().__init__(ohlc_df, plot_size)

    def plot_candlestick(self, has_volume: bool = False):
        title = 'OHLCV' if has_volume else 'OHLC'

        fig, axes = mpf.plot(
            self._time_series_frame,
            figsize=self.plot_size,
            title=f'{self._coin_meta.coin_id}: {title}',
            xlabel='Date',
            ylabel=self._coin_meta.currency,
            type='candle',
            volume=has_volume,
            columns=self._columns,
            returnfig=True,
        )

        main_ax = axes[0]
        self._format_plot_date(main_ax)

        return fig, axes

    @staticmethod
    def _check_if_ohlc_df(df):
        if not all([
            'open' in df.columns,
            'high' in df.columns,
            'low' in df.columns,
            'close' in df.columns,
            'total_volume' in df.columns,
        ]):
            raise ValueError('OHLC frame must have open, high, low, close, and total_volume columns')
