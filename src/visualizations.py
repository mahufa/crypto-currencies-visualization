import pandas as pd

from project_utils import CoinMetaData


class TimeSeriesPlotter:
    def __init__(self, time_series_frame: pd.DataFrame):
        self._time_series_frame = time_series_frame
        self._coin_meta = CoinMetaData.from_ts_frame(time_series_frame)
        self.plot_size = (10, 5)

    def make_price_plot(self):
        return self._make_plot_by_columns(['price'])

    def make_total_volume_plot(self):
        return self._make_plot_by_columns(['total_volume'])

    def make_market_cap_plot(self):
        return self._make_plot_by_columns(['market_cap'])

    def _make_plot_by_columns(
            self,
            columns: list[str],
    ):
        title = self._plot_title(columns)

        ax = self._time_series_frame.plot(
            y=columns,
            figsize= self.plot_size,
            title=f'{self._coin_meta.coin_id}: {title}',
            grid=True,
            xlabel = 'Date',
            ylabel = self._coin_meta.currency,
        )
        return ax

    @staticmethod
    def _plot_title(plotted_columns: list[str]):
        if not plotted_columns:
            raise ValueError('Must have at least one column')

        return plotted_columns[0] if len(plotted_columns) == 1 \
            else ', '.join(plotted_columns)