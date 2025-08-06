from matplotlib import pyplot as plt
from matplotlib.dates import DayLocator, DateFormatter
from matplotlib.ticker import FuncFormatter


class AxFormatter:
    def __init__(self,
                 *,
                 interval: int = 3,
                 date_format: str = '%b %d',
                 rotation: int = 45,
                 should_format_date: bool = True,
                 ):
        self._interval = interval
        self._date_format = date_format
        self._rotation = rotation
        self.should_format_date = should_format_date

    def format_ax(self, ax: plt.Axes):
        self._format_ax_date(ax)
        self._format_ax_prices(ax)


    def _format_ax_date(
            self,
            ax: plt.Axes,
    ) -> None:
        ax.xaxis.set_major_locator(DayLocator(interval=self._interval))
        ax.figure.autofmt_xdate(rotation=self._rotation)
        if self.should_format_date:
            ax.xaxis.set_major_formatter(DateFormatter(self._date_format))

    def _format_ax_prices(self, ax: plt.Axes):
        ax.yaxis.set_major_formatter(FuncFormatter(self._convert_large_number_to_readable))

    @staticmethod
    def _convert_large_number_to_readable(x: float, pos) -> str:
        if x >= 1e12:
            return f'{x / 1e12:.1f}T'  # Trillions
        elif x >= 1e9:
            return f'{x / 1e9:.1f}B'  # Billions
        elif x >= 1e6:
            return f'{x / 1e6:.1f}M'  # Millions
        elif x >= 1e3:
            return f'{x / 1e3:.1f}K'  # Thousands
        else:
            return f'{x:.0f}'
