from datetime import datetime
from typing import Any, TypeAlias

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from cache import CacheManager
from config import PRICE_PRECISION
from project_utils import CoinMetaData, days_to_call

JSON: TypeAlias = dict[str, Any] | list[Any] | str | int | float | bool | None


class PrintingRetry(Retry):
    def sleep(self, response=None):
        retry_after = self.get_retry_after(response)
        backoff     = self.get_backoff_time()
        sleep_time  = retry_after if retry_after else backoff

        if sleep_time:
            url    = getattr(response, "url", "unknown") if response else "unknown"
            reason = f"Retry-After={retry_after}s" if retry_after else f"back-off={sleep_time:.1f}s"
            print(f"⚠️  Rate-limit/back-off triggered for {url} — {reason}")

        super().sleep(response)

_retry_policy = PrintingRetry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=(408, 429, 500, 502, 503, 504),
    allowed_methods={"GET"},
    respect_retry_after_header=True,
)

_adapter = HTTPAdapter(
    max_retries=_retry_policy,
    pool_connections=20,
    pool_maxsize=20,
)

_session = requests.Session()
_session.mount("https://", _adapter)


def get(url: str,
        params: dict[str, Any] = None,
        *,
        timeout: tuple[float, float] = (5, 30)) -> JSON:
    response = _session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def get_time_series(url: str,
                    coin_meta: CoinMetaData,
                    starting_dt: datetime | None,
                    table_name: str) -> dict | Any | None:

    with (CacheManager(coin_meta, table_name) as cache):
        days = days_to_call(starting_dt,
                            cache.last_dt(),
                            is_ohlc = table_name=='ohlc_data')
        if days != 0:
            params = {"vs_currency": coin_meta.currency,
                      "precision": PRICE_PRECISION,
                      "days": days}

            raw_data = get(url, params)
            cache.upsert(raw_data)

        return cache.fetch_local()

__all__ = ["get", "get_time_series"]