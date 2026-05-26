import logging
from collections.abc import Iterator
from urllib.parse import quote

import requests
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from app.config import Settings, get_settings

logger = logging.getLogger(__name__)

BASE_URL = "https://prices.azure.com/api/retail/prices"


class RetryableHTTPError(Exception):
    """Raised for 429/5xx responses so tenacity will retry them."""

    def __init__(self, status_code: int, retry_after: float | None = None):
        super().__init__(f"retryable HTTP {status_code}")
        self.status_code = status_code
        self.retry_after = retry_after


def _is_retryable_status(status: int) -> bool:
    return status == 429 or 500 <= status < 600


def _honor_retry_after(state: RetryCallState) -> float:
    """If the last exception carried a Retry-After hint, honor it; else fall back to default wait."""
    exc = state.outcome.exception() if state.outcome else None
    if isinstance(exc, RetryableHTTPError) and exc.retry_after is not None:
        return max(0.0, float(exc.retry_after))
    return wait_exponential_jitter(initial=1, max=30, jitter=1)(state)


def _build_initial_url(settings: Settings) -> str:
    params = [
        f"api-version={quote(settings.azure_api_version)}",
        f"currencyCode='{quote(settings.azure_currency)}'",
    ]
    if settings.azure_optional_filter:
        params.append(f"$filter={quote(settings.azure_optional_filter)}")
    return f"{BASE_URL}?{'&'.join(params)}"


def fetch_pages(
    settings: Settings | None = None,
    session: requests.Session | None = None,
) -> Iterator[tuple[int, list[dict]]]:
    """Yield (page_index, items) for every page in the Azure Retail Prices response chain."""
    settings = settings or get_settings()
    own_session = session is None
    session = session or requests.Session()

    @retry(
        retry=retry_if_exception_type(
            (RetryableHTTPError, requests.ConnectionError, requests.Timeout)
        ),
        stop=stop_after_attempt(max(1, settings.azure_max_retries)),
        wait=_honor_retry_after,
        reraise=True,
    )
    def _get(url: str) -> dict:
        resp = session.get(url, timeout=settings.azure_request_timeout_s)
        if _is_retryable_status(resp.status_code):
            retry_after = resp.headers.get("Retry-After")
            ra_seconds: float | None = None
            if retry_after:
                try:
                    ra_seconds = float(retry_after)
                except ValueError:
                    ra_seconds = None
            raise RetryableHTTPError(resp.status_code, ra_seconds)
        resp.raise_for_status()
        return resp.json()

    try:
        url: str | None = _build_initial_url(settings)
        page_idx = 0
        while url:
            data = _get(url)
            items = data.get("Items", []) or []
            yield page_idx, items
            url = data.get("NextPageLink") or None
            page_idx += 1
    finally:
        if own_session:
            session.close()
