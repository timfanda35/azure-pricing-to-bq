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

# Documented `serviceFamily` values from
# https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices
# (section "Supported serviceFamily values"). The Azure Retail Prices API caps
# `$skip` at 1,000,000 — once `$skip >= 1_000_000` the API returns HTTP 400.
# Page size is 1000, so a single fetch chain can return at most ~1M items
# before hitting the cap. The whole Azure corpus is in the ~600k-1M range and
# growing, so we slice by `serviceFamily` to give each chain plenty of headroom
# (every family stays well under 1M, restarts at `$skip=0`).
SERVICE_FAMILIES: tuple[str, ...] = (
    "Analytics",
    "Azure Arc",
    "Azure Communication Services",
    "Azure Security",
    "Azure Stack",
    "Compute",
    "Containers",
    "Data",
    "Databases",
    "Developer Tools",
    "Dynamics",
    "Gaming",
    "Integration",
    "Internet of Things",
    "Management and Governance",
    "Microsoft Syntex",
    "Mixed Reality",
    "Networking",
    "Other",
    "Power Platform",
    "Quantum Computing",
    "Security",
    "Storage",
    "Telecommunications",
    "Web",
    "Windows Virtual Desktop",
)


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
    if own_session:
        session = requests.Session()
        proxies = {
            k: v
            for k, v in {"http": settings.http_proxy, "https": settings.https_proxy}.items()
            if v
        }
        if proxies:
            session.proxies.update(proxies)
        if settings.no_proxy:
            session.proxies["no"] = settings.no_proxy
    else:
        session = session

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


def _resolve_service_families(settings: Settings) -> tuple[str, ...]:
    """Return the list of serviceFamily values to iterate over.

    Honors the optional `AZURE_SERVICE_FAMILIES` env var (CSV) when set,
    otherwise falls back to the built-in documented list.
    """
    raw = (settings.azure_service_families or "").strip()
    if not raw:
        return SERVICE_FAMILIES
    return tuple(p.strip() for p in raw.split(",") if p.strip())


def _build_session(settings: Settings) -> requests.Session:
    session = requests.Session()
    proxies = {
        k: v for k, v in {"http": settings.http_proxy, "https": settings.https_proxy}.items() if v
    }
    if proxies:
        session.proxies.update(proxies)
    if settings.no_proxy:
        session.proxies["no"] = settings.no_proxy
    return session


def fetch_all_pages(
    settings: Settings | None = None,
    session: requests.Session | None = None,
) -> Iterator[tuple[int, list[dict]]]:
    """Yield (page_index, items) across the full corpus, working around Azure's $skip limit.

    Behavior:
      * If `settings.azure_optional_filter` is set, that filter is honored as-is
        and pages are fetched in a single stream — the caller has already
        constrained the result set.
      * Otherwise, the corpus is partitioned by `serviceFamily`. Each family
        gets its own fetch chain starting at $skip=0, so the per-chain depth
        stays well below the API's $skip = 1,000,000 hard cap (HTTP 400 above).

    Page indices are global across partitions so the loader's page counter
    increments monotonically.
    """
    settings = settings or get_settings()

    if settings.azure_optional_filter:
        yield from fetch_pages(settings, session)
        return

    own_session = session is None
    if own_session:
        session = _build_session(settings)

    families = _resolve_service_families(settings)
    page_idx = 0
    try:
        for i, family in enumerate(families, start=1):
            sub_settings = settings.model_copy(
                update={"azure_optional_filter": f"serviceFamily eq '{family}'"}
            )
            logger.info(
                "azure.fetch_all_pages.family family=%r (%d/%d)",
                family,
                i,
                len(families),
            )
            family_items = 0
            for _, items in fetch_pages(sub_settings, session):
                family_items += len(items)
                yield page_idx, items
                page_idx += 1
            logger.info(
                "azure.fetch_all_pages.family.done family=%r items=%d",
                family,
                family_items,
            )
    finally:
        if own_session:
            session.close()
