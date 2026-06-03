from unittest.mock import patch

import pytest
import responses

from app.services import azure_client


def _page(items: list[dict], next_link: str | None = None) -> dict:
    return {
        "BillingCurrency": "USD",
        "Items": items,
        "NextPageLink": next_link,
        "Count": len(items),
    }


@responses.activate
def test_fetch_pages_follows_next_page_link(settings):
    url1 = "https://prices.azure.com/api/retail/prices?api-version=2023-01-01-preview&currencyCode='USD'"
    url2 = "https://prices.azure.com/api/retail/prices?$skip=1000"
    responses.add(responses.GET, url1, json=_page([{"meterId": "a"}], next_link=url2))
    responses.add(
        responses.GET, url2, json=_page([{"meterId": "b"}, {"meterId": "c"}], next_link=None)
    )

    pages = list(azure_client.fetch_pages(settings))

    assert len(pages) == 2
    assert pages[0] == (0, [{"meterId": "a"}])
    assert pages[1] == (1, [{"meterId": "b"}, {"meterId": "c"}])


@responses.activate
def test_fetch_pages_retries_on_429_then_succeeds(settings):
    url = "https://prices.azure.com/api/retail/prices?api-version=2023-01-01-preview&currencyCode='USD'"
    responses.add(responses.GET, url, status=429, headers={"Retry-After": "0"})
    responses.add(responses.GET, url, json=_page([{"meterId": "x"}], next_link=None))

    with patch("time.sleep"):
        pages = list(azure_client.fetch_pages(settings))

    assert pages == [(0, [{"meterId": "x"}])]
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_pages_retries_on_5xx(settings):
    url = "https://prices.azure.com/api/retail/prices?api-version=2023-01-01-preview&currencyCode='USD'"
    responses.add(responses.GET, url, status=503)
    responses.add(responses.GET, url, json=_page([], next_link=None))

    with patch("time.sleep"):
        pages = list(azure_client.fetch_pages(settings))

    assert pages == [(0, [])]
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_pages_gives_up_after_max_retries(settings):
    url = "https://prices.azure.com/api/retail/prices?api-version=2023-01-01-preview&currencyCode='USD'"
    for _ in range(settings.azure_max_retries):
        responses.add(responses.GET, url, status=500)

    with patch("time.sleep"):
        with pytest.raises(azure_client.RetryableHTTPError):
            list(azure_client.fetch_pages(settings))


def test_proxy_settings_applied_to_session(settings):
    settings.http_proxy = "http://proxy.example.com:8080"
    settings.https_proxy = "https://proxy.example.com:8080"

    import unittest.mock as mock

    with mock.patch("requests.Session") as MockSession:
        mock_session = MockSession.return_value
        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = {"Items": [], "NextPageLink": None}
        list(azure_client.fetch_pages(settings))

    mock_session.proxies.update.assert_called_once_with(
        {"http": "http://proxy.example.com:8080", "https": "https://proxy.example.com:8080"}
    )


def test_no_proxy_applied_to_session(settings):
    settings.https_proxy = "https://proxy.example.com:8080"
    settings.no_proxy = "169.254.169.254,metadata.google.internal"

    import unittest.mock as mock

    with mock.patch("requests.Session") as MockSession:
        mock_session = MockSession.return_value
        mock_session.proxies = {}
        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = {"Items": [], "NextPageLink": None}
        list(azure_client.fetch_pages(settings))

    assert mock_session.proxies["no"] == "169.254.169.254,metadata.google.internal"


@responses.activate
def test_optional_filter_is_included(settings):
    settings.azure_optional_filter = "serviceName eq 'Virtual Machines'"
    responses.add(
        responses.GET,
        "https://prices.azure.com/api/retail/prices",
        json=_page([], next_link=None),
    )
    list(azure_client.fetch_pages(settings))
    # The filter must end up in the query string of the first request.
    assert "$filter=serviceName" in responses.calls[0].request.url


def test_fetch_all_pages_partitions_by_service_family_when_no_user_filter(settings):
    """With no AZURE_OPTIONAL_FILTER, the loader should request one fetch chain
    per serviceFamily so no single chain exceeds Azure's $skip limit."""
    settings.azure_optional_filter = ""
    # Use a small override list so the test stays fast.
    settings.azure_service_families = "Compute,Networking,Storage"

    seen_filters: list[str] = []

    def _fake_fetch_pages(sub_settings, session=None):
        seen_filters.append(sub_settings.azure_optional_filter)
        # Yield one page per family with one item each.
        yield 0, [{"meterId": f"m-{sub_settings.azure_optional_filter}"}]

    with patch.object(azure_client, "fetch_pages", side_effect=_fake_fetch_pages):
        pages = list(azure_client.fetch_all_pages(settings))

    assert seen_filters == [
        "serviceFamily eq 'Compute'",
        "serviceFamily eq 'Networking'",
        "serviceFamily eq 'Storage'",
    ]
    # Three families, one page each, global page indices increment monotonically.
    assert [p[0] for p in pages] == [0, 1, 2]
    assert {item["meterId"] for _, items in pages for item in items} == {
        "m-serviceFamily eq 'Compute'",
        "m-serviceFamily eq 'Networking'",
        "m-serviceFamily eq 'Storage'",
    }


def test_fetch_all_pages_honors_user_filter_without_partitioning(settings):
    """If the caller supplies AZURE_OPTIONAL_FILTER, fetch_all_pages should
    delegate to fetch_pages once (no partitioning)."""
    settings.azure_optional_filter = "serviceName eq 'Azure DNS'"

    seen_filters: list[str] = []

    def _fake_fetch_pages(sub_settings, session=None):
        seen_filters.append(sub_settings.azure_optional_filter)
        yield 0, [{"meterId": "x"}]

    with patch.object(azure_client, "fetch_pages", side_effect=_fake_fetch_pages):
        pages = list(azure_client.fetch_all_pages(settings))

    assert seen_filters == ["serviceName eq 'Azure DNS'"]
    assert pages == [(0, [{"meterId": "x"}])]


def test_fetch_all_pages_uses_builtin_family_list_when_csv_empty(settings):
    """When AZURE_SERVICE_FAMILIES is empty, fall back to the built-in list."""
    settings.azure_optional_filter = ""
    settings.azure_service_families = ""

    fetch_count = 0

    def _fake_fetch_pages(sub_settings, session=None):
        nonlocal fetch_count
        fetch_count += 1
        yield 0, []

    with patch.object(azure_client, "fetch_pages", side_effect=_fake_fetch_pages):
        list(azure_client.fetch_all_pages(settings))

    # The built-in list is the SERVICE_FAMILIES module constant.
    assert fetch_count == len(azure_client.SERVICE_FAMILIES)


def test_service_families_list_includes_the_big_ones():
    """The hardcoded list must include the families that produce most of the corpus."""
    families = set(azure_client.SERVICE_FAMILIES)
    for required in ("Compute", "Networking", "Storage", "Databases", "Other"):
        assert required in families, f"missing {required}"
