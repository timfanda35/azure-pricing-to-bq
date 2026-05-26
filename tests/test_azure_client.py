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
