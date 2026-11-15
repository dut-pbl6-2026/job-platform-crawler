"""Unit tests for BlockDetectionMiddleware (CRAWL-01-07): what counts as a block."""

from unittest.mock import MagicMock, patch

import pytest
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse, Request, Response

from crawler.middlewares import BlockDetectionMiddleware, block_reason

URL = "https://vieclam.gov.vn/search/?page=1"


def html_response(status=200, title="Viec lam", body_extra=""):
    body = f"<html><head><title>{title}</title></head><body>{body_extra}</body></html>"
    return HtmlResponse(
        URL,
        status=status,
        body=body.encode("utf-8"),
        headers={"Content-Type": "text/html; charset=utf-8"},
    )


@pytest.fixture
def mw():
    middleware = BlockDetectionMiddleware(threshold=2, network_retry_times=2)
    with patch.object(BlockDetectionMiddleware, "_run_seed_fallback") as fallback:
        middleware._fallback = fallback
        yield middleware


def test_403_and_429_are_block_signals():
    assert block_reason(Response(URL, status=403)) == "HTTP 403"
    assert block_reason(Response(URL, status=429)) == "HTTP 429"


def test_server_errors_are_not_block_signals():
    for status in (500, 502, 503, 504):
        assert block_reason(Response(URL, status=status)) is None


def test_known_block_page_title_is_a_block_signal():
    response = html_response(title="Attention Required! | Cloudflare")
    assert block_reason(response) == "block page"


def test_normal_page_mentioning_captcha_is_not_a_block_signal():
    # Only <title> is inspected, so a reCAPTCHA script in a normal page is fine.
    response = html_response(body_extra="<script>captcha widget</script>")
    assert block_reason(response) is None


def test_block_title_on_non_html_is_ignored():
    response = Response(
        URL,
        status=200,
        body=b"<title>Access denied</title>",
        headers={"Content-Type": "application/json"},
    )
    assert block_reason(response) is None


def test_network_error_is_retried_not_counted(mw):
    request = Request(URL)
    for _ in range(10):  # far above threshold=2
        with patch.object(
            BlockDetectionMiddleware, "_after", return_value="deferred"
        ) as after:
            result = mw.process_exception(
                request, TimeoutError("timed out"), spider=None
            )
        assert result == "deferred"
        assert after.call_count == 1
    assert mw.consecutive_failures == 0
    mw._fallback.assert_not_called()


def test_network_retry_backoff_doubles_and_is_bounded(mw):
    mw.network_retry_backoff = 2.0
    request = Request(URL)
    delays = []

    def capture(delay, retried):
        delays.append((delay, retried.meta["network_retry_count"]))
        return "deferred"

    with patch.object(BlockDetectionMiddleware, "_after", side_effect=capture):
        retried = request
        for _ in range(2):
            mw.process_exception(retried, ConnectionRefusedError(), spider=None)
            retried = request.replace(meta={"network_retry_count": len(delays)})
        # attempt 0 -> wait 2s and mark 1; attempt 1 -> wait 4s and mark 2
        assert delays == [(2.0, 1), (4.0, 2)]
        # retries exhausted: give up and let the errback handle it
        assert (
            mw.process_exception(retried, ConnectionRefusedError(), spider=None) is None
        )


def test_retry_uses_dont_filter_and_keeps_callback(mw):
    def cb(response):
        return None

    request = Request(URL, callback=cb)
    with patch.object(
        BlockDetectionMiddleware, "_after", side_effect=lambda d, r: r
    ) as after:
        retried = mw.process_exception(request, TimeoutError(), spider=None)
    assert after.called
    assert retried.dont_filter is True
    assert retried.callback is cb
    assert request.meta.get("network_retry_count") is None  # original untouched


def test_consecutive_403_over_threshold_triggers_fallback_and_closes_engine(mw):
    spider = MagicMock()
    for _ in range(2):  # threshold=2: two signals are tolerated
        mw.process_response(Request(URL), Response(URL, status=403), spider)
    assert mw._fallback.call_count == 0
    with pytest.raises(IgnoreRequest):
        mw.process_response(Request(URL), Response(URL, status=403), spider)
    mw._fallback.assert_called_once_with(spider)
    # Must close the engine: raising CloseSpider from a downloader middleware does not.
    spider.crawler.engine.close_spider.assert_called_once_with(
        spider, reason="blocked_fallback"
    )


def test_success_resets_consecutive_count(mw):
    spider = MagicMock()
    mw.process_response(Request(URL), Response(URL, status=429), spider)
    mw.process_response(Request(URL), html_response(status=200), spider)
    assert mw.consecutive_failures == 0
    for _ in range(2):
        mw.process_response(Request(URL), Response(URL, status=403), spider)
    mw._fallback.assert_not_called()


def test_network_errors_between_blocks_do_not_reset_or_count(mw):
    spider = MagicMock()
    mw.process_response(Request(URL), Response(URL, status=403), spider)
    with patch.object(BlockDetectionMiddleware, "_after", return_value="deferred"):
        mw.process_exception(Request(URL), TimeoutError(), spider)
    assert mw.consecutive_failures == 1
