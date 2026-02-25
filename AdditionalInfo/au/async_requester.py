import asyncio
import json
import logging
import random
import time
from datetime import datetime
from typing import Any, Optional, TypedDict, Union
from urllib.parse import urlencode

import aiohttp
import async_timeout
from django.utils import timezone
from proxy_service.proxy_service import ProxyService

from .logging import setup_logger

PROXY_LIMIT_COUNTRY_CODES = ["AU"]
PROXY_MAX_ATTEMPTS = 10
PROXY_MAX_REATTEMPT_WAIT_TIME = 5

class HttpResponse(TypedDict):
    url: str
    requestParams: Optional[dict[str, Any]]
    requestHeaders: Optional[dict[str, str]]
    requestedAt: datetime
    responseTime: Optional[float]
    responseHeaders: dict[str, str]
    statusCode: Optional[int]
    exception: Optional[Exception]
    body: Optional[Union[str, list, dict]]


class AsyncRequester:
    """
    Asynchronous HTTP requester with retry and backoff logic.
    """
    _DEFAULT_MAX_RETRIES = 3
    _DEFAULT_MAX_WAIT = 60
    _DEFAULT_REQUEST_TIMEOUT = 30
    _DEFAULT_SENSITIVE_HEADERS = {"set-cookie", "authorization"}

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        max_retries: Optional[int] = None,
        max_wait: Optional[int] = None,
        request_timeout: Optional[int] = None,
        sensitive_headers: Optional[set[str]] = None,
    ):
        self.logger = logger or setup_logger()
        self.max_retries = max_retries or self._DEFAULT_MAX_RETRIES
        self.max_wait = max_wait or self._DEFAULT_MAX_WAIT
        self.request_timeout = request_timeout or self._DEFAULT_REQUEST_TIMEOUT
        self.sensitive_headers = sensitive_headers or self._DEFAULT_SENSITIVE_HEADERS
        self.successful_proxies: list[str] = []
        self.proxies: list[str] = [p.get("http") for p in ProxyService().get_proxy_list(PROXY_LIMIT_COUNTRY_CODES)]
        if not self.proxies:
            self.proxies: list[str] = [p.get("http") for p in ProxyService().get_proxy_list()]
        self.proxies = [p for p in self.proxies if p is not None]

    async def get_request(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        prepend_to_log: Optional[str] = "",
    ) -> HttpResponse:
        """
        Perform a HTTP GET request with retries and backoff.

        Args:
            session: aiohttp.ClientSession to use.
            url: The URL to request.
            params: Optional query parameters.
            headers: Optional request headers.
            prepend_to_log: Optional string to prepend to log messages for context.

        Returns:
            HttpResponse: TypedDict with response details.
        """
        request_url = f"{url}?{urlencode(params)}" if params else url
        requested_at = timezone.now()
        response_time = None
        response_headers = {}
        status_code = None
        exception = None
        body = None

        # Temporary workaround for TMBG APIs
        max_retries = 10 if url.startswith("https://ob.tmbl.com.au/") else self.max_retries

        use_proxy = False
        proxies_to_try: set[str] = set()

        attempt = 0
        while attempt < max_retries and (len(proxies_to_try) > 0 or not use_proxy):
            attempt += 1
            proxy = None
            if use_proxy:
                if proxy not in proxies_to_try:
                    try:
                        proxy = next(p for p in self.successful_proxies if p in proxies_to_try)
                    except:
                        proxy = random.choice([*proxies_to_try])
            try:
                log_info = f"{prepend_to_log}{request_url} | Attempt {attempt}"
                if proxy:
                    log_info += f" (with proxy: {proxy})"

                # Reset variables on retry
                if attempt > 1:
                    response_time = None
                    response_headers = {}
                    status_code = None
                    exception = None
                    body = None

                wait = None
                requested_at = timezone.now()
                start_time = time.perf_counter()

                # Fetch and load the response
                async with async_timeout.timeout(self.request_timeout):
                    response = await session.get(url, params=params, headers=headers, proxy=proxy)
                    content_type = response.headers.get("Content-Type", "").lower()
                    body = None

                    if response.status == 403:
                        # Potentially blocked, retry with Proxy Service if there are proxies available
                        if not use_proxy and len(self.proxies) > 0:
                            use_proxy = True
                            self.logger.warning(f"{log_info}: Potentially blocked by provider, trying again with proxy service")
                            proxies_to_try = set(self.proxies)
                            max_retries = min(max(5, max_retries), 1 + len(proxies_to_try))
                        elif proxy is not None:
                            proxies_to_try.remove(proxy)
                    elif response.status == 200 and proxy is not None and proxy not in self.successful_proxies:
                        self.successful_proxies.append(proxy)

                    try:
                        if "application/json" in content_type:
                            body = await response.json()
                        else:
                            self.logger.warning(f"{log_info}: Unexpected Content-Type '{content_type}'")
                            body = await response.text()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError) as e:
                        self.logger.warning(f"{log_info}: [{type(e).__name__}] Failed to decode body based on Content-Type '{content_type}'")
                        exception = e
                        if "application/json" in content_type:
                            try:
                                body = await response.text()
                            except Exception:
                                pass

                response_time = time.perf_counter() - start_time
                response_headers = dict(response.headers)
                status_code = response.status

                # Handle successful response
                if status_code == 200:
                    self.logger.debug(f"{log_info}: Request succeeded with status {status_code} in {response_time:.2f}s")
                    break

                # Handle non-200 responses
                self.logger.warning(f"{log_info}: Request failed with status {status_code}")
                if status_code in {429, 503}:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait = float(retry_after)
                        except ValueError:
                            self.logger.warning(f"{log_info}: Invalid Retry-After value '{retry_after}'")

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                self.logger.warning(f"{log_info}: [{type(e).__name__}] Failed to make request")
                exception = e

            # Retry logic
            if attempt < max_retries:
                if wait is None:
                    wait = 2 ** attempt + random.uniform(0, 1) # Exponential backoff with jitter
                wait = min(wait, self.max_wait)
                self.logger.debug(f"{log_info}: Retrying in {wait:.2f}s")
                await asyncio.sleep(wait)
                continue

            self.logger.error(f"{log_info}: Max retries reached ({max_retries})")

        return {
            "url": url,
            "requestParams": params or {},
            "requestHeaders": headers or {},
            "requestedAt": requested_at,
            "responseTime": response_time,
            "responseHeaders": self._sanitise_headers(response_headers),
            "statusCode": status_code,
            "exception": exception,
            "body": body,
        }

    def _sanitise_headers(self, headers: dict[str, str]) -> dict[str, str]:
        return {k: v for k, v in headers.items() if k.lower() not in self.sensitive_headers}
