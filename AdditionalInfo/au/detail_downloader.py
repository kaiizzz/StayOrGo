import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Any

import aiohttp

from . import db
from .async_requester import HttpResponse, AsyncRequester
from .config import COMMON_HEADERS, INDUSTRY_CONFIG
from .master_saver_mixin import MasterSaverMixin
from .registry import Registry
from .slack_update_mixin import SlackUpdateMixin
from .utils import JsonHttpResponse, serialise_http_response, is_empty_detail_response


class DetailDownloader(MasterSaverMixin, SlackUpdateMixin):
    _MAX_CONCURRENCY = 100 # TODO: Set limit per data holder (i.e. per base URI)
    _PARAMS = None
    _HEADERS = {
        **COMMON_HEADERS
    }

    def __init__(self, today_str: str, slack_updates: bool, is_backup: bool, industry: str, logger: logging.Logger, registry: Registry):
        self.today_str = today_str
        self.slack_updates = slack_updates
        self.is_backup = is_backup
        self.industry = industry
        self.logger = logger
        self.registry = registry

        industry_config = INDUSTRY_CONFIG[industry]

        self.summary_path = industry_config["summary_path"]
        self.api_name = industry_config["detail_api_name"]
        self.api_versions = industry_config["detail_api_versions"]
        self.detail_id_key = industry_config["detail_id_key"]
        self.detail_category_key = industry_config["detail_category_key"]
        self.update_api_response_table = industry_config["update_api_response_table"]

    async def run(self) -> None:
        self.logger.info(f"{__class__.__name__} running...")

        try:
            start_time = time.time()
            self.semaphore = asyncio.Semaphore(self._MAX_CONCURRENCY)
            self.requester = AsyncRequester(self.logger)

            endpoints = self._endpoints_from_registry()
            master = await self._fetch_detail_data(endpoints)

            self._save_master(master)
            self._send_slack_update(True)
            self.logger.info(f"...{__class__.__name__} finished ({time.time() - start_time:0.2f} seconds)")

        except Exception as e:
            self._send_slack_update(False, e)
            self.logger.exception(f"{__class__.__name__} failed: {e}")

    def _endpoints_from_registry(self) -> dict:
        self.logger.info("Retrieving endpoints from registry")
        endpoints = {}

        summary_apis = self.registry.get_summary_apis()
        detail_apis = self.registry.get_detail_apis()

        for brand_id, details in detail_apis.items():
            if brand_id not in summary_apis:
                self.logger.warning(f"Brand ID '{brand_id}' in detail APIs not found in summary APIs")
                continue

            for detail_id, detail_data in details.items():
                if detail_data.skip:
                    self.logger.info(f"Skipping {self.detail_id_key} '{detail_id}' under brandId '{brand_id}'")
                    continue

                brand_name = summary_apis[brand_id].brandNameOverride or summary_apis[brand_id].brandName
                base_uri = summary_apis[brand_id].baseUriOverride or summary_apis[brand_id].baseUri
                url = f"{base_uri.rstrip('/')}{self.summary_path}/{detail_id}"

                endpoints[url] = {
                    "brand_id": brand_id,
                    "brand_name": brand_name,
                    "detail_id": detail_id,
                    "sub_brand": detail_data.subBrand,
                    "category": getattr(detail_data, self.detail_category_key),
                }

        return endpoints

    async def _fetch_detail_data(self, endpoints: dict) -> dict:
        self.logger.info("Fetching detail data")
        api_name = self.api_name
        api_versions = self.api_versions
        api_responses = []
        master = {}

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            for api_version in api_versions:
                headers = {**self._HEADERS, "x-v": api_version}
                tasks = []

                for url in endpoints:
                    brand_name = endpoints[url]["brand_name"]
                    prepend_to_log = f"{api_name} v{api_version} | {brand_name} | "
                    tasks.append(
                        self._bounded_get_request(session, url, params=self._PARAMS, headers=headers, prepend_to_log=prepend_to_log)
                    )

                responses = await asyncio.gather(*tasks)

                for response in responses:
                    url = response["url"]
                    status_code = response["statusCode"]

                    brand_id = endpoints[url]["brand_id"]
                    brand_name = endpoints[url]["brand_name"]
                    detail_id = endpoints[url]["detail_id"]
                    sub_brand = endpoints[url]["sub_brand"]
                    category = endpoints[url]["category"]

                    entry = serialise_http_response(response)
                    master.setdefault(api_name, {}).setdefault(f"v{api_version}", {}).setdefault(brand_name, {})[detail_id] = entry

                    self._update_detail_registry(brand_id, detail_id, entry)

                    # TODO: decouple and update API responses from master JSON after downloader runs
                    if self.update_api_response_table:
                        api_response = await db.create_api_response_object(
                            url=entry["url"],
                            brand_id=brand_id,
                            brand_name=brand_name,
                            status_code=status_code,
                            is_empty=is_empty_detail_response(response["body"]) if status_code == 200 else False,
                            api_name=api_name,
                            api_version=f"v{api_version}",
                            requested_at=response["requestedAt"],
                            sub_brand=sub_brand,
                            product_category=category,
                        )
                        if api_response:
                            api_responses.append(api_response)

        if self.update_api_response_table:
            await db.bulk_create_api_responses(api_responses)

        return master

    async def _bounded_get_request(self, session: aiohttp.ClientSession, url: str, params: dict[str, Any], headers: dict[str, str], prepend_to_log: str) -> HttpResponse:
        async with self.semaphore:
            return await self.requester.get_request(session, url, params=params, headers=headers, prepend_to_log=prepend_to_log)

    def _update_detail_registry(self, brand_id: str, detail_id: str, entry: JsonHttpResponse) -> None:
        status_code = entry["statusCode"]
        requested_at = entry["requestedAt"]

        detail_data = self.registry.get_detail_data(brand_id, detail_id)

        if detail_data is None:
            return

        if status_code == 200:
            detail_data.last200Response = requested_at
            return

        # Automatically remove detail API if it has failed for 90 days or more
        requested_at = datetime.fromisoformat(requested_at)
        last_seen = datetime.fromisoformat(detail_data.lastSeen)
        last_200_response = datetime.fromisoformat(detail_data.last200Response) if detail_data.last200Response else datetime.fromisoformat(detail_data.firstSeen)

        if (last_200_response < requested_at - timedelta(days=90)) and (last_seen < requested_at - timedelta(days=90)):
            self.logger.warning(f"Removing {self.detail_id_key} '{detail_id}' under brandId '{brand_id}' from detail APIs")
            self.registry.delete_detail_api(brand_id, detail_id)
