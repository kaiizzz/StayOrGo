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
from .registry import BankingDetailData, EnergyDetailData, Registry
from .slack_update_mixin import SlackUpdateMixin
from .utils import JsonHttpResponse, serialise_http_response, is_empty_summary_response


class SummaryDownloader(MasterSaverMixin, SlackUpdateMixin):
    _MAX_CONCURRENCY = 100 # TODO: Set limit per data holder (i.e. per base URI)
    _PARAMS = {
        "effective": "ALL",
        "page": 1,
        "page-size": 500, # TODO: check if this causes issues with some APIs
    }
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
        self.api_name = industry_config["summary_api_name"]
        self.api_versions = industry_config["summary_api_versions"]
        self.summary_key = industry_config["summary_key"]
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
            master = await self._fetch_summary_data(endpoints)

            self._save_master(master)
            self._send_slack_update(True)
            self.logger.info(f"...{__class__.__name__} finished ({time.time() - start_time:0.2f} seconds)")

        except Exception as e:
            self._send_slack_update(False, e)
            self.logger.exception(f"{__class__.__name__} failed: {e}")

    def _endpoints_from_registry(self) -> dict:
        self.logger.info("Retrieving endpoints from registry")
        endpoints = {}

        for brand_id, summary_data in self.registry.get_summary_apis().items():
            if summary_data.skip:
                self.logger.info(f"Skipping brandId '{brand_id}'")
                continue

            brand_name = summary_data.brandNameOverride or summary_data.brandName
            base_uri = summary_data.baseUriOverride or summary_data.baseUri
            url = f"{base_uri.rstrip('/')}{self.summary_path}"

            endpoints[url] = {
                "brand_id": brand_id,
                "brand_name": brand_name,
            }

        return endpoints

    async def _fetch_summary_data(self, endpoints: dict) -> dict:
        self.logger.info("Fetching summary data")
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

                    entry = serialise_http_response(response)
                    master.setdefault(api_name, {}).setdefault(f"v{api_version}", {}).setdefault(brand_name, []).append(entry)

                    self._update_summary_registry(brand_id, entry)
                    self._update_detail_registry(brand_id, entry)

                    # TODO: decouple and update API responses from master JSON after downloader runs
                    if self.update_api_response_table:
                        api_response = await db.create_api_response_object(
                            url=entry["url"],
                            brand_id=brand_id,
                            brand_name=brand_name,
                            status_code=status_code,
                            is_empty=is_empty_summary_response(response["body"], self.summary_key) if status_code == 200 else False,
                            api_name=api_name,
                            api_version=f"v{api_version}",
                            requested_at=response["requestedAt"],
                        )
                        if api_response:
                            api_responses.append(api_response)

                    if status_code != 200:
                        continue

                    prepend_to_log = f"{api_name} v{api_version} | {brand_name} | "

                    try:
                        total_pages = int(response["body"]["meta"]["totalPages"])
                    except Exception as e:
                        self.logger.error(f"{prepend_to_log}{url} | Failed to extract totalPages: {e}")
                        total_pages = 1

                    if total_pages > 1:
                        page_tasks = [
                            self._bounded_get_request(session, url, params={**self._PARAMS, "page": p}, headers=headers, prepend_to_log=prepend_to_log)
                            for p in range(2, total_pages + 1)
                        ]
                        page_responses = await asyncio.gather(*page_tasks)

                        for page_response in page_responses:
                            page_status_code = page_response["statusCode"]

                            page_entry = serialise_http_response(page_response)
                            master[api_name][f"v{api_version}"][brand_name].append(page_entry)

                            self._update_detail_registry(brand_id, page_entry)

                            # TODO: decouple and update API responses from master JSON after downloader runs
                            page_api_response = await db.create_api_response_object(
                                url=page_entry["url"],
                                brand_id=brand_id,
                                brand_name=brand_name,
                                status_code=page_status_code,
                                is_empty=is_empty_summary_response(page_response["body"], self.summary_key) if page_status_code == 200 else False,
                                api_name=api_name,
                                api_version=f"v{api_version}",
                                requested_at=page_response["requestedAt"],
                            )
                            if page_api_response:
                                api_responses.append(page_api_response)

        if self.update_api_response_table:
            await db.bulk_create_api_responses(api_responses)

        return master

    async def _bounded_get_request(self, session: aiohttp.ClientSession, url: str, params: dict[str, Any], headers: dict[str, str], prepend_to_log: str) -> HttpResponse:
        async with self.semaphore:
            return await self.requester.get_request(session, url, params=params, headers=headers, prepend_to_log=prepend_to_log)

    def _update_summary_registry(self, brand_id: str, entry: JsonHttpResponse) -> None:
        status_code = entry["statusCode"]
        requested_at = entry["requestedAt"]

        summary_data = self.registry.get_summary_data(brand_id)

        if status_code == 200:
            summary_data.last200Response = requested_at
            return

        # Automatically remove summary API (and associated detail APIs) if it has failed for 90 days or more
        requested_at = datetime.fromisoformat(requested_at)
        last_seen = datetime.fromisoformat(summary_data.lastSeen)
        last_200_response = datetime.fromisoformat(summary_data.last200Response) if summary_data.last200Response else datetime.fromisoformat(summary_data.firstSeen)

        if (last_200_response < requested_at - timedelta(days=90)) and (last_seen < requested_at - timedelta(days=90)):
            self.logger.warning(f"Removing brandId '{brand_id}' from summary and detail APIs")
            self.registry.delete_summary_api(brand_id)
            self.registry.delete_detail_api(brand_id, None)

    def _update_detail_registry(self, brand_id: str, entry: JsonHttpResponse) -> None:
        if entry["statusCode"] != 200:
            return

        try:
            requested_at = entry["requestedAt"]
            summary_list = entry["body"]["data"][self.summary_key]

            for summary in summary_list:
                if not isinstance(summary, dict):
                    continue

                detail_id = summary.get(self.detail_id_key)

                if detail_id is None:
                    continue

                sub_brand = summary.get("brand")
                detail_category = summary.get(self.detail_category_key)

                detail_id = str(detail_id)
                sub_brand = str(sub_brand) if sub_brand is not None else None
                detail_category = str(detail_category) if detail_category is not None else None

                detail_data = self.registry.get_detail_data(brand_id, detail_id)

                if detail_data:
                    self.logger.debug(f"Updating {self.detail_id_key} '{detail_id}' under brandId '{brand_id}'")
                    detail_data.subBrand = sub_brand
                    setattr(detail_data, self.detail_category_key, detail_category)
                    detail_data.lastSeen = requested_at
                else:
                    self.logger.info(f"New {self.detail_id_key} '{detail_id}' under brandId '{brand_id}'")
                    detail_data_class = BankingDetailData if self.industry == "banking" else EnergyDetailData
                    detail_data = detail_data_class(
                        subBrand=sub_brand,
                        **{self.detail_category_key: detail_category},
                        firstSeen=requested_at,
                        lastSeen=requested_at,
                    )
                    self.registry.create_detail_api(brand_id, detail_id, detail_data)

        except Exception as e:
            self.logger.error(f"Failed to update detail registry: {e}")
