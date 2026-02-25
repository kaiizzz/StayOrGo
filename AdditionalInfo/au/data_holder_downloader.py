import aiohttp
import logging
import time
from typing import Union

from cdr_monitor.util import HISTORIC_DATA_FOLDER
from utils.fs import write_json_file

from .async_requester import AsyncRequester
from .config import COMMON_HEADERS, INDUSTRY_CONFIG
from .registry import Registry, SummaryData
from .slack_update_mixin import SlackUpdateMixin
from .utils import JsonHttpResponse, serialise_http_response


class DataHolderDownloader(SlackUpdateMixin):
    _API_NAME = "Get Data Holder Brands Summary"
    _PARAMS = None
    _HEADERS = {
        **COMMON_HEADERS,
        "x-v": "1",
    }

    def __init__(self, today_str: str, slack_updates: bool, is_backup: bool, industry: str, logger: logging.Logger, registry: Registry):
        self.today_str = today_str
        self.slack_updates = slack_updates
        self.is_backup = is_backup
        self.industry = industry
        self.logger = logger
        self.registry = registry
        self.brands_summary_endpoint = INDUSTRY_CONFIG[industry]["brands_summary_endpoint"]
        self.summary_path = INDUSTRY_CONFIG[industry]["summary_path"]
        self.requester = AsyncRequester(logger)

    async def run(self) -> None:
        self.logger.info(f"{__class__.__name__} running...")

        try:
            start_time = time.time()
            brand_summary_filename = f"dh_brand_summary_{self.industry}_{self.today_str}.json"
            brand_summary_file = HISTORIC_DATA_FOLDER() / self.today_str / brand_summary_filename

            brand_summary = await self._retrieve_data_holder_brand_summary()

            if brand_summary:
                self._update_registry(brand_summary)

            self.logger.info(f"Writing to {brand_summary_filename}")
            write_json_file(brand_summary_file, brand_summary)

            self._send_slack_update(True)
            self.logger.info(f"...{__class__.__name__} finished ({time.time() - start_time:0.2f} seconds)")

        except Exception as e:
            self._send_slack_update(False, e)
            self.logger.exception(f"{__class__.__name__} failed: {e}")

    async def _retrieve_data_holder_brand_summary(self) -> Union[JsonHttpResponse, dict]:
        self.logger.info("Retrieving data holder brand summary")
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                url = self.brands_summary_endpoint
                prepend_to_log = f"{self._API_NAME} v{self._HEADERS['x-v']} | "
                response = await self.requester.get_request(session, url, params=self._PARAMS, headers=self._HEADERS, prepend_to_log=prepend_to_log)

                if response["statusCode"] != 200:
                    self._send_slack_update(False, Exception("Non-200 response from data holder brands summary"))

                return serialise_http_response(response)

        except Exception as e:
            self._send_slack_update(False, e)
            self.logger.exception(f"Failed to retrieve data holder brands summary: {e}")
            return {}

    def _update_registry(self, brand_summary: JsonHttpResponse) -> None:
        try:
            if brand_summary["statusCode"] != 200:
                return

            self.logger.info("Updating registry")

            requested_at = brand_summary["requestedAt"]
            brand_summary_data = brand_summary["body"]["data"]

            for i, brand in enumerate(brand_summary_data):
                if not isinstance(brand, dict):
                    self.logger.error(f"data[{i}] in brand summary is not a dict")
                    continue

                brand_id = brand.get("dataHolderBrandId") or brand.get("interimId")
                brand_name = brand.get("brandName")
                base_uri = brand.get("publicBaseUri")

                if not (brand_id and brand_name and base_uri):
                    self.logger.error(f"data[{i}] in brand summary missing brandId, brandName or publicBaseUri")
                    continue

                brand_id = str(brand_id) if brand_id else None
                brand_name = str(brand_name) if brand_name else None
                base_uri = str(base_uri) if base_uri else None

                summary_data = self.registry.get_summary_data(brand_id)

                if summary_data:
                    self.logger.debug(f"Updating brandId '{brand_id}'")
                    summary_data.brandName = brand_name
                    summary_data.baseUri = base_uri
                    summary_data.lastSeen = requested_at
                else:
                    self.logger.info(f"New brandId '{brand_id}'")
                    summary_data = SummaryData(
                        brandName=brand_name,
                        baseUri=base_uri,
                        firstSeen=requested_at,
                        lastSeen=requested_at,
                    )
                    self.registry.create_summary_api(brand_id, summary_data)

            # Set firstSeen and lastSeen for any summary APIs manually added
            for brand_id, summary_data in self.registry.get_summary_apis().items():
                if summary_data.firstSeen and summary_data.lastSeen:
                    continue
                self.logger.info(f"Setting firstSeen and lastSeen for brandId '{brand_id}'")
                summary_data.firstSeen = requested_at
                summary_data.lastSeen = requested_at

        except Exception as e:
            self._send_slack_update(False, e)
            self.logger.exception(f"Failed to update registry: {e}")


agent = DataHolderDownloader
agent.run()