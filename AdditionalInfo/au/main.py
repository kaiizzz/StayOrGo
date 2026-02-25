import asyncio
import logging
import os
import traceback
from pathlib import Path

from cdr_monitor.util import HISTORIC_DATA_FOLDER
from slack.slack import Slack
from utils.datetime_helpers import get_current_datetime

from .config import INDUSTRIES, MAX_CONCURRENT_INDUSTRIES
from .data_holder_downloader import DataHolderDownloader
from .detail_downloader import DetailDownloader
from .logging import setup_logger
from .registry import Registry
from .summary_downloader import SummaryDownloader


async def run_downloaders(today_str: str, slack_updates: bool, is_backup: bool, industry: str, logger: logging.Logger, registry: Registry) -> None:
    await DataHolderDownloader(today_str, slack_updates, is_backup, industry, logger, registry).run()
    await SummaryDownloader(today_str, slack_updates, is_backup, industry, logger, registry).run()
    await DetailDownloader(today_str, slack_updates, is_backup, industry, logger, registry).run()


async def run_industry(today_str: str, today_dir: Path, slack_updates: bool, upload_to_s3: bool, is_backup: bool, industry: str, semaphore: asyncio.Semaphore) -> None:
    async with semaphore:
        try:
            log_name = f"{industry}.downloader"
            log_path = today_dir / f"log_download_{industry}_{today_str}.log"
            logger = setup_logger(log_name, log_path)

            registry = Registry(industry, upload_to_s3=upload_to_s3)
            registry.load()

            await run_downloaders(today_str, slack_updates, is_backup, industry, logger, registry)

            registry.save()

        except Exception as e:
            print(f"\n** Error {repr(e)}\n\n````{traceback.format_exc()}````")
            if slack_updates:
                Slack().send_app_update(f"{'Backup ' if is_backup else ''}Downloader ({industry.capitalize()})", False, exception=e)


async def run_all_industries(today_str: str, slack_updates: bool, upload_to_s3: bool, is_backup: bool) -> None:
    today_dir = HISTORIC_DATA_FOLDER() / today_str
    os.makedirs(today_dir, exist_ok=True)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_INDUSTRIES)

    tasks = [
        run_industry(today_str, today_dir, slack_updates, upload_to_s3, is_backup, industry, semaphore)
        for industry in INDUSTRIES
    ]

    await asyncio.gather(*tasks)


def run(today_str: str, slack_updates: bool = True, upload_to_s3: bool = True, is_backup: bool = False) -> None:
    asyncio.run(run_all_industries(today_str, slack_updates, upload_to_s3, is_backup))


if __name__ == "__main__":
    # run(get_current_datetime().strftime("%Y-%m-%d"), False, False)
    run_downloaders(get_current_datetime().strftime("%Y-%m-%d"), True, False, "banking", setup_logger("banking.downloader", Path("temp/log_download_banking.log")), Registry("banking"))
