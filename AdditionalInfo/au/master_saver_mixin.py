from cdr_monitor.util import HISTORIC_DATA_FOLDER
from downloaders.au.utils import format_master_filename
from utils.fs import write_json_file


class MasterSaverMixin:    
    def _save_master(self, master: dict) -> None:
        for api_name, api_versions in master.items():
            for api_version, brands in api_versions.items():
                master_filename = format_master_filename(api_name, api_version, self.today_str)
                master_file = HISTORIC_DATA_FOLDER() / self.today_str / master_filename
                self.logger.info(f"Writing to {master_filename}")
                write_json_file(master_file, brands)
