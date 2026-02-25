import pathlib
from dataclasses import asdict, dataclass
from typing import Optional, Union

from aws.aws import AWS
from utils.fs import check_exists, get_root_dir, read_json_file, write_json_file

from .config import INDUSTRY_CONFIG


@dataclass
class SummaryData:
    brandName: str
    baseUri: str
    firstSeen: Optional[str]
    lastSeen: Optional[str]
    brandNameOverride: Optional[str] = None
    baseUriOverride: Optional[str] = None
    last200Response: Optional[str] = None
    skip: bool = False


@dataclass
class BankingDetailData:
    subBrand: Optional[str]
    productCategory: Optional[str]
    firstSeen: str
    lastSeen: str
    last200Response: Optional[str] = None
    skip: bool = False


@dataclass
class EnergyDetailData:
    subBrand: Optional[str]
    fuelType: Optional[str]
    firstSeen: str
    lastSeen: str
    last200Response: Optional[str] = None
    skip: bool = False


DetailData = Union[BankingDetailData, EnergyDetailData]


class Registry:
    def __init__(self, industry: str, upload_to_s3: bool = False):
        self.industry: str = industry
        self.upload_to_s3: bool = upload_to_s3

        self._summary_apis: dict[str, SummaryData] = {}
        self._detail_apis: dict[str, dict[str, DetailData]] = {}

        self._registry_path: pathlib.Path = get_root_dir() / "temp" / "registry"
        self._registry_path.mkdir(parents=True, exist_ok=True)

        self._summary_apis_filename: str = INDUSTRY_CONFIG[industry]["summary_apis_filename"]
        self._detail_apis_filename: str = INDUSTRY_CONFIG[industry]["detail_apis_filename"]

        self._summary_apis_file: str = str(self._registry_path / self._summary_apis_filename)
        self._detail_apis_file: str = str(self._registry_path / self._detail_apis_filename)

        self._summary_apis_file_s3: str = f"registry/{self._summary_apis_filename}"
        self._detail_apis_file_s3: str = f"registry/{self._detail_apis_filename}"

    # Local files

    def load(self) -> dict:
        try:
            print("Downloading registry files from S3")
            self._download_files_from_s3()
        except Exception as e:
            print(f"Error downloading registry files from S3: {e}")

        if check_exists(self._summary_apis_file):
            print(f"Loading summary APIs from {self._summary_apis_file}")
            summary_apis = read_json_file(self._summary_apis_file)
            self._summary_apis = {
                summary_id: SummaryData(**summary_data) 
                for summary_id, summary_data in summary_apis.items()
            }

        if check_exists(self._detail_apis_file):
            print(f"Loading detail APIs from {self._detail_apis_file}")
            detail_apis = read_json_file(self._detail_apis_file)
            detail_data_class = BankingDetailData if self.industry == "banking" else EnergyDetailData
            self._detail_apis = {
                summary_id: {
                    detail_id: detail_data_class(**detail_data)
                    for detail_id, detail_data in details.items()
                }
                for summary_id, details in detail_apis.items()
            }

    def save(self) -> None:
        print(f"Saving summary APIs to {self._summary_apis_file}")
        summary_apis = {
            summary_id: asdict(summary_api)
            for summary_id, summary_api in self._summary_apis.items()
        }
        write_json_file(self._summary_apis_file, summary_apis)

        print(f"Saving detail APIs to {self._detail_apis_file}")
        detail_apis = {
            summary_id: {
                detail_id: asdict(detail_api)
                for detail_id, detail_api in details.items()
            }
            for summary_id, details in self._detail_apis.items()
        }
        write_json_file(self._detail_apis_file, detail_apis)

        if self.upload_to_s3:
            try:
                print("Uploading registry files to S3")
                self._upload_files_to_s3()
            except Exception as e:
                print(f"Error uploading registry files to S3: {e}")

    def validate(self) -> None:
        # TODO: Implement validation logic for summary and detail files
        pass

    # S3

    def _download_files_from_s3(self) -> None:
        AWS().cdr_download(self._summary_apis_file_s3, self._summary_apis_file)
        AWS().cdr_download(self._detail_apis_file_s3, self._detail_apis_file)

    def _upload_files_to_s3(self) -> None:
        AWS().cdr_upload(self._summary_apis_file, self._summary_apis_file_s3)
        AWS().cdr_upload(self._detail_apis_file, self._detail_apis_file_s3)

    # Summary APIs

    def get_summary_apis(self) -> dict[str, SummaryData]:
        return self._summary_apis

    def get_summary_data(self, summary_id: str) -> Optional[SummaryData]:
        return self._summary_apis.get(summary_id)

    def create_summary_api(self, summary_id: str, summary_data: SummaryData) -> None:
        self._summary_apis[summary_id] = summary_data

    def delete_summary_api(self, summary_id: str) -> None:
        try:
            del self._summary_apis[summary_id]
        except KeyError:
            print(f"brandId '{summary_id}' not found in summary APIs")

    # Detail APIs

    def get_detail_apis(self) -> dict[str, dict[str, DetailData]]:
        return self._detail_apis

    def get_detail_data(self, summary_id: str, detail_id: str) -> Optional[DetailData]:
        return self._detail_apis.get(summary_id, {}).get(detail_id)

    def create_detail_api(self, summary_id: str, detail_id: str, detail_data: DetailData) -> None:
        self._detail_apis.setdefault(summary_id, {})[detail_id] = detail_data

    def delete_detail_api(self, summary_id: str, detail_id: Optional[str]) -> None:
        if detail_id:
            try:
                del self._detail_apis[summary_id][detail_id]
            except KeyError:
                print(f"detailId '{detail_id}' under brandId '{summary_id}' not found in detail APIs")
        else:
            try:
                del self._detail_apis[summary_id]
            except KeyError:
                print(f"brandId '{summary_id}' not found in detail APIs")


if __name__ == "__main__":
    for industry in ["banking", "energy"]:
        registry = Registry(industry)
        registry.load()

    # DETAIL_IDS_TO_REMOVE = {
    #     "49c500b3-dacf-eb11-a824-000d3a884a20": [
    #         "Package_HL_Invest_Non_FHB_and_FHB_PI_2YR_FR",
    #         "Package_HL_Invest_Non_FHB_and_FHB_PI_3YR_FR",
    #         "Package_HL_Invest_Non_FHB_and_FHB_IO_VR",
    #         "Package_HL_Invest_Non_FHB_and_FHB_PI_VR",
    #         "Standard_HL_Invest_PI_3YR_FR",
    #         "Package_HL_Invest_Non_FHB_and_FHB_IO_1YR_FR",
    #         "Package_HL_Invest_Non_FHB_and_FHB_PI_1YR_FR",
    #         "Standard_HL_Invest_IO_3YR_FR",
    #         "Package_HL_Owner_Occ_PI_1YR_FR",
    #         "Package_HL_Invest_Non_FHB_and_FHB_IO_2YR_FR",
    #         "Package_HL_Owner_Occ_PI_2YR_FR",
    #         "Standard_HL_Invest_PI_2YR_FR",
    #         "Standard_HL_Invest_PI_VR",
    #         "Standard_HL_Owner_Occ_PI_3YR_FR",
    #         "Budget_HL_Owner_Occ_VR",
    #         "Package_HL_Owner_Occ_FHB_PI_VR",
    #         "Package_HL_Owner_Occ_PI_3YR_FR",
    #         "Package_HL_Invest_Non_FHB_and_FHB_IO_3YR_FR",
    #         "Standard_HL_Invest_PI_1_YR_FR",
    #         "Standard_HL_Owner_Occ_PI_VR",
    #         "Standard_HL_Invest_IO_VR",
    #         "Standard_HL_Owner_Occ_PI_2YR_FR",
    #         "Budget_HL_Invest_VR",
    #         "Package_HL_Owner_Occ_FHB_PI_3YR_FR",
    #         "Bus_Mort_Loan_Special_3Yr_FR",
    #         "Standard_HL_Invest_IO_2YR_FR",
    #         "Commercial_Loan_VR",
    #         "Package_HL_Owner_Occ_FHB_PI_2YR_FR",
    #         "Bus_Mort_Loan_Special_VR",
    #         "Bus_Mort_Loan_VR",
    #         "Package_HL_Owner_Occ_PI_VR",
    #         "Budget_HL_Invest_Special_VR",
    #         "Standard_HL_Owner_Occ_PI_1YR_FR",
    #         "Package_HL_Owner_Occ_FHB_PI_1YR_FR",
    #         "Bus_Mort_Loan_1Yr_FR",
    #         "Bus_Mort_Loan_Special_1Yr_FR",
    #         "Standard_HL_Invest_IO_1YR_FR",
    #         "Bus_Mort_Loan_2Yr_FR",
    #         "Bus_Mort_Loan_Special_2Yr_FR",
    #         "Budget_HL_Owner_Occ_Special_VR",
    #         "Bus_Mort_Loan_3Yr_FR",
    #     ]
    # }

    # for summary_id, detail_ids in DETAIL_IDS_TO_REMOVE.items():
    #     print(f"Deleting {len(detail_ids)} detail APIs under brandId '{summary_id}'")

    #     for detail_id in detail_ids:
    #         registry.delete_detail_api(summary_id, detail_id)

    # registry.save()
