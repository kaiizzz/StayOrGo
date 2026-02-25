COMMON_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
}

INDUSTRIES = ["banking", "energy"]

MAX_CONCURRENT_INDUSTRIES = 2

INDUSTRY_CONFIG = {
    "banking": {
        "summary_apis_filename": "banking-summary-apis.json",
        "detail_apis_filename": "banking-detail-apis.json",
        "brands_summary_endpoint": "https://api.cdr.gov.au/cdr-register/v1/banking/data-holders/brands/summary",
        "summary_path": "/cds-au/v1/banking/products",
        "summary_api_name": "Get Products",
        "summary_api_versions": ["3", "4"],
        "summary_key": "products",
        "detail_api_name": "Get Product Detail",
        "detail_api_versions": ["4", "5", "6"],
        "detail_id_key": "productId",
        "detail_category_key": "productCategory",
        "update_api_response_table": True,
    },
    "energy": {
        "summary_apis_filename": "energy-summary-apis.json",
        "detail_apis_filename": "energy-detail-apis.json",
        "brands_summary_endpoint": "https://api.cdr.gov.au/cdr-register/v1/energy/data-holders/brands/summary",
        "summary_path": "/cds-au/v1/energy/plans",
        "summary_api_name": "Get Generic Plans",
        "summary_api_versions": ["1"],
        "summary_key": "plans",
        "detail_api_name": "Get Generic Plan Detail",
        "detail_api_versions": ["3"],
        "detail_id_key": "planId",
        "detail_category_key": "fuelType",
        "update_api_response_table": False,
    }
}
