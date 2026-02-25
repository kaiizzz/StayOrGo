import os
from datetime import datetime
from typing import Optional

from asgiref.sync import sync_to_async
import django

from bank_data.au.bank_data import BankData

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from api.models import ApiResponse


@sync_to_async
def create_api_response_object(
    url: str,
    brand_id: str,
    brand_name: str,
    status_code: int,
    is_empty: bool,
    api_name: str,
    api_version: str,
    requested_at: datetime,
    sub_brand: Optional[str] = None, # N/A for summary responses
    product_category: Optional[str] = None, # N/A for summary responses
) -> None:
    try:
        provider = None

        if api_name == "Get Product Detail":
            cdr_key = f"{brand_name} / {sub_brand}" if sub_brand else brand_name
            provider = BankData().provider.get_provider_by_cdr_key(cdr_key)

        return ApiResponse(
            url=url,
            brand_id=brand_id,
            provider_id=provider.id[:5] if provider else None,
            status_code=status_code,
            is_empty=is_empty,
            product_category=product_category,
            api_name=api_name,
            api_version=api_version,
            created=requested_at,
        )

    except Exception as e:
        print(f"Error occurred during create_api_response_object: {e}")
        return None


@sync_to_async
def bulk_create_api_responses(api_responses: list[ApiResponse]) -> None:
    try:
        if api_responses:
            ApiResponse.objects.bulk_create(api_responses)
            print(f"Successfully created {len(api_responses)} ApiResponse objects")
    except Exception as e:
        print(f"Error occurred during bulk_create_api_responses: {e}")
