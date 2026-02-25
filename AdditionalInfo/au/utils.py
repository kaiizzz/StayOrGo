from typing import Any, Optional, TypedDict, Union
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

from .async_requester import HttpResponse


class JsonHttpResponse(TypedDict):
    url: str
    requestParams: dict[str, Any]
    requestHeaders: dict[str, str]
    requestedAt: str # ISO 8601 string
    responseTime: Optional[float]
    responseHeaders: dict[str, str]
    statusCode: Optional[int]
    exception: Optional[str] # exception type
    body: Optional[Union[str, list, dict]]


def update_url_query_params(url: str, params: Optional[dict[str, Any]]) -> str:
    if not params:
        return url
    url_parts = list(urlparse(url))
    query = dict(parse_qsl(url_parts[4]))
    query.update({k: v for k, v in params.items() if v is not None})
    url_parts[4] = urlencode(query)
    return urlunparse(url_parts)


def serialise_http_response(response: HttpResponse) -> JsonHttpResponse:
    url = response["url"]
    params = response["requestParams"]
    requested_at = response["requestedAt"]
    exception = response["exception"]
    return {
        "url": update_url_query_params(url, params),
        "requestParams": params,
        "requestHeaders": response["requestHeaders"],
        "requestedAt": requested_at.isoformat(timespec="seconds"),
        "responseTime": response["responseTime"],
        "responseHeaders": response["responseHeaders"],
        "statusCode": response["statusCode"],
        "exception": type(exception).__name__ if exception else None,
        "body": response["body"],
    }


def is_empty_summary_response(body: Optional[Union[str, list, dict]], summary_key: str) -> bool:
    try:
        summary = body["data"][summary_key]
        return not (isinstance(summary, list) and summary)
    except Exception:
        return True


def is_empty_detail_response(body: Optional[Union[str, list, dict]]) -> bool:
    try:
        detail = body["data"]
        return not (isinstance(detail, dict) and detail)
    except Exception:
        return True


def format_master_filename(api_name: str, api_version: str, today_str: str) -> str:
    return f"{api_name.replace(' ', '_').lower()}_{api_version}_{today_str}.json"
