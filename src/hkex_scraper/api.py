"""HKEx JSON API: session management, form POST, record fetching, monthly chunking."""

from __future__ import annotations

import re
from calendar import monthrange
from datetime import datetime
from typing import List, Tuple

from .config import HKEX_API_ENDPOINT, HKEX_BASE_URL, HKEX_SEARCH_PAGE
from .utils import log, squash_ws

# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------
try:
    import requests as _requests  # type: ignore
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

try:
    from bs4 import BeautifulSoup  # type: ignore
    _BS4_AVAILABLE = True
except ImportError:
    _BS4_AVAILABLE = False

# ---------------------------------------------------------------------------
# Record parsing
# ---------------------------------------------------------------------------

def _parse_api_record(record: dict) -> dict:
    """Convert a raw HKEx API JSON record into our internal filing format.

    The API returns records like::

        {
            "FILE_INFO": "53KB",
            "NEWS_ID": "12022263",
            "STOCK_NAME": "ZHONGTAIFUTURES",
            "STOCK_CODE": "01461",
            "TITLE": "Articles of Association",
            "FILE_TYPE": "PDF",
            "DATE_TIME": "11/02/2026 19:10",
            "FILE_LINK": "/listedco/listconews/sehk/2026/0211/2026021100854.pdf",
            "LONG_TEXT": "Announcements and Notices - [Other]",
            "TOTAL_COUNT": "14957"
        }
    """
    date_time = record.get("DATE_TIME", "")
    date_part = date_time.split(" ")[0] if date_time else ""

    raw_code = record.get("STOCK_CODE", "")
    raw_code = raw_code.split("<br/>")[0].strip()

    raw_name = record.get("STOCK_NAME", "")
    raw_name = raw_name.split("<br/>")[0].strip()

    file_link = record.get("FILE_LINK", "")
    if file_link and file_link.startswith("/"):
        file_link = HKEX_BASE_URL + file_link

    title = record.get("TITLE", "")
    title = title.replace("&#x3b;", ";").replace("&amp;", "&")

    return {
        "date": date_part,
        "stockCode": raw_code,
        "stockName": squash_ws(raw_name),
        "title": squash_ws(title),
        "link": file_link,
    }


# ---------------------------------------------------------------------------
# HTML fallback parser (for initial page)
# ---------------------------------------------------------------------------

def _parse_initial_page_html(html: str) -> Tuple[list, int]:
    """Parse the initial HKEx search page HTML to extract the first ~100 filings."""
    if not _BS4_AVAILABLE:
        return [], 0

    soup = BeautifulSoup(html, "html.parser")

    total_count = 0
    text = soup.get_text()
    m = re.search(r"Total records found:\s*([\d,]+)", text)
    if m:
        total_count = int(m.group(1).replace(",", ""))

    filings: List[dict] = []
    for row in soup.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        date_text = cells[0].get_text(strip=True).replace("Release Time:", "").strip()
        date_part = date_text.split(" ")[0] if date_text else ""
        code_text = cells[1].get_text(strip=True).replace("Stock Code:", "").strip()
        code_match = re.search(r"\d{4,5}", code_text)
        code = code_match.group(0) if code_match else ""
        name = re.sub(r"Stock Short Name:\s*", "", cells[2].get_text(strip=True)).strip()
        link_el = cells[3].find("a")
        link, title = "", ""
        if link_el:
            href = link_el.get("href", "")
            link = (HKEX_BASE_URL + href) if href.startswith("/") else href
            title = re.sub(r"Document:\s*", "", link_el.get_text(strip=True)).strip()
        if code and link:
            filings.append(
                {
                    "date": date_part,
                    "stockCode": code,
                    "stockName": squash_ws(name),
                    "title": squash_ws(title),
                    "link": link,
                }
            )
    return filings, total_count


# ---------------------------------------------------------------------------
# Monthly chunking
# ---------------------------------------------------------------------------

def generate_monthly_chunks(
    date_from: datetime, date_to: datetime
) -> List[Tuple[datetime, datetime]]:
    """Generate ``(chunk_from, chunk_to)`` pairs in 1-month increments (newest first).

    HKEx limits searches to one month when no stock code / document type is specified.
    """
    chunks: List[Tuple[datetime, datetime]] = []
    cursor = datetime(date_to.year, date_to.month, 1)
    while cursor >= datetime(date_from.year, date_from.month, 1):
        chunk_start = max(cursor, date_from)
        _, last_day = monthrange(cursor.year, cursor.month)
        chunk_end = min(datetime(cursor.year, cursor.month, last_day), date_to)
        chunks.append((chunk_start, chunk_end))
        if cursor.month == 1:
            cursor = datetime(cursor.year - 1, 12, 1)
        else:
            cursor = datetime(cursor.year, cursor.month - 1, 1)
    return chunks


# ---------------------------------------------------------------------------
# API fetch (single chunk)
# ---------------------------------------------------------------------------

def fetch_chunk_via_api(
    session,
    date_from_yyyymmdd: str,
    date_to_yyyymmdd: str,
    max_records: int = 0,
) -> list:
    """Fetch all filings for a date range chunk using the HKEx JSON API.

    Steps:
        1. POST the JSF form with from/to dates to set the session's date range.
        2. Call the JSON API with increasing ``rowRange`` to paginate through all records.
    """
    import json

    # Step 1: POST the form to set the date range on the server session
    page_resp = session.get(
        HKEX_SEARCH_PAGE,
        params={
            "sortDir": "0",
            "sortByRecordDate": "on",
            "searchType": "0",
            "category": "0",
            "t1code": "-2",
            "t2Gcode": "-2",
            "t2code": "-2",
            "documentType": "-1",
            "rowRange": "0",
            "lang": "EN",
        },
        timeout=30,
    )
    page_resp.raise_for_status()

    # Extract ViewState and form action
    if _BS4_AVAILABLE:
        soup = BeautifulSoup(page_resp.text, "html.parser")
        vs_el = soup.find("input", {"name": "javax.faces.ViewState"})
        view_state = vs_el["value"] if vs_el else ""
        form_el = soup.find("form")
        form_action = form_el.get("action", "") if form_el else ""
    else:
        vs_match = re.search(
            r'javax\.faces\.ViewState.*?value="([^"]+)"', page_resp.text
        )
        view_state = vs_match.group(1) if vs_match else ""
        fa_match = re.search(r'<form[^>]*action="([^"]+)"', page_resp.text)
        form_action = fa_match.group(1) if fa_match else ""

    submit_url = (
        f"{HKEX_BASE_URL}{form_action}"
        if form_action.startswith("/")
        else form_action
    )
    session.post(
        submit_url,
        data={
            "j_idt10": "j_idt10",
            "j_idt10:loadMoreRange": "100",
            "javax.faces.ViewState": view_state,
            "from": date_from_yyyymmdd,
            "to": date_to_yyyymmdd,
        },
        timeout=30,
    )

    # Step 2: Fetch records via the JSON API
    all_records: list = []
    API_CHUNK = 5000
    fetched = 0
    api_total = None

    while True:
        if max_records > 0 and len(all_records) >= max_records:
            break
        chunk_size = API_CHUNK
        if max_records > 0:
            chunk_size = min(chunk_size, max_records - len(all_records))
        row_range = fetched + chunk_size

        api_resp = session.get(
            HKEX_API_ENDPOINT,
            params={
                "sortDir": "0",
                "sortByOptions": "DateTime",
                "category": "0",
                "market": "SEHK",
                "stockId": "-1",
                "documentType": "-1",
                "fromDate": date_from_yyyymmdd,
                "toDate": date_to_yyyymmdd,
                "title": "",
                "searchType": "0",
                "t1code": "-2",
                "t2Gcode": "-2",
                "t2code": "-2",
                "rowRange": str(row_range),
                "lang": "E",
            },
            headers={
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Referer": HKEX_SEARCH_PAGE,
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=120,
        )
        api_resp.raise_for_status()

        data = api_resp.json()
        raw_result = data.get("result", "null")
        if not raw_result or raw_result == "null":
            break

        records = json.loads(raw_result)
        if api_total is None and records:
            api_total = int(records[0].get("TOTAL_COUNT", "0"))

        new_records = records[fetched:]
        for rec in new_records:
            all_records.append(_parse_api_record(rec))

        fetched = len(records)
        has_next = data.get("hasNextRow", False)
        if not has_next:
            break
        if api_total and fetched >= api_total:
            break

    return all_records
