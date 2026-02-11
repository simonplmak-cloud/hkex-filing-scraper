"""Document text and table extraction for PDF, HTML, and Excel files."""

from __future__ import annotations

import io
from typing import Tuple

from .utils import log, squash_ws

# ---------------------------------------------------------------------------
# Optional dependencies (graceful degradation)
# ---------------------------------------------------------------------------
try:
    import fitz as pymupdf  # type: ignore  # pip install PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

try:
    from bs4 import BeautifulSoup  # type: ignore  # pip install beautifulsoup4
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

try:
    import openpyxl  # type: ignore  # pip install openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False


def check_dependencies() -> None:
    """Print warnings for missing optional extraction libraries."""
    if not PYMUPDF_AVAILABLE:
        print("WARNING: PyMuPDF not installed. PDF text extraction disabled. "
              "Run: pip install PyMuPDF")
    if not BS4_AVAILABLE:
        print("WARNING: beautifulsoup4 not installed. HTML text extraction disabled. "
              "Run: pip install beautifulsoup4")
    if not OPENPYXL_AVAILABLE:
        print("WARNING: openpyxl not installed. Excel text extraction disabled. "
              "Run: pip install openpyxl")


# ---------------------------------------------------------------------------
# Markdown table helper
# ---------------------------------------------------------------------------

def _table_to_markdown(headers: list, rows: list) -> str:
    """Convert a list of headers and rows into a Markdown table string."""
    if not headers and not rows:
        return ""
    if not headers or all(h is None for h in headers):
        ncols = len(rows[0]) if rows else 0
        headers = [f"Col{i + 1}" for i in range(ncols)]
    headers = [squash_ws(str(h)) if h else "" for h in headers]
    md_lines = ["| " + " | ".join(headers) + " |"]
    md_lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        cells = [squash_ws(str(c)) if c else "" for c in row]
        while len(cells) < len(headers):
            cells.append("")
        cells = cells[: len(headers)]
        md_lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(md_lines)


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------

def extract_pdf_content(raw_bytes: bytes) -> Tuple[str, list]:
    """Extract text and structured tables from PDF bytes."""
    if not PYMUPDF_AVAILABLE:
        return "", []
    tables_json: list = []
    pages_text: list = []
    table_index = 0
    try:
        doc = pymupdf.open(stream=raw_bytes, filetype="pdf")
        for page_num, page in enumerate(doc, start=1):
            page_text = page.get_text("text") or ""
            page_text = squash_ws(page_text)
            page_tables_md: list = []
            try:
                tab_finder = page.find_tables()
                for tab in tab_finder.tables:
                    table_index += 1
                    data = tab.extract()
                    if not data or len(data) < 2:
                        continue
                    headers = data[0]
                    rows = data[1:]
                    md = _table_to_markdown(headers, rows)
                    if md:
                        page_tables_md.append(md)
                        tables_json.append(
                            {
                                "tableIndex": table_index,
                                "page": page_num,
                                "headers": headers,
                                "rowCount": len(rows),
                                "markdown": md,
                            }
                        )
            except Exception:
                pass
            combined = page_text
            if page_tables_md:
                combined += "\n\n" + "\n\n".join(page_tables_md)
            pages_text.append(combined)
        doc.close()
    except Exception as e:
        log(f"  PDF extraction error: {e}")
    return "\n\n".join(pages_text), tables_json


# ---------------------------------------------------------------------------
# HTML extraction
# ---------------------------------------------------------------------------

def extract_html_content(raw_bytes: bytes) -> Tuple[str, list]:
    """Extract text and tables from HTML bytes."""
    if not BS4_AVAILABLE:
        return "", []
    tables_json: list = []
    try:
        soup = BeautifulSoup(raw_bytes, "html.parser")
        for s in soup(["script", "style", "nav", "footer", "header"]):
            s.decompose()
        text = squash_ws(soup.get_text(separator=" "))
        for idx, table in enumerate(soup.find_all("table"), start=1):
            rows_data: list = []
            headers: list = []
            for tr in table.find_all("tr"):
                cells = [squash_ws(td.get_text()) for td in tr.find_all(["td", "th"])]
                if not headers:
                    headers = cells
                else:
                    rows_data.append(cells)
            if headers and rows_data:
                md = _table_to_markdown(headers, rows_data)
                if md:
                    tables_json.append(
                        {
                            "tableIndex": idx,
                            "headers": headers,
                            "rowCount": len(rows_data),
                            "markdown": md,
                        }
                    )
        return text, tables_json
    except Exception as e:
        log(f"  HTML extraction error: {e}")
        return "", []


# ---------------------------------------------------------------------------
# Excel extraction
# ---------------------------------------------------------------------------

def extract_excel_content(raw_bytes: bytes) -> Tuple[str, list]:
    """Extract text and tables from Excel bytes."""
    if not OPENPYXL_AVAILABLE:
        return "", []
    tables_json: list = []
    all_text_parts: list = []
    try:
        wb = openpyxl.load_workbook(
            io.BytesIO(raw_bytes), read_only=True, data_only=True
        )
        for sheet_idx, sheet_name in enumerate(wb.sheetnames, start=1):
            ws = wb[sheet_name]
            rows_data: list = []
            for row in ws.iter_rows(values_only=True):
                cells = [str(c) if c is not None else "" for c in row]
                if any(c.strip() for c in cells):
                    rows_data.append(cells)
            if not rows_data:
                continue
            headers = rows_data[0]
            data_rows = rows_data[1:]
            md = _table_to_markdown(headers, data_rows)
            if md:
                tables_json.append(
                    {
                        "tableIndex": sheet_idx,
                        "sheetName": sheet_name,
                        "headers": headers,
                        "rowCount": len(data_rows),
                        "markdown": md,
                    }
                )
            text_parts: list = []
            for row in rows_data:
                text_parts.append(" ".join(c for c in row if c.strip()))
            all_text_parts.append(
                f"[Sheet: {sheet_name}]\n" + "\n".join(text_parts)
            )
        wb.close()
    except Exception as e:
        log(f"  Excel extraction error: {e}")
    return "\n\n".join(all_text_parts), tables_json


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

def extract_content_with_tables(
    raw_bytes: bytes, doc_url: str
) -> Tuple[str, list]:
    """Route to the appropriate extractor based on file extension."""
    u = doc_url.lower().split("?")[0].split("#")[0]
    if u.endswith(".pdf"):
        return extract_pdf_content(raw_bytes)
    if u.endswith(".htm") or u.endswith(".html"):
        return extract_html_content(raw_bytes)
    if u.endswith(".xlsx") or u.endswith(".xls"):
        return extract_excel_content(raw_bytes)
    return "", []
