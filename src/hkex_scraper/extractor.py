"""Document text and table extraction for PDF, HTML, and Excel files.

Outputs clean, structured Markdown suitable for LLM consumption and human reading.
Uses pymupdf4llm for PDF extraction (headings, bold, tables, lists) and applies
post-processing to remove page headers/footers, normalize bullets, and fix spacing.
"""

from __future__ import annotations

import io
import re
from typing import List, Tuple

from .utils import log, squash_ws

# ---------------------------------------------------------------------------
# Optional dependencies (graceful degradation)
# ---------------------------------------------------------------------------
try:
    import pymupdf  # type: ignore  # pip install PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    try:
        import fitz as pymupdf  # type: ignore  # older PyMuPDF
        PYMUPDF_AVAILABLE = True
    except ImportError:
        PYMUPDF_AVAILABLE = False

try:
    import pymupdf4llm  # type: ignore  # pip install pymupdf4llm
    PYMUPDF4LLM_AVAILABLE = True
except ImportError:
    PYMUPDF4LLM_AVAILABLE = False

try:
    from bs4 import BeautifulSoup, Tag  # type: ignore  # pip install beautifulsoup4
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
    if not PYMUPDF4LLM_AVAILABLE:
        print("WARNING: pymupdf4llm not installed. PDF Markdown extraction degraded. "
              "Run: pip install pymupdf4llm")
    if not BS4_AVAILABLE:
        print("WARNING: beautifulsoup4 not installed. HTML text extraction disabled. "
              "Run: pip install beautifulsoup4")
    if not OPENPYXL_AVAILABLE:
        print("WARNING: openpyxl not installed. Excel text extraction disabled. "
              "Run: pip install openpyxl")


# ---------------------------------------------------------------------------
# Markdown post-processing (shared across all extractors)
# ---------------------------------------------------------------------------

# Common HKEx PDF form codes that appear as page headers
_FORM_CODES = re.compile(
    r"^(?:FF\d{3}|EF\d{3}|MF\d{3}|SF\d{3}|CF\d{3})\s*$",
    re.MULTILINE,
)

# Page footer patterns: "Page X of Y", "v X.Y.Z", "Page X of Y v X.Y.Z"
_PAGE_FOOTERS = re.compile(
    r"^(?:Page\s+\d+\s+of\s+\d+(?:\s+v\s+[\d.]+)?|v\s+[\d.]+)\s*$",
    re.MULTILINE | re.IGNORECASE,
)

# Bullet normalization: various bullet characters → standard "- "
_BULLET_CHARS = re.compile(r"^[\s]*[•●○▪▸►◆◇→⇒➤]\s*", re.MULTILINE)

# Numbered list normalization: "(1)" or "(a)" style → "1." or "a."
_PAREN_NUMBERS = re.compile(r"^\((\d+)\)\s*", re.MULTILINE)
_PAREN_LETTERS = re.compile(r"^\(([a-z])\)\s*", re.MULTILINE)


def _clean_markdown(text: str) -> str:
    """Apply post-processing to clean up extracted Markdown text.

    Removes page headers/footers, normalizes bullets, collapses excessive
    blank lines, and trims trailing whitespace.
    """
    if not text:
        return ""

    # Remove HKEx form codes (e.g., "FF301")
    text = _FORM_CODES.sub("", text)

    # Remove page footers
    text = _PAGE_FOOTERS.sub("", text)

    # Normalize bullet characters to standard Markdown "- "
    text = _BULLET_CHARS.sub("- ", text)

    # Normalize parenthesized numbers/letters to standard list format
    text = _PAREN_NUMBERS.sub(r"\1. ", text)
    text = _PAREN_LETTERS.sub(r"\1. ", text)

    # Remove lines that are only dashes or underscores (decorative separators)
    text = re.sub(r"^[\s]*[-_=]{5,}[\s]*$", "", text, flags=re.MULTILINE)

    # Collapse 3+ consecutive blank lines to 2
    text = re.sub(r"\n{4,}", "\n\n\n", text)

    # Strip trailing whitespace from each line
    text = "\n".join(line.rstrip() for line in text.split("\n"))

    # Trim leading/trailing whitespace
    return text.strip()


# ---------------------------------------------------------------------------
# Markdown table helper (for HTML and Excel extractors)
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


def _extract_tables_from_md(md_text: str) -> List[dict]:
    """Parse Markdown tables from text and return structured table metadata.

    Scans the Markdown text for table blocks (lines starting with |) and
    extracts headers, row counts, and the raw Markdown for each table.
    """
    tables: List[dict] = []
    lines = md_text.split("\n")
    i = 0
    table_index = 0

    while i < len(lines):
        # Detect start of a Markdown table (line starts with |)
        if lines[i].strip().startswith("|"):
            table_lines: List[str] = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                table_lines.append(lines[i])
                i += 1

            # Need at least header + separator + 1 data row
            if len(table_lines) >= 3:
                table_index += 1
                header_line = table_lines[0]
                # Parse headers from first row
                headers = [
                    cell.strip()
                    for cell in header_line.strip("|").split("|")
                ]
                # Count data rows (skip header and separator)
                data_rows = [
                    ln for ln in table_lines[2:]
                    if ln.strip() and not re.match(r"^\|[\s\-:|]+\|$", ln.strip())
                ]
                tables.append({
                    "tableIndex": table_index,
                    "headers": headers,
                    "rowCount": len(data_rows),
                    "markdown": "\n".join(table_lines),
                })
        else:
            i += 1

    return tables


# ---------------------------------------------------------------------------
# PDF extraction (using pymupdf4llm for structured Markdown)
# ---------------------------------------------------------------------------

def extract_pdf_content(raw_bytes: bytes) -> Tuple[str, list]:
    """Extract text and structured tables from PDF bytes as clean Markdown.

    Uses pymupdf4llm.to_markdown() for high-quality extraction that preserves
    headings, bold/italic, tables, and lists. Falls back to basic PyMuPDF
    text extraction if pymupdf4llm is not available.
    """
    if not PYMUPDF_AVAILABLE:
        return "", []

    try:
        doc = pymupdf.open(stream=raw_bytes, filetype="pdf")

        if PYMUPDF4LLM_AVAILABLE:
            # Use pymupdf4llm for structured Markdown extraction
            md_text = pymupdf4llm.to_markdown(doc)
            doc.close()
            log(f"    pymupdf4llm extracted {len(md_text)} chars")
        else:
            # Fallback: basic text extraction with page separation
            pages_text: list = []
            for page in doc:
                page_text = page.get_text("text") or ""
                if page_text.strip():
                    pages_text.append(page_text)
            doc.close()
            md_text = "\n\n---\n\n".join(pages_text)
            log(f"    PyMuPDF fallback extracted {len(md_text)} chars")

        # Post-process: clean headers/footers, normalize formatting
        md_text = _clean_markdown(md_text)

        # Extract structured table metadata from the Markdown
        tables_json = _extract_tables_from_md(md_text)
        log(f"    Found {len(tables_json)} tables in PDF")

        return md_text, tables_json

    except Exception as e:
        log(f"  PDF extraction error: {e}")
        return "", []


# ---------------------------------------------------------------------------
# HTML extraction (preserving heading structure)
# ---------------------------------------------------------------------------

def extract_html_content(raw_bytes: bytes) -> Tuple[str, list]:
    """Extract text and tables from HTML bytes as clean Markdown.

    Preserves heading hierarchy (h1-h6 → # tags), converts HTML tables to
    Markdown tables, and maintains paragraph structure. Avoids duplicate
    content from nested elements by tracking already-processed nodes.
    """
    if not BS4_AVAILABLE:
        return "", []

    tables_json: list = []
    try:
        soup = BeautifulSoup(raw_bytes, "html.parser")

        # Remove non-content elements
        for s in soup(["script", "style", "nav", "footer", "header", "noscript"]):
            s.decompose()

        # Collect top-level content elements to avoid duplicate traversal.
        # HKEx HTML filings are often table-based layouts where the entire
        # page is wrapped in nested tables. We process tables separately
        # and extract text from non-table elements.
        md_parts: list = []
        table_index = 0
        processed_tables: set = set()  # Track processed table elements

        root = soup.body if soup.body else soup

        def _process_element(el) -> None:
            """Recursively process an element, skipping already-handled tables."""
            nonlocal table_index

            if not isinstance(el, Tag):
                return

            tag = el.name

            # Headings → Markdown headings
            if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
                level = int(tag[1])
                heading_text = squash_ws(el.get_text())
                if heading_text:
                    md_parts.append(f"\n\n{'#' * level} {heading_text}\n")
                return  # Don't recurse into heading children

            # Paragraphs → separated text blocks
            if tag == "p":
                p_text = squash_ws(el.get_text())
                if p_text:
                    md_parts.append(f"\n\n{p_text}\n")
                return

            # List items
            if tag == "li":
                li_text = squash_ws(el.get_text())
                if li_text:
                    parent = el.parent
                    if parent and parent.name == "ol":
                        siblings = list(parent.find_all("li", recursive=False))
                        idx = siblings.index(el) + 1 if el in siblings else 1
                        md_parts.append(f"\n{idx}. {li_text}")
                    else:
                        md_parts.append(f"\n- {li_text}")
                return

            # Tables → Markdown tables (only process innermost data tables)
            if tag == "table":
                if id(el) in processed_tables:
                    return
                processed_tables.add(id(el))

                # Check if this is a layout table (contains other tables)
                nested_tables = el.find_all("table")
                if nested_tables:
                    # Layout table — recurse into children instead
                    for child in el.children:
                        if isinstance(child, Tag):
                            _process_element(child)
                    return

                # Data table — extract as Markdown table
                table_index += 1
                rows_data: list = []
                headers: list = []
                for tr in el.find_all("tr"):
                    cells = [squash_ws(td.get_text()) for td in tr.find_all(["td", "th"])]
                    if not headers:
                        headers = cells
                    else:
                        rows_data.append(cells)
                if headers and rows_data:
                    md = _table_to_markdown(headers, rows_data)
                    if md:
                        md_parts.append(f"\n\n{md}\n")
                        tables_json.append({
                            "tableIndex": table_index,
                            "headers": headers,
                            "rowCount": len(rows_data),
                            "markdown": md,
                        })
                elif headers:
                    # Single-row table (common in HKEx layouts) — extract as text
                    text = " | ".join(c for c in headers if c.strip())
                    if text:
                        md_parts.append(f"\n\n{text}\n")
                return

            # Container elements — recurse into children
            if tag in ("div", "span", "td", "th", "tr", "tbody", "thead",
                       "tfoot", "section", "article", "main", "aside",
                       "ul", "ol", "dl", "dd", "dt", "figure",
                       "figcaption", "blockquote", "form", "fieldset"):
                for child in el.children:
                    if isinstance(child, Tag):
                        _process_element(child)
                    elif hasattr(child, 'string') and child.string:
                        text = child.string.strip()
                        if text:
                            md_parts.append(f" {text}")
                return

            # Line breaks
            if tag == "br":
                md_parts.append("\n")
                return

        _process_element(root)
        raw_md = "".join(md_parts)

        # If structured extraction produced very little, fall back to plain text
        if len(raw_md.strip()) < 50:
            raw_md = soup.get_text(separator="\n\n")

        md_text = _clean_markdown(raw_md)
        log(f"    HTML extracted {len(md_text)} chars, {len(tables_json)} tables")
        return md_text, tables_json

    except Exception as e:
        log(f"  HTML extraction error: {e}")
        return "", []


# ---------------------------------------------------------------------------
# Excel extraction (structured Markdown with sheet headings and tables)
# ---------------------------------------------------------------------------

def extract_excel_content(raw_bytes: bytes) -> Tuple[str, list]:
    """Extract text and tables from Excel bytes as clean Markdown.

    Each sheet becomes a section with a heading, and data is formatted as
    a Markdown table. Key-value style sheets (2 columns) are formatted
    as definition lists for readability.
    """
    if not OPENPYXL_AVAILABLE:
        return "", []

    tables_json: list = []
    all_parts: list = []
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

            # Sheet heading
            sheet_md = f"## Sheet: {sheet_name}\n\n"

            headers = rows_data[0]
            data_rows = rows_data[1:]

            # Detect key-value layout (2 columns, first column is labels)
            is_kv = (
                len(headers) == 2
                and len(data_rows) >= 2
                and all(row[0].strip() for row in data_rows if len(row) >= 2)
            )

            if is_kv:
                # Format as key-value list
                kv_lines: list = []
                label_h = squash_ws(headers[0]) if headers[0].strip() else "Field"
                value_h = squash_ws(headers[1]) if headers[1].strip() else "Value"
                kv_lines.append(f"**{label_h}** | **{value_h}**")
                kv_lines.append("---|---")
                for row in data_rows:
                    key = squash_ws(row[0]) if len(row) > 0 else ""
                    val = squash_ws(row[1]) if len(row) > 1 else ""
                    kv_lines.append(f"{key} | {val}")
                sheet_md += "\n".join(kv_lines)
                tables_json.append({
                    "tableIndex": sheet_idx,
                    "sheetName": sheet_name,
                    "headers": [label_h, value_h],
                    "rowCount": len(data_rows),
                    "markdown": "\n".join(kv_lines),
                })
            else:
                # Standard table format
                md = _table_to_markdown(headers, data_rows)
                if md:
                    sheet_md += md
                    tables_json.append({
                        "tableIndex": sheet_idx,
                        "sheetName": sheet_name,
                        "headers": [squash_ws(str(h)) for h in headers],
                        "rowCount": len(data_rows),
                        "markdown": md,
                    })

            all_parts.append(sheet_md)

        wb.close()
    except Exception as e:
        log(f"  Excel extraction error: {e}")

    md_text = _clean_markdown("\n\n".join(all_parts))
    log(f"    Excel extracted {len(md_text)} chars, {len(tables_json)} tables")
    return md_text, tables_json


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
