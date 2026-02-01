"""
Household Food Basket PDF Scraper

Scrapes the PMBEJD Household Affordability Index page,
downloads the latest PDF, extracts the
'Household Food Basket: Per area, compared' table,
and loads it into a Pandas DataFrame.
"""

from io import BytesIO
from urllib.parse import urljoin
import hashlib
import re

import requests
import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup


INDEX_URL = "https://pmbejd.org.za/index.php/household-affordability-index"

# Stable part of the title (month/year + section number vary)
TARGET_TITLE_REGEX = re.compile(
    r"(?:\d+\.\s*)?"                  # optional section number like "8. "
    r"(?:[A-Z]+\s+\d{4}\s+)?"         # optional month+year like "JANUARY 2026 "
    r"Household\s+Food\s+Basket\s*:"  # "Household Food Basket:"
    r"\s*Per\s+area\s*,\s*compared",  # "Per area, compared"
    re.IGNORECASE,
)

EXPECTED_COLUMNS = {
    "foods tracked",
    "quantity tracked",
    "joburg",
    "durban",
    "cape town",
}


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def _norm(s: str | None) -> str:
    """Normalize PDF text/cell content for matching (collapse whitespace)."""
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip().lower()


def _title_matches(page_text: str) -> bool:
    """Match the target table title while ignoring varying month/year/section."""
    return bool(TARGET_TITLE_REGEX.search(page_text or ""))


# -------------------------------------------------------------------
# Web scraping
# -------------------------------------------------------------------

def get_latest_pdf_url(index_url: str) -> str:
    """Scrape index page and return the latest PDF URL."""
    response = requests.get(
        index_url,
        timeout=30,
        headers={"User-Agent": "household-food-basket-scraper/1.0"},
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.lower().endswith(".pdf"):
            return urljoin(index_url, href)

    raise RuntimeError("No PDF link found on index page")


def download_pdf(pdf_url: str) -> tuple[bytes, str]:
    """Download PDF and return bytes + SHA256 checksum."""
    response = requests.get(
        pdf_url,
        timeout=30,
        headers={"User-Agent": "household-food-basket-scraper/1.0"},
    )
    response.raise_for_status()

    if "pdf" not in response.headers.get("Content-Type", "").lower():
        raise ValueError("Downloaded file is not a PDF")

    pdf_bytes = response.content
    checksum = hashlib.sha256(pdf_bytes).hexdigest()

    return pdf_bytes, checksum


# -------------------------------------------------------------------
# PDF extraction
# -------------------------------------------------------------------

def extract_target_table(pdf_bytes: bytes) -> list[list[str]]:
    """
    Extract the 'Household Food Basket: Per area, compared' table
    from a multi-table PDF.

    Strategy:
    - Prefer pages where the title matches (regex that ignores month/year/section).
    - On those pages, pick the table whose header contains EXPECTED_COLUMNS.
    - If title detection fails due to PDF text quirks, fall back to scanning all pages
      for a table with the expected header.
    """
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        title_hit_pages: list[int] = []

        # Pass 1: pages with matching title
        for page_number, page in enumerate(pdf.pages, start=1):
            page_text = page.extract_text() or ""
            if not _title_matches(page_text):
                continue

            title_hit_pages.append(page_number)
            tables = page.extract_tables() or []
            if not tables:
                # likely an index/contents mention of the title
                continue

            for table in tables:
                if not table or not table[0]:
                    continue

                header_norm = [_norm(cell) for cell in table[0] if _norm(cell)]
                if EXPECTED_COLUMNS.issubset(set(header_norm)):
                    print(f"Target table found on page {page_number}")
                    return table

        # Pass 2: fallback scan (sometimes title text isn't extracted reliably)
        for page_number, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables() or []
            for table in tables:
                if not table or not table[0]:
                    continue

                header_norm = [_norm(cell) for cell in table[0] if _norm(cell)]
                if EXPECTED_COLUMNS.issubset(set(header_norm)):
                    print(f"Target table found on page {page_number} (fallback)")
                    return table

    if title_hit_pages:
        raise RuntimeError(
            f"Title matched on page(s) {title_hit_pages}, but no matching table was detected. "
            "This can happen if the PDF table header format changed."
        )

    raise RuntimeError("Household Food Basket table not found in PDF")


# -------------------------------------------------------------------
# DataFrame creation
# -------------------------------------------------------------------

def table_to_dataframe(table: list[list[str]]) -> pd.DataFrame:
    """Convert extracted table into a Pandas DataFrame."""
    header = table[0]
    rows = table[1:]

    df = pd.DataFrame(rows, columns=header)

    # Clean column names
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.replace("Averag", "Average", regex=False)
    )

    return df


# -------------------------------------------------------------------
# Main orchestration
# -------------------------------------------------------------------

def run() -> pd.DataFrame:
    pdf_url = get_latest_pdf_url(INDEX_URL)
    print(f"Latest PDF: {pdf_url}")

    pdf_bytes, checksum = download_pdf(pdf_url)
    print(f"PDF checksum: {checksum}")

    table = extract_target_table(pdf_bytes)
    df = table_to_dataframe(table)

    print("\nExtracted DataFrame preview:")
    print(df.head())

    return df


if __name__ == "__main__":
    run()
