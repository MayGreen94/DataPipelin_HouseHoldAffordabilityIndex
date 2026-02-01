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
import requests
import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup


INDEX_URL = "https://pmbejd.org.za/index.php/household-affordability-index"
TARGET_HEADER = "Household Food Basket: Per area, compared"

EXPECTED_COLUMNS = {
    "foods tracked",
    "quantity tracked",
    "joburg",
    "durban",
    "cape town",
}


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
    """
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            page_text = (page.extract_text() or "").lower()

            if TARGET_HEADER.lower() not in page_text:
                continue

            tables = page.extract_tables()
            for table in tables:
                header = [cell.lower() for cell in table[0] if cell]
                if EXPECTED_COLUMNS.issubset(set(header)):
                    return table

            raise RuntimeError(
                f"Header found on page {page_number}, but matching table not detected"
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
