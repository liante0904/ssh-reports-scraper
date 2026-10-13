"""Bounded content verification for LS CDN URLs inferred without an attachment."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
import re
import subprocess
import tempfile
import unicodedata
from urllib.parse import urlparse

import requests
from pypdf import PdfReader


MAX_PDF_BYTES = 20 * 1024 * 1024


@dataclass(frozen=True)
class PdfVerificationResult:
    matched: bool
    reason: str


def normalize_ls_title(value: str) -> str:
    """Make Korean/English report titles comparable without fuzzy guessing."""
    value = unicodedata.normalize("NFKC", value or "").lower()
    value = re.sub(r"^\s*\[[^\]]+\]\s*", "", value)
    return "".join(re.findall(r"[가-힣a-z0-9]+", value))


def title_matches_pdf(expected_title: str, extracted_text: str) -> bool:
    expected = normalize_ls_title(expected_title)
    observed = normalize_ls_title(extracted_text)
    if not expected or not observed:
        return False
    if expected in observed:
        return True

    # Never accept scattered title words from the body.  Board titles are
    # commonly stored with an ellipsis, and their normalized prefix still
    # appears contiguously in the PDF title.  A false negative is safe here;
    # a false positive republishes the wrong research report.
    return False


def _ocr_first_page(pdf_bytes: bytes) -> str:
    """OCR only PDFs with no extractable text; the image is never persisted."""
    with tempfile.TemporaryDirectory(prefix="ls-pdf-ocr-") as tmp:
        root = Path(tmp)
        pdf_path = root / "candidate.pdf"
        pdf_path.write_bytes(pdf_bytes)
        image_prefix = root / "page"
        subprocess.run(
            ["pdftoppm", "-f", "1", "-l", "1", "-r", "150", "-png", str(pdf_path), str(image_prefix)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=20,
        )
        images = list(root.glob("page-*.png"))
        if not images:
            return ""
        completed = subprocess.run(
            ["tesseract", str(images[0]), "stdout", "-l", "kor+eng", "--psm", "6"],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        )
        return completed.stdout


def _same_detail_attachment_stem(source_name: str, pdf_url: str) -> bool:
    source = re.fullmatch(
        r"(?P<prefix>.+)_(?P<seq>\d+)_(?P<date>\d{8})\.(?:png|jpe?g|pdf)",
        Path(source_name).name,
        re.IGNORECASE,
    )
    candidate = re.fullmatch(
        r"K_(?P<date>\d{8})_(?P<prefix>.+)_(?P<seq>\d+)\.pdf",
        Path(urlparse(pdf_url).path).name,
        re.IGNORECASE,
    )
    return bool(
        source
        and candidate
        and source.group("date") == candidate.group("date")
        and source.group("prefix").casefold() == candidate.group("prefix").casefold()
        and source.group("seq") == candidate.group("seq")
    )


def _detail_attachment_metadata_matches(
    source_name: str,
    pdf_url: str,
    expected_title: str,
    expected_writer: str,
    expected_date: str,
    first_page_text: str,
) -> bool:
    """Use exact source-file identity plus cover metadata for renamed titles."""
    if not _same_detail_attachment_stem(source_name, pdf_url):
        return False
    page = normalize_ls_title(first_page_text)
    writer = normalize_ls_title(expected_writer)
    date_digits = re.sub(r"\D", "", expected_date)
    if len(date_digits) != 8:
        return False
    year, month, day = int(date_digits[:4]), int(date_digits[4:6]), int(date_digits[6:])
    normalized_date = normalize_ls_title(f"{year} {month} {day}")
    if not writer or writer not in page or normalized_date not in page:
        return False

    # Require a distinctive topic word from the board title on the PDF cover.
    # This avoids accepting a same-author, same-day neighboring report.
    title_body = re.sub(r"^\s*\[[^\]]+\]\s*", "", expected_title or "")
    topic_words = re.findall(r"[가-힣]{3,}", title_body)
    return any(normalize_ls_title(word) in page for word in topic_words)


def verify_ls_pdf_candidate(
    url: str,
    expected_title: str,
    headers: dict,
    proxies: dict | None = None,
    *,
    source_attachment_name: str = "",
    expected_writer: str = "",
    expected_date: str = "",
) -> PdfVerificationResult:
    """Verify title text, or an exact detail-linked image/PDF identity pair."""
    if not expected_title:
        return PdfVerificationResult(False, "missing expected article title")

    try:
        response = requests.get(url, headers=headers, proxies=proxies, verify=False, timeout=20, stream=True)
        if response.status_code != 200:
            return PdfVerificationResult(False, f"HTTP {response.status_code}")
        chunks: list[bytes] = []
        size = 0
        for chunk in response.iter_content(64 * 1024):
            if not chunk:
                continue
            size += len(chunk)
            if size > MAX_PDF_BYTES:
                return PdfVerificationResult(False, "PDF exceeds verification size limit")
            chunks.append(chunk)
        pdf_bytes = b"".join(chunks)
    except Exception as exc:
        return PdfVerificationResult(False, f"download failed: {type(exc).__name__}")

    if not pdf_bytes.startswith(b"%PDF-"):
        return PdfVerificationResult(False, "response is not a PDF")

    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        page_texts = [page.extract_text() or "" for page in reader.pages[:2]]
        first_page_text = page_texts[0] if page_texts else ""
    except Exception as exc:
        return PdfVerificationResult(False, f"PDF parse failed: {type(exc).__name__}")

    if any(title_matches_pdf(expected_title, text) for text in page_texts):
        return PdfVerificationResult(True, "first-two-page text matched")

    if _detail_attachment_metadata_matches(
        source_attachment_name,
        url,
        expected_title,
        expected_writer,
        expected_date,
        first_page_text,
    ):
        return PdfVerificationResult(True, "detail-linked attachment stem and cover metadata matched")

    if normalize_ls_title(first_page_text):
        return PdfVerificationResult(False, "first-two-page text title mismatch")

    try:
        ocr_text = _ocr_first_page(pdf_bytes)
    except Exception as exc:
        return PdfVerificationResult(False, f"OCR failed: {type(exc).__name__}")
    return PdfVerificationResult(
        title_matches_pdf(expected_title, ocr_text),
        "first-page OCR matched" if title_matches_pdf(expected_title, ocr_text) else "first-page OCR title mismatch",
    )
