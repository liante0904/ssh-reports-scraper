"""Bounded content verification for LS CDN URLs inferred without an attachment."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
import re
import subprocess
import tempfile
import unicodedata

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


def verify_ls_pdf_candidate(url: str, expected_title: str, headers: dict, proxies: dict | None = None) -> PdfVerificationResult:
    """Accept an inferred URL only when the first page proves its identity."""
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
        first_page_text = reader.pages[0].extract_text() or ""
    except Exception as exc:
        return PdfVerificationResult(False, f"PDF parse failed: {type(exc).__name__}")

    if normalize_ls_title(first_page_text):
        return PdfVerificationResult(
            title_matches_pdf(expected_title, first_page_text),
            "first-page text matched" if title_matches_pdf(expected_title, first_page_text) else "first-page text title mismatch",
        )

    try:
        ocr_text = _ocr_first_page(pdf_bytes)
    except Exception as exc:
        return PdfVerificationResult(False, f"OCR failed: {type(exc).__name__}")
    return PdfVerificationResult(
        title_matches_pdf(expected_title, ocr_text),
        "first-page OCR matched" if title_matches_pdf(expected_title, ocr_text) else "first-page OCR title mismatch",
    )
