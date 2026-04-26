import html
import json
import re
from typing import Any


ISBN_RE = re.compile(r"(?:ISBN(?:-1[03])?:?\s*)?([0-9Xx][0-9Xx\-\s]{8,24}[0-9Xx])")
HTML_TAG_RE = re.compile(r"<[^>]+>")
NOISE_BRACKET_RE = re.compile(r"\(([^)]{1,80})\)|\[([^\]]{1,80})\]|\{([^}]{1,80})\}")
NOISE_TOKEN_RE = re.compile(
    r"\b("
    r"audiobook|audio\s*book|ebook|e-book|epub|mobi|azw3|pdf|mp3|m4b|flac|aac|"
    r"unabridged|abridged|retail|audible|overdrive|libby|complete|read\s+by|"
    r"\d+\s*kbps|\d+\s*kbit"
    r")\b",
    re.IGNORECASE,
)
SEPARATOR_RE = re.compile(r"\s+[-_:|/]\s+")
SERIES_NUMBER_RE = re.compile(r"\s*#\s*[\w.\-]+(?:\s*-\s*[\w.\-]+)?\s*$")
GENERIC_AUTHOR_NAMES = {
    "multiple authors",
    "n/a",
    "na",
    "unknown",
    "various",
    "various authors",
}


def normalize_whitespace(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def strip_markup(value: Any) -> str:
    return normalize_whitespace(HTML_TAG_RE.sub(" ", html.unescape(str(value or ""))))


def parse_mam_metadata_value(value: Any, *, is_series: bool = False) -> str:
    """Return a readable MAM metadata string from JSON-ish author/series fields."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join(normalize_whitespace(item) for item in value if normalize_whitespace(item))
    if isinstance(value, dict):
        items = []
        for entry in value.values():
            if is_series and isinstance(entry, (list, tuple)):
                name = normalize_whitespace(entry[0] if len(entry) else "")
                number = normalize_whitespace(entry[1] if len(entry) > 1 else "")
                if name and number:
                    items.append(f"{name} #{number}")
                elif name:
                    items.append(name)
            else:
                text = normalize_whitespace(entry)
                if text:
                    items.append(text)
        return html.unescape(", ".join(items))

    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return parse_mam_metadata_value(json.loads(text), is_series=is_series)
    except (json.JSONDecodeError, TypeError, ValueError):
        return strip_markup(text)


def _is_noise_bracket(match: re.Match) -> str:
    content = next((group for group in match.groups() if group), "")
    return " " if NOISE_TOKEN_RE.search(content) else match.group(0)


def clean_title(value: Any) -> str:
    """Clean tracker-ish title text enough for Hardcover search without overfitting."""
    text = strip_markup(value)
    if not text:
        return ""

    text = NOISE_BRACKET_RE.sub(_is_noise_bracket, text)
    text = re.sub(r"\b(?:mp3|m4b|epub|mobi|azw3|pdf)\b$", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d{2,4}\s*kbps\b", " ", text, flags=re.IGNORECASE)
    text = normalize_whitespace(text.replace("_", " "))

    # MAM titles often look like "Author - Title"; Hardcover search works better on
    # the title part when author metadata is already available separately.
    parts = [part.strip() for part in SEPARATOR_RE.split(text) if part.strip()]
    if len(parts) >= 2 and len(parts[0].split()) <= 5:
        text = parts[-1]

    return normalize_whitespace(text)


def _isbn10_is_valid(isbn: str) -> bool:
    if not re.fullmatch(r"\d{9}[\dX]", isbn):
        return False
    total = 0
    for index, char in enumerate(isbn):
        value = 10 if char == "X" else int(char)
        total += (10 - index) * value
    return total % 11 == 0


def _isbn13_is_valid(isbn: str) -> bool:
    if not re.fullmatch(r"\d{13}", isbn):
        return False
    if not (isbn.startswith("978") or isbn.startswith("979")):
        return False
    total = sum((1 if index % 2 == 0 else 3) * int(char) for index, char in enumerate(isbn))
    return total % 10 == 0


def normalize_isbn(value: Any) -> str | None:
    digits = re.sub(r"[^0-9Xx]", "", str(value or "")).upper()
    if len(digits) == 10 and _isbn10_is_valid(digits):
        return digits
    if len(digits) == 13 and _isbn13_is_valid(digits):
        return digits
    return None


def extract_isbns(result: dict[str, Any]) -> list[str]:
    """Extract valid ISBN-10/ISBN-13 values from known fields and text blobs."""
    fields = [
        "isbn",
        "isbn10",
        "isbn_10",
        "isbn13",
        "isbn_13",
        "isbn_info",
        "title",
        "description",
    ]
    found: list[str] = []
    seen = set()

    for field in fields:
        value = result.get(field)
        if not value:
            continue
        for match in ISBN_RE.finditer(str(value)):
            isbn = normalize_isbn(match.group(1))
            if isbn and isbn not in seen:
                seen.add(isbn)
                found.append(isbn)

    return found


def detect_author_name(result: dict[str, Any]) -> str:
    author = parse_mam_metadata_value(result.get("author_info"))
    if author:
        first_author = author.split(",")[0].strip()
        if first_author.lower() not in GENERIC_AUTHOR_NAMES:
            return first_author
    title = strip_markup(result.get("title"))
    parts = [part.strip() for part in SEPARATOR_RE.split(title) if part.strip()]
    if len(parts) >= 2 and len(parts[0].split()) <= 5:
        return parts[0]
    return ""


def detect_series_names(result: dict[str, Any]) -> list[str]:
    series_text = parse_mam_metadata_value(result.get("series_info"), is_series=True)
    names = []
    seen = set()
    for raw_item in series_text.split(","):
        name = SERIES_NUMBER_RE.sub("", raw_item).strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    return names


def mam_original_metadata(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": result.get("id"),
        "title": strip_markup(result.get("title")),
        "author_info": parse_mam_metadata_value(result.get("author_info")),
        "series_info": parse_mam_metadata_value(result.get("series_info"), is_series=True),
        "narrator_info": parse_mam_metadata_value(result.get("narrator_info")),
        "category": result.get("catname") or result.get("category"),
        "language": result.get("language_name") or result.get("lang_code") or result.get("language"),
    }
