from difflib import SequenceMatcher
import re
from typing import Any

try:
    from rapidfuzz import fuzz
except ImportError:  # pragma: no cover - requirements include rapidfuzz.
    fuzz = None


AUTHOR_MATCH_THRESHOLD = 80.0
AUTHOR_MISMATCH_SCORE_CAP = 72.0


def _string_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [str(v) for v in value.values() if v]
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value if v]
    return [str(value)]


def _candidate_titles(candidate: dict[str, Any]) -> list[str]:
    titles = _string_list(candidate.get("title") or candidate.get("name"))
    titles.extend(_string_list(candidate.get("alternative_titles")))
    return [title for title in titles if title]


def _candidate_authors(candidate: dict[str, Any]) -> list[str]:
    authors = _string_list(candidate.get("author_names"))
    authors.extend(_string_list(candidate.get("author_name")))
    contributions = candidate.get("contributions")
    if isinstance(contributions, list):
        for contribution in contributions:
            if not isinstance(contribution, dict):
                continue
            author = contribution.get("author")
            if isinstance(author, dict) and author.get("name"):
                authors.append(str(author["name"]))
    return [author for author in authors if author]


def _candidate_series(candidate: dict[str, Any]) -> list[str]:
    series = _string_list(candidate.get("series_names"))
    featured = candidate.get("featured_series")
    if isinstance(featured, dict):
        featured_series = featured.get("series")
        if isinstance(featured_series, dict) and featured_series.get("name"):
            series.append(str(featured_series["name"]))
    return [item for item in series if item]


def _normalize_key(value: Any) -> str:
    text = str(value or "").lower()
    text = re.sub(r"&", " and ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _without_leading_article(value: Any) -> str:
    return re.sub(r"^(?:the|a|an)\s+", "", _normalize_key(value))


def _max_fuzzy_score(query: str, values: list[str]) -> float:
    return max((fuzzy_score(query, value) for value in values), default=0.0)


def _max_ratio_score(query: str, values: list[str]) -> float:
    return max((ratio_score(query, value) for value in values), default=0.0)


def candidate_match_details(
    query: str,
    candidate: dict[str, Any],
    *,
    author_name: str = "",
    series_names: list[str] | None = None,
) -> dict[str, Any]:
    titles = _candidate_titles(candidate)
    title_score = _max_fuzzy_score(query, titles)
    title_ratio_score = _max_ratio_score(query, titles)

    query_key = _without_leading_article(query)
    exact_title = bool(query_key) and any(_without_leading_article(title) == query_key for title in titles)

    candidate_authors = _candidate_authors(candidate)
    author_score = _max_fuzzy_score(author_name, candidate_authors) if author_name else 0.0
    author_mismatch = bool(
        author_name
        and candidate_authors
        and author_score < AUTHOR_MATCH_THRESHOLD
    )
    series_score = 0.0
    candidate_series = _candidate_series(candidate)
    for series_name in series_names or []:
        series_score = max(series_score, _max_fuzzy_score(series_name, candidate_series))

    score = title_score
    if exact_title:
        score = max(score, 100.0)
    if title_score >= 70 and author_score >= 90:
        score = max(score, 82.0)
    if title_score >= 65 and author_score >= 90 and series_score >= 90:
        score = max(score, 88.0)
    if author_mismatch:
        score = min(score, AUTHOR_MISMATCH_SCORE_CAP)

    return {
        "score": round(score, 1),
        "title_score": round(title_score, 1),
        "title_ratio_score": round(title_ratio_score, 1),
        "author_score": round(author_score, 1),
        "series_score": round(series_score, 1),
        "exact_title": exact_title,
        "author_mismatch": author_mismatch,
    }


def candidate_validation_text(candidate: dict[str, Any]) -> str:
    parts = [
        candidate.get("title") or candidate.get("name") or "",
        " ".join(_string_list(candidate.get("author_names"))),
        candidate.get("author_name") or "",
        " ".join(_string_list(candidate.get("series_names"))),
    ]
    books = candidate.get("books")
    if isinstance(books, list):
        parts.extend(str(book.get("title") if isinstance(book, dict) else book) for book in books[:3])
    return " ".join(part for part in parts if part).strip()


def fuzzy_score(query: str, candidate_text: str) -> float:
    query = str(query or "").strip()
    candidate_text = str(candidate_text or "").strip()
    if not query or not candidate_text:
        return 0.0
    if fuzz is not None:
        return float(fuzz.token_set_ratio(query, candidate_text))
    return SequenceMatcher(None, query.lower(), candidate_text.lower()).ratio() * 100


def ratio_score(query: str, candidate_text: str) -> float:
    query = str(query or "").strip()
    candidate_text = str(candidate_text or "").strip()
    if not query or not candidate_text:
        return 0.0
    if fuzz is not None:
        return float(fuzz.ratio(query, candidate_text))
    return SequenceMatcher(None, query.lower(), candidate_text.lower()).ratio() * 100


def pick_valid_candidate(
    query: str,
    candidates: list[dict[str, Any]],
    threshold: float,
    *,
    author_name: str = "",
    series_names: list[str] | None = None,
) -> tuple[dict[str, Any] | None, float, list[dict[str, Any]]]:
    scored = []
    best_item = None
    best_key = None
    for index, candidate in enumerate(candidates):
        details = candidate_match_details(
            query,
            candidate,
            author_name=author_name,
            series_names=series_names,
        )
        item = {"candidate": candidate, **details}
        scored.append(item)
        key = (
            details["score"],
            1 if details["exact_title"] else 0,
            details["author_score"],
            details["series_score"],
            details["title_score"],
            details["title_ratio_score"],
            -index,
        )
        if best_key is None or key > best_key:
            best_key = key
            best_item = item

    if best_item and best_item["score"] >= threshold:
        return best_item["candidate"], best_item["score"], scored

    best_score = best_item["score"] if best_item else 0.0
    return None, best_score, scored
