"""Duplicate matching for processed car ads."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Protocol

from app.shared.models.processed import CaradDocData

logger = logging.getLogger(__name__)

TIME_WINDOW_DAYS = 5
PARSER_LAG_DAYS = 1
PRICE_TOLERANCE = 0.10
MILEAGE_TOLERANCE = 0.05
MAX_RESULTS = 200

SEARCH_SOURCE_FIELDS = [
    "offer_end",
    "latest_price",
    "mileage",
]


class SearchClient(Protocol):
    async def search(self, **kwargs: Any) -> Mapping[str, Any]:
        """Execute an Elasticsearch search request."""
        ...


class DuplicateMatcher:
    """Find the best duplicate for a processed ad."""

    def __init__(
        self,
        *,
        client: SearchClient,
        index_name: str,
        max_results: int = MAX_RESULTS,
    ) -> None:
        self._client = client
        self._index_name = index_name
        self._max_results = max(1, min(max_results, 500))

    async def find_best_duplicate(
        self,
        candidate: CaradDocData,
        candidate_id: str | None = None,
    ) -> tuple[str | None, float]:
        query = _build_search_query(candidate, candidate_id=candidate_id)
        response = await self._client.search(
            index=self._index_name,
            query=query,
            size=self._max_results,
            source=SEARCH_SOURCE_FIELDS,
        )

        hits = response.get("hits", {}).get("hits", [])
        best_duplicate_id: str | None = None
        best_score = 0.0
        best_breakdown: dict[str, float] | None = None

        for hit in hits:
            if not isinstance(hit, Mapping):
                continue

            duplicate_id = _extract_duplicate_id(hit)
            source = _extract_source(hit)
            if duplicate_id is None or source is None:
                continue

            score, breakdown = _score_duplicate_hit(candidate, source)
            if score is None:
                logger.debug("Skipping %s: missing offer_end", duplicate_id)
                continue

            if best_duplicate_id is None or score > best_score:
                best_duplicate_id = duplicate_id
                best_score = score
                best_breakdown = breakdown

        if best_duplicate_id is None:
            return None, 0.0

        logger.debug(
            "Selected duplicate candidate %s after Python scoring: score=%.6f breakdown=%s",
            best_duplicate_id,
            best_score,
            best_breakdown,
        )
        return best_duplicate_id, best_score


_default_matcher: DuplicateMatcher | None = None


def configure_matcher(
    *,
    client: SearchClient,
    index_name: str,
    max_results: int = MAX_RESULTS,
) -> None:
    """Configure the module-level matcher used by `find_best_duplicate`."""

    global _default_matcher
    _default_matcher = DuplicateMatcher(
        client=client,
        index_name=index_name,
        max_results=max_results,
    )


async def find_best_duplicate(
    candidate: CaradDocData,
    candidate_id: str | None = None,
) -> tuple[str | None, float]:
    """Find the best duplicate for the provided candidate."""

    if _default_matcher is None:
        raise RuntimeError("Matching service is not configured.")
    return await _default_matcher.find_best_duplicate(candidate, candidate_id=candidate_id)


def _build_search_query(candidate: CaradDocData, *, candidate_id: str | None = None) -> dict[str, Any]:
    must_not: list[dict[str, Any]] = [
        {"exists": {"field": "successor_id"}},
    ]

    resolved_candidate_id = _extract_candidate_id(candidate_id, candidate)
    if resolved_candidate_id is not None:
        must_not.insert(0, {"term": {"_id": resolved_candidate_id}})

    filters: list[dict[str, Any]] = [
        {
            "range": {
                "offer_end": {
                    "gte": (candidate.offer_start - timedelta(days=TIME_WINDOW_DAYS)).isoformat(),
                    "lte": (candidate.offer_start + timedelta(days=PARSER_LAG_DAYS)).isoformat(),
                }
            }
        },
    ]

    price_filter = _build_relative_range_filter(
        field_name="latest_price",
        value=candidate.initial_price,
        tolerance=PRICE_TOLERANCE,
    )
    if price_filter is not None:
        filters.append(price_filter)

    mileage_filter = _build_relative_range_filter(
        field_name="mileage",
        value=candidate.mileage,
        tolerance=MILEAGE_TOLERANCE,
    )
    if mileage_filter is not None:
        filters.append(mileage_filter)

    term_clauses = [
        ("site_name.keyword", candidate.site_name),
        ("region.keyword", candidate.region),
        ("seller_type", candidate.seller_type),
        ("name.keyword", candidate.name),
        ("brand.keyword", candidate.brand),
        ("model.keyword", candidate.model),
        ("generation.keyword", candidate.generation),
        ("build_year", candidate.build_year),
        ("vin.keyword", candidate.vin),
        ("place.keyword", candidate.place),
        ("steering_position.keyword", candidate.steering_position),
        ("gear_box.keyword", candidate.gear_box),
        ("gear_type.keyword", candidate.gear_type),
        ("body_type.keyword", candidate.body_type),
        ("doors_num", candidate.doors_num),
        ("modification.keyword", candidate.modification),
        ("complectation.keyword", candidate.complectation),
        ("engine_power", candidate.engine_power),
        ("engine_volume_liters", candidate.engine_volume_liters),
        ("fuel.keyword", candidate.fuel),
        ("body_color.keyword", candidate.body_color),
        ("count_owner", candidate.count_owner),
        ("condition.keyword", candidate.condition),
        ("is_new", candidate.is_new),
    ]
    for field_name, value in term_clauses:
        _append_exact_or_missing_clause(filters, must_not, field_name, value)

    return {"bool": {"must_not": must_not, "filter": filters}}


def _extract_candidate_id(candidate_id: str | None, candidate: CaradDocData) -> str | None:
    if candidate_id is not None:
        cleaned_candidate_id = str(candidate_id).strip()
        if cleaned_candidate_id:
            return cleaned_candidate_id

    if candidate.original_id is None:
        return None
    fallback_candidate_id = str(candidate.original_id).strip()
    if not fallback_candidate_id:
        return None
    return fallback_candidate_id


def _build_relative_range_filter(
    *,
    field_name: str,
    value: int | float | None,
    tolerance: float,
) -> dict[str, Any] | None:
    numeric_value = _coerce_number(value)
    if numeric_value is None or tolerance < 0:
        return None

    lower_bound = round(numeric_value * (1 - tolerance), 6)
    upper_bound = round(numeric_value * (1 + tolerance), 6)
    if lower_bound > upper_bound:
        return None

    return {
        "range": {
            field_name: {
                "gte": lower_bound,
                "lte": upper_bound,
            }
        }
    }


def _score_duplicate_hit(candidate: CaradDocData, source: Mapping[str, Any]) -> tuple[float | None, dict[str, float]]:
    offer_end = _parse_datetime(source.get("offer_end"))
    if offer_end is None:
        return None, {}

    earned_score = 0.0
    max_score = 0.0
    breakdown: dict[str, float] = {}

    offer_end_score, offer_end_weight = _score_date_proximity(
        actual=offer_end,
        target=candidate.offer_start,
        scale=timedelta(days=TIME_WINDOW_DAYS),
        weight=3.0,
    )
    earned_score += offer_end_score
    max_score += offer_end_weight
    breakdown["offer_end"] = offer_end_score

    price_score, price_weight = _score_numeric_proximity(
        actual=source.get("latest_price"),
        target=candidate.initial_price,
        tolerance=PRICE_TOLERANCE,
        weight=2.0,
    )
    earned_score += price_score
    max_score += price_weight
    breakdown["latest_price"] = price_score

    mileage_score, mileage_weight = _score_numeric_proximity(
        actual=source.get("mileage"),
        target=candidate.mileage,
        tolerance=MILEAGE_TOLERANCE,
        weight=1.5,
    )
    earned_score += mileage_score
    max_score += mileage_weight
    breakdown["mileage"] = mileage_score

    if max_score <= 0:
        return 0.0, breakdown

    normalized_score = round(min(max(earned_score / max_score, 0.0), 1.0), 6)
    breakdown["normalized"] = normalized_score
    return normalized_score, breakdown


def _score_numeric_proximity(
    *,
    actual: Any,
    target: int | float | None,
    tolerance: float,
    weight: float,
) -> tuple[float, float]:
    actual_value = _coerce_number(actual)
    target_value = _coerce_number(target)
    if actual_value is None or target_value is None or tolerance < 0 or weight <= 0:
        return 0.0, 0.0

    scale = max(abs(target_value) * tolerance, 1.0)
    distance = abs(actual_value - target_value)
    proximity = max(0.0, 1.0 - (distance / scale))
    return round(proximity * weight, 6), weight


def _score_date_proximity(
    *,
    actual: datetime,
    target: datetime,
    scale: timedelta,
    weight: float,
) -> tuple[float, float]:
    scale_seconds = scale.total_seconds()
    if scale_seconds <= 0 or weight <= 0:
        return 0.0, 0.0

    distance_seconds = abs((actual - target).total_seconds())
    proximity = max(0.0, 1.0 - (distance_seconds / scale_seconds))
    return round(proximity * weight, 6), weight


def _append_exact_or_missing_clause(
    filters: list[dict[str, Any]],
    must_not: list[dict[str, Any]],
    field_name: str,
    value: Any,
) -> None:
    term_clause = _build_exact_term_clause(field_name, value)
    if term_clause is not None:
        filters.append(term_clause)
        return

    must_not.append({"exists": {"field": field_name.split(".")[0]}})


def _build_exact_term_clause(field_name: str, value: Any) -> dict[str, Any] | None:
    if isinstance(value, str):
        cleaned = _clean_string(value)
        if cleaned is None:
            return None
        return {"term": {field_name: cleaned}}

    if value is None:
        return None

    return {"term": {field_name: value}}


def _extract_duplicate_id(hit: Mapping[str, Any]) -> str | None:
    hit_id = hit.get("_id")
    if hit_id is None:
        return None
    return str(hit_id)


def _extract_source(hit: Mapping[str, Any]) -> Mapping[str, Any] | None:
    source = hit.get("_source")
    if isinstance(source, Mapping):
        return source
    return None


def _clean_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return cleaned


def _coerce_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
