"""Unit tests for processed-ad duplicate matching."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import pytest

from app.services.matching_service.matcher import DuplicateMatcher
from app.shared.models.processed import CaradDocData


class FakeSearchClient:
    def __init__(self, response: dict[str, Any]) -> None:
        self._response = response
        self.last_kwargs: dict[str, Any] | None = None

    async def search(self, **kwargs: Any) -> dict[str, Any]:
        self.last_kwargs = kwargs
        return self._response


def _build_candidate(**overrides: Any) -> CaradDocData:
    payload = {
        "original_id": "candidate-1",
        "parapi_unique_id": 101,
        "url": "https://example.com/ad/1",
        "site_name": "avito",
        "seller_type": "private",
        "name": "Seller",
        "last_checked": datetime(2026, 1, 10, 10, 0, 0),
        "parsed_at": datetime(2026, 1, 10, 10, 0, 0),
        "offer_start": datetime(2026, 1, 10, 10, 0, 0),
        "initial_price": 1_000_000.0,
        "latest_price": 1_000_000.0,
        "is_new": False,
        "brand": "Toyota",
        "model": "Camry",
        "generation": "XV70",
        "modification": "2.0 AT",
        "complectation": "Premium",
        "build_year": 2020,
        "vin": "VIN123456789",
        "engine_power": 150.0,
        "engine_volume_liters": 2.0,
        "fuel": "petrol",
        "gear_type": "front",
        "gear_box": "automatic",
        "steering_position": "left",
        "body_type": "sedan",
        "body_color": "black",
        "doors_num": "4",
        "count_owner": 1,
        "condition": "used",
        "mileage": 100_000,
        "place": "Moscow",
        "region": "Moscow region",
    }
    payload.update(overrides)
    return CaradDocData(**payload)


def test_duplicate_matcher_uses_strong_prefilters_and_selects_best_hit() -> None:
    candidate = _build_candidate()
    client = FakeSearchClient(
        {
            "hits": {
                "hits": [
                    {
                        "_id": "weaker-hit",
                        "_source": {
                            "offer_end": "2026-01-09T10:00:00",
                            "latest_price": 1_050_000.0,
                            "mileage": 130_000,
                            "generation": "XV50",
                            "modification": "2.5 AT",
                            "engine_power": 181.0,
                            "engine_volume_liters": 2.5,
                            "fuel": "petrol",
                            "region": "Tver region",
                            "place": "Tver",
                            "build_year": 2018,
                        },
                    },
                    {
                        "_id": "best-hit",
                        "_source": {
                            "offer_end": "2026-01-08T10:00:00",
                            "latest_price": 1_020_000.0,
                            "mileage": 98_000,
                            "generation": "XV70",
                            "modification": "2.0 AT",
                            "engine_power": 150.0,
                            "engine_volume_liters": 2.0,
                            "fuel": "petrol",
                            "region": "Moscow region",
                            "place": "Moscow",
                            "build_year": 2020,
                        },
                    },
                ]
            }
        }
    )
    matcher = DuplicateMatcher(client=client, index_name="processed-carads")

    duplicate_id, score = asyncio.run(
        matcher.find_best_duplicate(candidate, candidate_id="es-doc-123")
    )

    assert duplicate_id == "best-hit"
    assert score == pytest.approx(0.736527)
    assert client.last_kwargs is not None
    assert client.last_kwargs["index"] == "processed-carads"
    assert client.last_kwargs["size"] == 200
    assert client.last_kwargs["source"] == [
        "offer_end",
        "latest_price",
        "mileage",
        "place",
        "steering_position",
        "gear_box",
        "gear_type",
        "body_type",
        "doors_num",
        "modification",
        "complectation",
        "engine_power",
        "engine_volume_liters",
        "fuel",
        "body_color",
        "count_owner",
        "condition",
        "is_new",
    ]

    must_not = client.last_kwargs["query"]["bool"]["must_not"]
    assert {"term": {"_id": "es-doc-123"}} in must_not
    assert {"term": {"is_duplicate": True}} in must_not
    assert {"exists": {"field": "successor_id"}} in must_not

    filters = client.last_kwargs["query"]["bool"]["filter"]
    assert {"term": {"site_name.keyword": "avito"}} in filters
    assert {"term": {"region.keyword": "Moscow region"}} in filters
    assert {"term": {"seller_type": "private"}} in filters
    assert {"term": {"name.keyword": "Seller"}} in filters
    assert {"term": {"brand.keyword": "Toyota"}} in filters
    assert {"term": {"model.keyword": "Camry"}} in filters
    assert {
        "range": {
            "offer_end": {
                "gte": "2026-01-05T10:00:00",
                "lte": "2026-01-12T10:00:00",
            }
        }
    } in filters
    assert {"range": {"latest_price": {"gte": 900000.0, "lte": 1100000.0}}} in filters
    assert {"range": {"mileage": {"gte": 95000.0, "lte": 105000.0}}} in filters
    assert {"term": {"generation.keyword": "XV70"}} in filters
    assert {"term": {"build_year": 2020}} in filters
    assert not any("place.keyword" in item.get("term", {}) for item in filters)
    assert not any("modification.keyword" in item.get("term", {}) for item in filters)
    assert not any("complectation.keyword" in item.get("term", {}) for item in filters)
    assert not any("engine_power" in item.get("term", {}) for item in filters)
    assert not any("engine_volume_liters" in item.get("term", {}) for item in filters)
    assert not any("fuel.keyword" in item.get("term", {}) for item in filters)
    assert not any("gear_type.keyword" in item.get("term", {}) for item in filters)
    assert not any("gear_box.keyword" in item.get("term", {}) for item in filters)
    assert not any("steering_position.keyword" in item.get("term", {}) for item in filters)
    assert not any("body_type.keyword" in item.get("term", {}) for item in filters)
    assert not any("body_color.keyword" in item.get("term", {}) for item in filters)
    assert not any("doors_num" in item.get("term", {}) for item in filters)
    assert not any("count_owner" in item.get("term", {}) for item in filters)
    assert not any("condition.keyword" in item.get("term", {}) for item in filters)
    assert not any("is_new" in item.get("term", {}) for item in filters)


def test_duplicate_matcher_skips_missing_optional_filters_and_accepts_valid_hit() -> None:
    candidate = _build_candidate(
        place=None,
        mileage=None,
        generation=None,
        modification=None,
        complectation=None,
        build_year=None,
        vin=None,
        engine_power=None,
        engine_volume_liters=None,
        fuel=None,
        gear_type=None,
        gear_box=None,
        steering_position=None,
        body_type=None,
        body_color=None,
        doors_num=None,
        count_owner=None,
        condition=None,
        region=None,
    )
    client = FakeSearchClient(
        {
            "hits": {
                "hits": [
                    {"_id": "valid-hit", "_source": {"offer_end": "2026-01-09T10:00:00", "latest_price": 1_000_000.0}}
                ]
            }
        }
    )
    matcher = DuplicateMatcher(client=client, index_name="processed-carads")

    duplicate_id, score = asyncio.run(
        matcher.find_best_duplicate(candidate, candidate_id="es-doc-456")
    )

    assert duplicate_id == "valid-hit"
    assert score == pytest.approx(0.88)
    assert client.last_kwargs is not None

    must_not = client.last_kwargs["query"]["bool"]["must_not"]
    assert {"term": {"_id": "es-doc-456"}} in must_not
    assert {"term": {"is_duplicate": True}} in must_not
    assert {"exists": {"field": "successor_id"}} in must_not
    assert len(must_not) == 3

    filters = client.last_kwargs["query"]["bool"]["filter"]
    assert {"term": {"site_name.keyword": "avito"}} in filters
    assert {"term": {"seller_type": "private"}} in filters
    assert {"term": {"brand.keyword": "Toyota"}} in filters
    assert {"term": {"model.keyword": "Camry"}} in filters
    assert {"range": {"latest_price": {"gte": 900000.0, "lte": 1100000.0}}} in filters
    assert not any("mileage" in item.get("range", {}) for item in filters)
    assert not any("region.keyword" in item.get("term", {}) for item in filters)
    assert not any("is_new" in item.get("term", {}) for item in filters)


def test_duplicate_matcher_requires_offer_end_within_five_days() -> None:
    candidate = _build_candidate()
    client = FakeSearchClient(
        {
            "hits": {
                "hits": [
                    {
                        "_id": "missing-offer-end",
                        "_source": {
                            "latest_price": 1_000_000.0,
                        },
                    },
                    {
                        "_id": "valid-hit",
                        "_source": {
                            "offer_end": "2026-01-09T10:00:00",
                        },
                    },
                ]
            }
        }
    )
    matcher = DuplicateMatcher(client=client, index_name="processed-carads")

    duplicate_id, score = asyncio.run(matcher.find_best_duplicate(candidate))

    assert duplicate_id == "valid-hit"
    assert score == pytest.approx(0.8)
