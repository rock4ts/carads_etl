"""Unit tests for raw-to-processed ad mapping."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.services.processing_service.mapper import map_raw_to_processed
from app.schemas.raw import RawAd

INGESTED_AT = datetime(2026, 1, 2, 3, 4, 5)
FIXTURES_DIR = Path(__file__).resolve().parents[3] / "fixtures" / "raw_ads"


def _load_raw_ad(source: str, fixture_name: str) -> RawAd:
    payload = json.loads((FIXTURES_DIR / fixture_name).read_text(encoding="utf-8"))
    return RawAd(source=source, ingested_at=INGESTED_AT, payload=payload)


@pytest.mark.parametrize(
    ("source", "fixture_name", "expected"),
    [
        (
            "drom",
            "drom_ad_example.json",
            {
                "original_id": "897218786",
                "parapi_unique_id": 145808463,
                "url": "https://auto.drom.ru/ekaterinburg/lada/granta_cross/897218786.html",
                "site_name": "drom",
                "seller_type": "company",
                "name": "ИЮЛЬ Лада (Екатеринбург)",
                "parapi_user_ident": "tsac_iyul",
                "last_checked": datetime(2026, 1, 1, 0, 0, 2),
                "offer_start": datetime(2025, 12, 19, 0, 0, 0),
                "initial_price": 1195000.0,
                "latest_price": 1195000.0,
                "is_new": True,
                "brand": "Лада",
                "model": "Гранта Кросс",
                "generation": "1 поколение",
                "modification": None,
                "complectation": None,
                "build_year": 2025,
                "vin": "XTA**************",
                "engine_power": 90.0,
                "engine_volume_liters": 1.6,
                "fuel": "бензин",
                "gear_type": "передний",
                "gear_box": "механика",
                "steering_position": "левый",
                "body_type": "универсал",
                "body_color": "красный",
                "doors_num": None,
                "count_owner": None,
                "condition": "не битый",
                "mileage": 0,
                "mileage_units": "km",
                "views_total": 7,
                "phone": None,
                "phone_protection": False,
                "place": "Екатеринбург",
                "region": "Свердловская область",
                "location": (56.838011, 60.597465),
                "offer_end": datetime(2026, 1, 1, 0, 0, 2),
                "images_count": 1,
                "first_image": "https://s11.auto.drom.ru/photo/v2/KTbWfpIm1UZ4BdD47_C88erXBwBbL52yvvjOLk5LbLWYOc8UCx-vtqL5ewV01AYKaI1QHPQs5lbtl5EV/gen1200.jpg",
                "first_transaction": {
                    "datetime": datetime(2025, 12, 19, 15, 4, 40),
                    "price": 1195000.0,
                    "views": 0,
                    "event_type": "top",
                },
            },
        ),
        (
            "avito",
            "avito_ad_example.json",
            {
                "original_id": "7576886901",
                "parapi_unique_id": 140633231,
                "url": "https://www.avito.ru/navoloki/avtomobili/toyota_hiace_2.4_mt_1999_585_000_km_7576886901",
                "site_name": "avito",
                "seller_type": "private",
                "name": "Пользователь",
                "parapi_user_ident": "651c7488fb3e99549fbeac75c3da0aafd18dcb165817dfd87a",
                "last_checked": datetime(2026, 1, 1, 0, 1, 2),
                "offer_start": datetime(2025, 8, 17, 21, 58, 5),
                "initial_price": 1300000.0,
                "latest_price": 1300000.0,
                "is_new": False,
                "brand": "Toyota",
                "model": "Hiace",
                "generation": "H100 (1989—2004)",
                "modification": "2.4 D MT (94 л.с.)",
                "complectation": "Базовая",
                "build_year": 1999,
                "vin": "JT12*************",
                "engine_power": 94.0,
                "engine_volume_liters": 2.4,
                "fuel": "дизель",
                "gear_type": "задний",
                "gear_box": "механика",
                "steering_position": "левый",
                "body_type": "минивэн",
                "body_color": "серебряный",
                "doors_num": None,
                "count_owner": 4,
                "condition": "не битый",
                "mileage": 585000,
                "mileage_units": "km",
                "views_total": 2103,
                "phone": None,
                "phone_protection": True,
                "place": "Наволоки",
                "region": "Ивановская область",
                "location": (57.470588, 41.957765),
                "offer_end": datetime(2026, 1, 1, 0, 1, 2),
                "images_count": 18,
                "first_image": "https://00.img.avito.st/image/1/1.hD96Zba4KNY81v7YfmfuKBbEKtDExNrEGMoq1MrMINzM.XwOsNCeoYWJfhO75ajhoGOzFhow9teruhfhBmOeWnVs?cqp=2.4e3T8kOmeNvDtl_4YZMTzlQLVn8SpaRJdwQxsmBG8hRDgqTzBbytY1M8t75RoOvSDrIHrfs74BDid1qgqQqVxu1_",
                "first_transaction": {
                    "datetime": datetime(2025, 8, 18, 6, 42, 55),
                    "price": 1300000.0,
                    "views": 0,
                    "event_type": "top",
                },
            },
        ),
        (
            "auto",
            "auto_ad_example.json",
            {
                "original_id": "1128919792",
                "parapi_unique_id": 139463051,
                "url": "https://auto.ru/cars/used/sale/kia/cerato/1128919792-74d0695b/",
                "site_name": "auto",
                "seller_type": "private",
                "name": "Сергей Розанов",
                "parapi_user_ident": "pkHLmsGhz_UflqKlY8P3U0iYuZZocFZEhNsqqp51deQ",
                "last_checked": datetime(2026, 1, 1, 0, 0, 20),
                "offer_start": datetime(2025, 7, 21, 11, 26, 3),
                "initial_price": 1530000.0,
                "latest_price": 1530000.0,
                "is_new": False,
                "brand": "KIA",
                "model": "Cerato",
                "generation": "III Рестайлинг",
                "modification": "2.0 AT (150 л.с.)",
                "complectation": "Premium",
                "build_year": 2017,
                "vin": "XWE**************",
                "engine_power": 150.0,
                "engine_volume_liters": 2.0,
                "fuel": "бензин",
                "gear_type": "передний",
                "gear_box": "автоматическая",
                "steering_position": "левый",
                "body_type": "седан",
                "body_color": "чёрный",
                "doors_num": "4",
                "count_owner": 2,
                "condition": "не битый",
                "mileage": 117000,
                "mileage_units": "km",
                "views_total": 366,
                "phone": None,
                "phone_protection": True,
                "place": "Кострома",
                "region": "Костромская область",
                "location": (57.767961, 40.926858),
                "offer_end": datetime(2026, 1, 1, 0, 0, 20),
                "images_count": 11,
                "first_image": "https://avatars.mds.yandex.net/get-autoru-vos/5177156/efdbf2494d33e79b9a056070b7cad669/832x624",
                "first_transaction": {
                    "datetime": datetime(2025, 7, 21, 11, 30, 28),
                    "price": 1530000.0,
                    "views": 0,
                    "event_type": "top",
                },
            },
        ),
    ],
)
def test_example_ads_are_mapped_to_processed_model(source: str, fixture_name: str, expected: dict[str, object]) -> None:
    raw_ad = _load_raw_ad(source, fixture_name)

    before_mapping = datetime.now(timezone.utc)
    mapped = map_raw_to_processed(raw_ad)
    after_mapping = datetime.now(timezone.utc)

    assert mapped.original_id == expected["original_id"]
    assert mapped.parapi_unique_id == expected["parapi_unique_id"]
    assert mapped.url == expected["url"]
    assert mapped.site_name == expected["site_name"]
    assert mapped.seller_type == expected["seller_type"]
    assert mapped.name == expected["name"]
    assert mapped.parapi_user_ident == expected["parapi_user_ident"]
    assert mapped.last_checked == expected["last_checked"]
    assert before_mapping <= mapped.parsed_at <= after_mapping
    assert mapped.offer_start == expected["offer_start"]
    assert len(mapped.trans_history) == 1
    assert mapped.trans_history[0].datetime == expected["first_transaction"]["datetime"]
    assert mapped.trans_history[0].price == expected["first_transaction"]["price"]
    assert mapped.trans_history[0].views == expected["first_transaction"]["views"]
    assert mapped.trans_history[0].event_type == expected["first_transaction"]["event_type"]
    assert mapped.initial_price == expected["initial_price"]
    assert mapped.latest_price == expected["latest_price"]
    assert mapped.is_new == expected["is_new"]
    assert mapped.brand == expected["brand"]
    assert mapped.model == expected["model"]
    assert mapped.generation == expected["generation"]
    assert mapped.modification == expected["modification"]
    assert mapped.complectation == expected["complectation"]
    assert mapped.build_year == expected["build_year"]
    assert mapped.vin == expected["vin"]
    assert mapped.engine_power == expected["engine_power"]
    assert mapped.engine_volume_liters == expected["engine_volume_liters"]
    assert mapped.fuel == expected["fuel"]
    assert mapped.gear_type == expected["gear_type"]
    assert mapped.gear_box == expected["gear_box"]
    assert mapped.steering_position == expected["steering_position"]
    assert mapped.body_type == expected["body_type"]
    assert mapped.body_color == expected["body_color"]
    assert mapped.doors_num == expected["doors_num"]
    assert mapped.count_owner == expected["count_owner"]
    assert mapped.condition == expected["condition"]
    assert mapped.mileage == expected["mileage"]
    assert mapped.mileage_units == expected["mileage_units"]
    assert mapped.views_total == expected["views_total"]
    assert mapped.phone == expected["phone"]
    assert mapped.phone_protection == expected["phone_protection"]
    assert mapped.place == expected["place"]
    assert mapped.region == expected["region"]
    assert mapped.location is not None
    assert (mapped.location.lat, mapped.location.lon) == expected["location"]
    assert mapped.predecessor_id is None
    assert mapped.successor_id is None
    assert mapped.offer_end == expected["offer_end"]
    assert len(mapped.images) == expected["images_count"]
    assert str(mapped.images[0]) == expected["first_image"]


def test_initial_price_falls_back_to_payload_price_when_history_is_empty() -> None:
    raw_ad = RawAd(
        source="auto",
        ingested_at=INGESTED_AT,
        payload={
            "id": "no-history-id",
            "unique_id": "12345",
            "url": "https://auto.example/no-history-id",
            "name": "No History Ad",
            "parsed": "2026-01-01 00:00:00",
            "added": "2025-12-31 00:00:00",
            "checked": "2026-01-01 00:10:00",
            "actual": "1",
            "price": "777777.50",
            "advert_info": [],
            "advert_image": [],
            "mark": "Brand",
            "model": "Model",
        },
    )

    mapped = map_raw_to_processed(raw_ad)

    assert mapped.trans_history == []
    assert mapped.initial_price == 777777.5
    assert mapped.latest_price == 777777.5
