"""Mapping helpers for converting raw parser payloads into processed models."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import HttpUrl, TypeAdapter

from app.shared.schemas.processed import CaradDocData, CaradTransaction, Location
from app.shared.schemas.raw import RawAd


def _none_if_blank(value: Any) -> Any:
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def _to_float(value: Any) -> float | None:
    value = _none_if_blank(value)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    value = _none_if_blank(value)
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool | None:
    value = _none_if_blank(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    return None


def _parse_datetime(value: Any) -> datetime | None:
    value = _none_if_blank(value)
    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def _get_params_map(payload: dict[str, Any]) -> dict[str, Any]:
    params_map: dict[str, Any] = {}
    for item in payload.get("params") or []:
        if not isinstance(item, dict):
            continue
        name = _none_if_blank(item.get("name"))
        if not isinstance(name, str):
            continue
        params_map[name.strip().lower()] = _none_if_blank(item.get("value"))
    return params_map


def _normalize_image_url(url: Any) -> str | None:
    url = _none_if_blank(url)
    if not isinstance(url, str):
        return None
    if url.startswith("//"):
        return f"https:{url}"
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return None


def _build_trans_history(payload: dict[str, Any], fallback_dt: datetime) -> list[CaradTransaction]:
    history: list[CaradTransaction] = []
    for item in payload.get("advert_info") or []:
        if not isinstance(item, dict):
            continue

        price = _to_float(item.get("price"))
        if price is None:
            continue

        history_dt = _parse_datetime(item.get("datetime")) or fallback_dt
        history.append(
            CaradTransaction(
                datetime=history_dt,
                price=price,
                views=_to_int(item.get("views")),
                event_type=_none_if_blank(item.get("type")),
            )
        )

    history.sort(key=lambda item: item.datetime)
    return history


def _build_location(payload: dict[str, Any]) -> Location | None:
    lat = _to_float(payload.get("city_latitude"))
    lon = _to_float(payload.get("city_longitude"))
    if lat is None and lon is None:
        return None
    return Location(lat=lat, lon=lon)


def _build_images(payload: dict[str, Any]) -> list[HttpUrl]:
    images: list[str] = []
    for item in payload.get("advert_image") or []:
        if not isinstance(item, dict):
            continue
        image_url = _normalize_image_url(item.get("image"))
        if image_url:
            images.append(image_url)
    return TypeAdapter(list[HttpUrl]).validate_python(images)


def _infer_site_name(source: str, site_id: Any) -> str:
    normalized_source = source.strip().lower()
    if normalized_source in {"avito"}:
        return "avito"
    if normalized_source in {"drom", "auto.drom", "auto_drom"}:
        return "drom"
    if normalized_source in {"auto", "auto.ru", "auto_ru", "autoru"}:
        return "auto"

    site_name_by_id = {
        "2": "avito",
        "3": "auto",
        "4": "drom",
    }
    return site_name_by_id.get(str(site_id), normalized_source or "unknown")


def _infer_seller_type(payload: dict[str, Any]) -> str:
    company = _to_bool(payload.get("company"))
    if company is True:
        return "company"
    if company is False:
        return "private"
    return "unknown"


def map_raw_to_processed(raw: RawAd) -> CaradDocData:
    payload = raw.payload or {}
    params = _get_params_map(payload)

    last_checked = _parse_datetime(payload.get("checked")) or raw.ingested_at
    parsed_timestamp = _parse_datetime(payload.get("parsed")) or last_checked
    # `added` looks closest to when the listing first appeared on the source site.
    offer_start = _parse_datetime(payload.get("added")) or parsed_timestamp
    trans_history = _build_trans_history(payload, fallback_dt=parsed_timestamp)

    payload_price = _to_float(payload.get("price"))
    initial_price = trans_history[0].price if trans_history else (payload_price or 0.0)
    latest_price = trans_history[-1].price if trans_history else initial_price

    actual_flag = _to_bool(payload.get("actual"))
    # `actual=0` means the ad is no longer active, so we use the last check time as the end marker.
    offer_end = last_checked if actual_flag is False else None

    return CaradDocData(
        original_id=_none_if_blank(payload.get("id")),
        parapi_unique_id=_to_int(payload.get("unique_id")) or 0,
        url=_none_if_blank(payload.get("url")) or "",
        site_name=_infer_site_name(raw.source, payload.get("site_id")),
        seller_type=_infer_seller_type(payload),
        name=_none_if_blank(payload.get("name")) or "",
        parapi_user_ident=_none_if_blank(payload.get("user_ident")),
        last_checked=last_checked,
        parsed_at=datetime.now(),
        offer_start=offer_start,
        trans_history=trans_history,
        initial_price=initial_price,
        latest_price=latest_price,
        is_new=_to_bool(payload.get("is_new")) or False,
        brand=_none_if_blank(payload.get("mark")) or "",
        parapi_brand_id=None,
        model=_none_if_blank(payload.get("model")) or "",
        parapi_model_id=_none_if_blank(payload.get("model_id")),
        generation=_none_if_blank(payload.get("generation")),
        parapi_generation_id=_none_if_blank(payload.get("generation_id")),
        modification=_none_if_blank(payload.get("modification")),
        parapi_modification_id=_to_int(payload.get("modification_id")),
        complectation=_none_if_blank(payload.get("complectation")),
        parapi_complectation_id=_none_if_blank(payload.get("complectation_id")),
        build_year=_to_int(payload.get("year")),
        vin=_none_if_blank(payload.get("vin")),
        engine_power=_to_float(payload.get("hourse")),
        engine_volume_liters=_to_float(payload.get("engine_vol")),
        fuel=_none_if_blank(params.get("тип двигателя")),
        gear_type=_none_if_blank(params.get("привод")),
        gear_box=_none_if_blank(params.get("коробка передач")),
        steering_position=_none_if_blank(params.get("руль")),
        body_type=_none_if_blank(params.get("тип кузова")),
        body_color=_none_if_blank(params.get("цвет")),
        doors_num=_none_if_blank(params.get("число дверей")),
        count_owner=_to_int(payload.get("owners_num")),
        condition=_none_if_blank(params.get("состояние")),
        mileage=_to_int(payload.get("run")),
        mileage_units=_none_if_blank(payload.get("run_type")) or "km",
        views_total=_to_int(payload.get("actual_views")),
        phone=_none_if_blank(payload.get("phone")),
        phone_protection=_to_bool(payload.get("phone_protection")),
        place=_none_if_blank(payload.get("city")),
        region=_none_if_blank(payload.get("region")),
        location=_build_location(payload),
        predecessor_id=None,
        successor_id=None,
        offer_end=offer_end,
        images=_build_images(payload),
    )
