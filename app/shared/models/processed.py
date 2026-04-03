"""Processed ad schemas used across ETL stages."""

from __future__ import annotations

from datetime import datetime
from typing import List, Union

from pydantic import BaseModel, Field, HttpUrl


class CaradTransaction(BaseModel):
    datetime: datetime
    price: float
    views: int | None = None
    type: str | None = None


class Location(BaseModel):
    lat: float | None = None
    lon: float | None = None


class CaradDocData(BaseModel):
    original_id: Union[int, str, None] = None
    parapi_unique_id: int
    url: str
    site_name: str
    seller_type: str
    name: str
    parapi_user_ident: str | None = None
    last_checked: datetime
    parsed_at: datetime
    offer_start: datetime
    trans_history: List[CaradTransaction] = Field(default_factory=list)
    initial_price: float
    latest_price: float
    is_new: bool  # about car
    brand: str
    parapi_brand_id: str | None = None
    model: str
    parapi_model_id: str | None = None
    generation: str | None = None
    parapi_generation_id: str | None = None
    modification: str | None = None
    parapi_modification_id: int | None = None
    complectation: str | None = None
    parapi_complectation_id: str | None = None
    build_year: int | None = None
    vin: str | None = None
    engine_power: float | None = None
    engine_volume_liters: float | None = None
    fuel: str | None = None
    gear_type: str | None = None
    gear_box: str | None = None
    steering_position: str | None = None
    body_type: str | None = None
    body_color: str | None = None
    doors_num: str | None = None
    count_owner: int | None = None
    condition: str | None = None
    mileage: int | None
    mileage_units: str | None = "km"
    views_total: int | None = None
    phone: str | None = None
    phone_protection: bool | None = None
    place: str | None = None
    region: str | None = None
    location: Location | None = None
    is_duplicate: bool = False  # about ad
    successor_id: str | None = None
    offer_end: datetime | None = None
    images: List[HttpUrl] = Field(default_factory=list)
