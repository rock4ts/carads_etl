from __future__ import annotations

import pytest

from app.services.ingestion_service.core.config import IngestionServiceSettings
from tests.unit.services.ingestion.support.builders import LOAD_TILL, T0, T1, T2, T3, build_settings


@pytest.fixture
def app_settings() -> IngestionServiceSettings:
    return build_settings()


@pytest.fixture
def timestamps() -> dict[str, object]:
    return {
        "T0": T0,
        "T1": T1,
        "T2": T2,
        "T3": T3,
        "LOAD_TILL": LOAD_TILL,
    }
