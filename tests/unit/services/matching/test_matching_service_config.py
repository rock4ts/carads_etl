from app.services.matching_service.core.config import MatchingServiceSettings


def test_matching_sites_parsing(monkeypatch) -> None:
    monkeypatch.setenv("MATCHING_SITES", "site_a, site_b, ,site_c")

    cfg = MatchingServiceSettings()

    assert cfg.matching_sites == ["site_a", "site_b", "site_c"]


def test_batch_size_is_clamped(monkeypatch) -> None:
    monkeypatch.setenv("MATCHING_BATCH_SIZE", "5000")

    cfg = MatchingServiceSettings()

    assert cfg.matching_batch_size == 1000


def test_database_url_uses_legacy_fallback(monkeypatch) -> None:
    monkeypatch.delenv("POSTGRES_DATABASE_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://example:test@localhost:5432/mydb")

    cfg = MatchingServiceSettings()

    assert cfg.postgres_database_url == "postgresql+psycopg://example:test@localhost:5432/mydb"


def test_telegram_progress_interval_minutes_is_clamped(monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_PROGRESS_INTERVAL_MINUTES", "0")

    cfg = MatchingServiceSettings()

    assert cfg.telegram_progress_interval_minutes == 1

