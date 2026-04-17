"""Ingestion service entrypoint."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from urllib import error, parse, request

from app.services.ingestion_service.core.config import IngestionServiceSettings, settings
from app.services.processing_service.mapper import map_raw_to_processed
from app.shared.database.session import build_postgres_session_factory
from app.shared.clients.processed_storage import save_processed_docs
from app.shared.clients.raw_storage import save_raw_ads
from app.shared.schemas.raw import RawAd
from app.uow.ingestion_state_uow import IngestionStateUnitOfWork, SqlAlchemyIngestionStateUnitOfWork

logger = logging.getLogger(__name__)
PARSER_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
PARSER_REQUEST_TIMEOUT_SECONDS = 30

StateUowFactory = Callable[[], IngestionStateUnitOfWork]


def _configure_logging() -> None:
    level = getattr(logging, settings.log_level.strip().upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _format_parser_datetime(value: datetime) -> str:
    return _to_naive_utc(value).strftime(PARSER_DATETIME_FORMAT)


def _to_naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_parser_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return _to_naive_utc(value)
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value.strip(), PARSER_DATETIME_FORMAT)
    except ValueError:
        return None


def _parse_ads_batch(payload: object) -> list[dict[str, object]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        ads = payload.get("ads")
        if isinstance(ads, list):
            return [item for item in ads if isinstance(item, dict)]
    raise RuntimeError("Parser response must be either a list or an object with an 'ads' list")


def _build_request_url(
    *,
    parser_api_url: str,
    parser_api_key: str,
    site_name: str,
    current_from: datetime,
) -> str:
    normalized_url = parser_api_url.rstrip("?&")
    query_string = parse.urlencode(
        {
            "key": parser_api_key,
            "site": site_name,
            "from_datetime": _format_parser_datetime(current_from),
        }
    )
    separator = "&" if "?" in normalized_url else "?"
    return f"{normalized_url}{separator}{query_string}"


def _fetch_parser_ads_sync(
    *,
    parser_api_url: str,
    parser_api_key: str,
    site_name: str,
    current_from: datetime,
) -> list[dict[str, object]]:
    url = _build_request_url(
        parser_api_url=parser_api_url,
        parser_api_key=parser_api_key,
        site_name=site_name,
        current_from=current_from,
    )
    req = request.Request(url=url, method="GET")
    try:
        with request.urlopen(req, timeout=PARSER_REQUEST_TIMEOUT_SECONDS) as response:
            body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Parser HTTP {exc.code} for {site_name} at {url}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Failed to reach parser for {site_name} at {url}: {exc}") from exc

    try:
        parsed_payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON response from parser for site {site_name}") from exc

    return _parse_ads_batch(parsed_payload)


async def fetch_parser_ads(
    *,
    parser_api_url: str,
    parser_api_key: str,
    site_name: str,
    current_from: datetime,
) -> list[dict[str, object]]:
    return await asyncio.to_thread(
        _fetch_parser_ads_sync,
        parser_api_url=parser_api_url,
        parser_api_key=parser_api_key,
        site_name=site_name,
        current_from=current_from,
    )


def _max_checked_timestamp(ads_batch: list[dict[str, object]]) -> datetime:
    max_checked: datetime | None = None
    for ad_payload in ads_batch:
        checked_value = _parse_parser_datetime(ad_payload.get("checked"))
        if checked_value is None:
            raise RuntimeError("Ad payload does not include a valid 'checked' timestamp")
        if max_checked is None or checked_value > max_checked:
            max_checked = checked_value
    if max_checked is None:
        raise RuntimeError("Unable to calculate cursor because batch does not contain ads")
    return max_checked


async def _persist_batch(
    *,
    site_name: str,
    request_params: dict[str, str],
    ads_batch: list[dict[str, object]],
    app_settings: IngestionServiceSettings,
) -> None:
    ingested_at = datetime.now(timezone.utc)
    raw_ads: list[RawAd] = []
    for payload in ads_batch:
        raw = RawAd(
            source=site_name,
            ingested_at=ingested_at,
            request_params=request_params,
            payload=payload,
        )
        raw_ads.append(raw)

    await save_raw_ads(
        raw_ads,
        mongo_uri=app_settings.mongo_uri,
        mongo_db=app_settings.mongo_db,
        raw_collection_name=app_settings.raw_collection_name,
    )
    processed_docs = [map_raw_to_processed(raw) for raw in raw_ads]
    await save_processed_docs(
        processed_docs,
        elasticsearch_url=app_settings.elasticsearch_url,
        elasticsearch_api_key=app_settings.elasticsearch_api_key,
        elasticsearch_username=app_settings.elasticsearch_username,
        elasticsearch_password=app_settings.elasticsearch_password,
    )


async def _process_site(
    *,
    site_name: str,
    state_uow_factory: StateUowFactory,
    load_till: datetime,
    app_settings: IngestionServiceSettings,
) -> None:
    with state_uow_factory() as uow:
        current_from = uow.ingestion_states.get_upload_timestamp(site_name)
    if current_from is None:
        logger.info("Ingestion [%s]: skipped, missing UploadTimestamp", site_name)
        return
    current_from = _to_naive_utc(current_from)
    starting_from = current_from
    processed_total = 0
    logger.info(
        "Ingestion [%s]: start from=%s load_till=%s",
        site_name,
        _format_parser_datetime(starting_from),
        _format_parser_datetime(load_till),
    )

    while current_from < load_till:
        request_from = current_from
        request_params = {
            "site": site_name,
            "from_datetime": _format_parser_datetime(request_from),
        }
        ads_batch = await fetch_parser_ads(
            parser_api_url=app_settings.parser_api_url,
            parser_api_key=app_settings.parser_api_key,
            site_name=site_name,
            current_from=request_from,
        )
        if not ads_batch:
            logger.info(
                "Ingestion [%s]: empty batch at cursor=%s processed=%s",
                site_name,
                request_params["from_datetime"],
                processed_total,
            )
            break

        await _persist_batch(
            site_name=site_name,
            request_params=request_params,
            ads_batch=ads_batch,
            app_settings=app_settings,
        )
        new_current_from = _max_checked_timestamp(ads_batch)
        new_current_from = _to_naive_utc(new_current_from)
        if new_current_from <= current_from:
            logger.warning(
                "Ingestion [%s]: non-advancing cursor current=%s next=%s; stopping",
                site_name,
                _format_parser_datetime(current_from),
                _format_parser_datetime(new_current_from),
            )
            break
        current_from = new_current_from
        with state_uow_factory() as uow:
            uow.ingestion_states.upsert_upload_timestamp(site_name, current_from)
            uow.commit()
        processed_total += len(ads_batch)
        logger.info(
            "Ingestion [%s]: from=%s current=%s batch=%s processed=%s",
            site_name,
            request_params["from_datetime"],
            _format_parser_datetime(current_from),
            len(ads_batch),
            processed_total,
        )

    logger.info(
        "Ingestion [%s]: finished start=%s current=%s processed=%s",
        site_name,
        _format_parser_datetime(starting_from),
        _format_parser_datetime(current_from),
        processed_total,
    )


async def _run() -> None:
    session_factory = build_postgres_session_factory(settings.postgres_database_url)

    def _state_uow_factory() -> SqlAlchemyIngestionStateUnitOfWork:
        return SqlAlchemyIngestionStateUnitOfWork(session_factory)

    with _state_uow_factory() as uow:
        sites = list(uow.ingestion_states.list_upload_sites())
    if not sites:
        logger.info("No sites available for ingestion")
        return

    load_till = _to_naive_utc(datetime.now(timezone.utc))
    for site_name in sites:
        try:
            await _process_site(
                site_name=site_name,
                state_uow_factory=_state_uow_factory,
                load_till=load_till,
                app_settings=settings,
            )
        except Exception:
            logger.exception("Ingestion [%s]: failed, checkpoint unchanged for current batch", site_name)


def main() -> None:
    _configure_logging()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
