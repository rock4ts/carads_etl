# Backend Architecture (Strict)

These rules are **mandatory** for all code generation and edits in this project. Prefer clean architecture over shortcuts. If existing code violates these rules, suggest or perform refactoring toward compliance.

---

## 1. Configuration (Pydantic Settings)

- **Single source of truth:** All environment-driven configuration MUST use Pydantic `BaseSettings` (or project-approved equivalent such as `Settings` from `pydantic-settings`).
- **Forbidden:** `os.getenv`, `os.environ[...]`, or reading process environment for app config **outside** the designated config module.
- **Location:** Application settings MUST be defined in `app/core/config.py` (one module; split into submodules only if the project already uses that pattern and imports re-export from `app/core/config.py`).
- **Singleton:** Expose exactly one shared instance: `settings = Settings()` (or `get_settings()` if lazy init is required, but still a single authoritative settings object).
- **`.env` support:** Settings MUST load from `.env` when present (via Pydantic/pydantic-settings conventions).
- **Typing:** Every setting field MUST be explicitly typed. No untyped or `Any`-only configuration bags without justification aligned with project standards.

---

## 2. Repository Pattern

- **All database and storage reads/writes** (PostgreSQL, MongoDB, Elasticsearch, ClickHouse, etc.) MUST go through **repository** classes—not through raw clients, ORM session usage, or drivers in services.
- **One repository per aggregate/entity** (e.g. `CarRepository`, `AdRepository`). Name and scope repositories by domain entity, not by arbitrary query groupings.
- **Constructor injection:** Repositories MUST accept a session, client, or connection object via `__init__` (or explicit factory parameters). No global connection pools or implicit singletons inside repositories.
- **No business logic:** Repositories MAY map rows/documents to DTOs/domain types and perform persistence-specific queries. They MUST NOT encode business rules, validation of business invariants beyond persistence constraints, or orchestration.
- **No commits:** Repositories MUST NOT call `commit`, `rollback`, or equivalent transaction boundaries. Transaction control belongs exclusively to Unit of Work (see §3).
- **Abstract interfaces:** Define abstract base repository interfaces (e.g. ABC or `Protocol`) in a dedicated place (e.g. `app/repositories/abstract/` or `app/domain/repositories/`). Concrete implementations live beside or under storage-specific packages (e.g. `app/repositories/postgres/`).

---

## 3. Unit of Work Pattern

- **All write operations** (insert, update, delete, bulk writes that mutate state) MUST go through a **Unit of Work** (UoW). Services and handlers MUST NOT open ad-hoc transactions on clients/sessions.
- **UoW responsibilities:**
  - Own transaction lifecycle (begin, commit, rollback) per storage/backend that supports transactions.
  - Construct or expose repository instances bound to the current session/client scope.
- **Required API:** Each UoW implementation MUST provide:
  - Context manager protocol: `__enter__` / `__exit__` (or async equivalents if the stack is async-first).
  - `commit()` and `rollback()` where applicable to the underlying storage.
- **No commits outside UoW:** Application code outside the UoW implementation MUST NOT call `commit`/`rollback` on sessions or clients except inside UoW internals.
- **Service usage:** Services MUST use the pattern `with uow:` (or `async with uow:`) for scoped work that performs writes, and obtain repositories from the UoW—not by constructing repositories with a global session.

---

## 4. Service Layer

- **Business logic** (rules, orchestration, use cases) MUST live **only** in the **service** layer (`app/services/` or equivalent project layout).
- **Dependencies:** Services accept UoW (and other ports) via constructor or explicit method parameters—no hidden globals.
- **Forbidden in services:**
  - Direct use of DB drivers, ORM sessions, or collection handles for querying/mutating data.
  - Direct import of ORM/document models **for querying** or persistence. Services depend on repositories, DTOs, and domain types—not ORM models as query surfaces.
- **Determinism and testing:** Services MUST remain deterministic where possible; external effects go through injected abstractions. Design services for unit testing with mocked repositories/UoW.

---

## 5. Multi-Storage Support

The architecture MUST support, as first-class concerns:

| Storage        | Typical role              |
|----------------|---------------------------|
| PostgreSQL     | Transactional data        |
| MongoDB        | Raw ingestion             |
| Elasticsearch  | Search                    |
| ClickHouse     | Analytics                 |

- **Primary persistence (PostgreSQL):** PostgreSQL is the **main persistence layer** and **system of record** for canonical domain state (entities, relationships, and transactional business data). Default new durable writes and authoritative reads for application features to PostgreSQL-backed repositories unless this document explicitly assigns another store to that concern (e.g. MongoDB for raw ingestion only, Elasticsearch for search views, ClickHouse for analytics).
- **Per-storage repositories:** Each storage has its own repository implementation for the same conceptual entity when the entity is persisted there. Do not merge storage concerns into one “god” repository.
- **No cross-storage logic in repositories:** Repositories MUST NOT coordinate reads/writes across PostgreSQL, MongoDB, Elasticsearch, and ClickHouse. Cross-store workflows belong in **services** or the **ETL/processing** layer.
- **Transformation:** Mapping between raw shapes, domain models, index documents, and analytics rows happens in **services** or **ETL/processing** modules—not inside repositories (beyond trivial row/document field mapping).

---

## 6. ETL / Processing Rules

- **Raw before processed:** Ingested raw payloads MUST be stored (e.g. in MongoDB) **before** downstream processing that produces normalized or derived data.
- **Idempotent processing:** Processing steps MUST be safe to retry: use stable ids, upserts, or explicit idempotency keys as required by the pipeline.
- **Deduplication:** Duplicate detection and merge logic MUST live in a **dedicated service** (or dedicated module with service-level semantics), not scattered inside parsers or repositories.
- **No parser → Elasticsearch direct writes:** Parsers and scrapers MUST NOT write directly to Elasticsearch. Ingestion flows through the prescribed raw store and processing/dedup path before search indexing.

---

## 7. Anti-Patterns (Forbidden)

Do **not**:

- Access databases or index clients **directly from services** (bypassing repositories/UoW).
- Use **global sessions** or global ORM `Session` / Mongo client singletons consumed by feature code.
- Perform **hidden commits** (e.g. `commit` inside repository helpers, middleware, or model hooks that services don’t control).
- **Mix schemas:** ORM models, API request/response models (e.g. Pydantic), and internal DTOs MUST NOT be conflated. Convert at boundaries explicitly.
- Place **business logic inside repositories**.
- Read **environment variables outside** `app/core/config.py` (and its explicitly documented re-exports if any).
- Treat **MongoDB, Elasticsearch, or ClickHouse as the primary store for canonical domain data** when that data should live in PostgreSQL per §5.

---

## 8. Code Generation Behavior

When generating or modifying code in this repository, you MUST:

1. Apply **all rules** in this document.
2. Prefer **clean architecture** and explicit boundaries over shortcuts.
3. **Flag violations** in user-provided snippets and propose concrete refactors (files, types, patterns).
4. **Structure files** consistently:
   - `app/core/` — config, cross-cutting primitives.
   - `app/repositories/` — abstract interfaces and concrete per-storage implementations.
   - `app/uow/` (or `app/unit_of_work/`) — UoW implementations.
   - `app/services/` — business logic and orchestration.
   - ETL/processing pipelines in their designated package (e.g. `app/etl/`, `app/processing/`, or existing project paths).

If a rule conflicts with an older project rule, **this document takes precedence** for backend architecture unless the user explicitly overrides for a specific change.
