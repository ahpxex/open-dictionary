# Open Dictionary Rewrite Charter

This repository is not a generic "dictionary scripts" project.
It is the rewrite line for a reproducible dictionary production system built
on top of Wiktionary / Wiktextract data.

The system must be designed as a staged data pipeline with explicit schemas,
explicit run metadata, and deterministic handoff points between stages.

## Product Framing

- Source of truth: Wiktionary / Wiktextract data, not Wikidata.
- Canonical unit: one headword equals one entry.
- Entry model: word-centric, not "word plus part-of-speech" as the top-level unit.
- LLM goal: produce structured, Chinese learner-friendly dictionary entries from
  curated source data.
- Human editorial authority: when the pipeline hits ambiguous curation questions,
  the user decides the editorial rule. The agent must not invent permanent
  curation policy without explicit approval.

## Core Workflow

The intended end-state workflow is:

1. Download a Wiktionary / Wiktextract snapshot.
2. Ingest the raw snapshot into PostgreSQL.
3. Build curated tables from raw tables.
4. Run LLM enrichment on curated entries.
5. Export stable distributable artifacts such as JSONL and SQLite.

Expressed as data layers:

- `raw`: source-faithful imported data.
- `curated`: normalized, cleaned, word-centric entries.
- `llm`: structured generated outputs plus generation metadata.
- `exports`: packaged read-only outputs for downstream distribution.
- `meta`: run tracking, versions, prompts, and operational audit data.

No stage may skip over the previous stage's contract.
For example, LLM enrichment must consume curated entries, not raw imported blobs.

## Technical Framework

### 1. Raw Ingestion Layer

Responsibilities:

- Download source snapshots.
- Record source identity, origin URL, timestamps, and hashes.
- Load raw payloads into PostgreSQL with minimal semantic mutation.
- Preserve source payloads well enough to re-run downstream stages.

Requirements:

- Raw ingestion must be idempotent at the run level.
- Every ingestion run must have a `run_id`.
- Every raw row must be traceable to a source snapshot and ingestion run.
- The code must not mix download logic, parsing logic, and curation logic in the
  same stage module.

### 2. Curation Layer

Responsibilities:

- Read from raw PostgreSQL tables only.
- Normalize Wiktionary's fragmented record shape into a canonical word-centric
  entry model.
- Remove technical noise and transform source structure into a stable contract
  for downstream enrichment.
- Produce new tables rather than mutating raw tables in place.

Requirements:

- Curation rules must be explicit and reviewable.
- Editorial ambiguity must be escalated to the user.
- The curation layer must record its input run, rule version, and output run.
- Curation must be restartable and safe to re-run.

Examples of questions that require user decisions:

- Which tags, qualifiers, and metadata are useful vs noise.
- How multiple parts of speech should be represented under one headword.
- Which examples, derivations, pronunciations, etymologies, and related forms
  are mandatory, optional, or discardable.
- How aggressively duplicate or overlapping senses should be merged.

### 3. LLM Enrichment Layer

Responsibilities:

- Consume curated entries only.
- Build prompt inputs from stable curated payloads.
- Generate structured outputs under strict schema validation.
- Persist generation metadata and outputs in PostgreSQL.

Requirements:

- Prompt versions must be explicit and tracked.
- Model name, provider, temperature, and other generation settings must be
  recorded per run.
- Input hash, output payload, failure reason, retry count, and timestamps must
  be recorded.
- Concurrency, retry, rate limiting, and resume behavior are required
  infrastructure, not optional helpers.
- A run must be resumable without duplicating successful rows.

The LLM layer must be auditable. "We called the model and saved the result" is
not enough.

### 4. Export Layer

Responsibilities:

- Produce stable distribution artifacts from curated and/or enriched tables.
- Export to JSONL, SQLite, and other read-only formats as needed.
- Treat exported artifacts as build outputs, not mutable working data.

Requirements:

- Exports must record the exact upstream run ids they were built from.
- Export scripts must never contain business logic that belongs in curation or
  LLM enrichment.

## Mandatory Reproducibility Rules

- Every stage must have a durable `run_id`.
- Every stage must record its upstream dependency run ids.
- Prompt text must be versioned.
- Table schemas must be migrated explicitly, never created implicitly by random
  business logic at runtime.
- Runtime code must not quietly `ALTER TABLE` as a substitute for migrations.
- Raw, curated, and LLM tables must be separable by stage and by run lineage.
- Outputs must be derivable from stored source data and stored configuration.

If a feature makes the pipeline less reproducible, less inspectable, or less
restartable, it is the wrong design.

## Repository Architecture

The rewrite should converge toward a structure similar to:

- `src/open_dictionary/cli.py`
  Entry point only. Parse commands and dispatch to stage runners.
- `src/open_dictionary/config/`
  Typed runtime configuration and environment loading.
- `src/open_dictionary/db/`
  PostgreSQL connections, query helpers, migrations, and run bookkeeping.
- `src/open_dictionary/pipeline/`
  Stage orchestration, run graph handling, and shared execution contracts.
- `src/open_dictionary/wiktionary/`
  Snapshot download, extraction, source parsing, and raw ingestion logic.
- `src/open_dictionary/curation/`
  Canonical entry assembly, filtering, normalization, and user-approved rules.
- `src/open_dictionary/llm/`
  Prompt compilation, schema definitions, generation workers, retries, and
  persistence of results.
- `src/open_dictionary/export/`
  JSONL, SQLite, and future artifact builders.
- `tests/`
  Integration-first tests around stage boundaries and run reproducibility.

The agent may reorganize toward this architecture when rewriting modules.
The agent should not preserve current module boundaries just because they
already exist.

## Coding Rules

- Target Python 3.12+.
- Use `uv` for dependency and command execution in this Python repository.
- Prefer typed dataclasses or Pydantic models for stage contracts.
- Keep parsing, curation, persistence, and orchestration separate.
- Favor explicit schemas over ad hoc dictionaries once data crosses a stage
  boundary.
- Prefer pure transformation functions for canonicalization logic.
- Keep side effects at stage edges.

Do not:

- Mix raw ingestion and curation in one function.
- Mix schema migration logic into ordinary business workflows.
- Call the LLM directly on raw Wiktionary blobs unless explicitly required for a
  temporary experiment.
- Leave prompt text or output schemas implicit inside random modules.
- Keep legacy code only because "it already works a little".

## Database Rules

- PostgreSQL is the system of record for pipeline execution.
- Raw tables are append-oriented and source-faithful.
- Curated tables are derived tables with explicit provenance.
- LLM output tables must store both validated structured output and operational
  metadata.
- Export artifacts are downstream products, not source-of-truth stores.

Suggested operational separation:

- `meta` schema for runs, versions, prompts, and audits.
- `raw` schema for imported source data.
- `curated` schema for normalized entries.
- `llm` schema for generated outputs.
- `export` schema for export bookkeeping.

The exact schema names may change, but stage separation may not disappear.

## Testing Rules

Testing priority is not unit-test vanity coverage.
Testing priority is stage correctness and reproducibility.

Minimum expectations:

- Integration tests for each stage boundary.
- End-to-end tests on a small fixture dataset.
- Validation that repeated runs do not corrupt data or duplicate results.
- Validation that curated output preserves required source traceability.
- Validation that LLM output parsing rejects invalid responses.

Useful test categories:

- ingestion smoke tests
- curation contract tests
- prompt and schema validation tests
- resume and retry behavior tests
- export consistency tests

## Operational Rules

- Use explicit commands for each stage and for full pipeline execution.
- A full pipeline command must still be composed from stage contracts, not from
  hidden side effects.
- Large datasets must be streamed; avoid loading entire dumps into memory.
- Progress reporting must never substitute for durable run metadata.
- Temporary experiments must be clearly isolated from production pipeline code.

## Branching and Rewrite Direction

- The intended long-term mainline for the rewrite is `master`.
- Legacy history may be retained as archive material, but development should
  follow the rewrite architecture above.
- Do not treat old implementation details as architectural constraints.

## Documentation Rules

- Documentation must describe the target architecture and current operational
  contract, not just list ad hoc commands.
- When a curation rule becomes approved by the user, write it down in the
  repository docs instead of keeping it implicit in code comments.
- Prompt versions and canonical entry contracts must be documented alongside the
  implementation.

