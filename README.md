# Open English Dictionary

## Rebuilding process WIP

## Currently, this project is being rebuilt.

New features are:

- Explicit rewrite foundation with tracked pipeline runs
- PostgreSQL-first raw ingestion for Wiktionary / Wiktextract snapshots
- Streamlined raw ingestion and curation rewrite
- Wiktionary grounding
  - Enormous words data across multiple languages
- New distribution format will be: jsonl, sqlite and more are to be determined
- Options are available to select specific category of words

**Behold and stay tuned!**

## Prerequisites

- Install project dependencies: `uv sync`
- Configure a `.env` file with `DATABASE_URL`
- Ensure a PostgreSQL database is reachable via that URL

## Initialize The Rewrite Foundation

Apply the rewrite schemas and metadata tables:

```bash
uv run open-dictionary db-init
```

This creates the initial `meta`, `raw`, `curated`, `llm`, and `export` schemas.

## Run The First Rewrite Stage

Ingest a Wiktionary snapshot into the tracked raw tables:

```bash
uv run open-dictionary raw-ingest --workdir data/raw
```

Or ingest from an already downloaded local archive:

```bash
uv run open-dictionary raw-ingest \
  --archive-path /path/to/raw-wiktextract-data.jsonl.gz \
  --workdir data/raw
```

This command:

- downloads or registers a source snapshot
- records a tracked pipeline run in `meta.pipeline_runs`
- records the source snapshot in `meta.source_snapshots`
- writes stage progress into `meta.stage_checkpoints`
- loads entries into `raw.wiktionary_entries`
- records malformed source records in `raw.wiktionary_ingest_anomalies`

The rewrite raw-ingest stage reads `.jsonl.gz` archives directly and does not
require a fully materialized extracted JSONL file.

## Build Curated Entries

Transform raw Wiktionary records into word-centric curated entries:

```bash
uv run open-dictionary curated-build
```

Useful flags:

```bash
uv run open-dictionary curated-build --limit-groups 100
uv run open-dictionary curated-build --lang-codes en zh
uv run open-dictionary curated-build --replace-existing
```

This stage writes to:

- `curated.entries`
- `curated.entry_relations`
- `curated.triage_queue`

## Run LLM Enrichment

Generate structured learner-friendly enrichment payloads from curated entries:

```bash
uv run open-dictionary llm-enrich
```

Useful flags:

```bash
uv run open-dictionary llm-enrich --limit-entries 50
uv run open-dictionary llm-enrich --max-workers 4
uv run open-dictionary llm-enrich --recompute-existing
```

This stage writes to:

- `llm.prompt_versions`
- `llm.entry_enrichments`

## Export Audit JSONL

Export the current merged curated-plus-LLM audit artifact:

```bash
uv run open-dictionary export-audit-jsonl --output data/export/audit.jsonl
```

Useful flags:

```bash
uv run open-dictionary export-audit-jsonl --include-unenriched
uv run open-dictionary export-audit-jsonl --model Qwen/Qwen3.5-122B-A10B-FP8
uv run open-dictionary export-audit-jsonl --prompt-version curated_v1_distribution_fields_v1
```

This stage records metadata in:

- `export.artifacts`

Important:

- this audit artifact is not the final learner-facing distribution contract
- it intentionally preserves the internal `curated` and `llm` stage split for
  debugging, replay, and auditability
- the future final distribution JSONL must flatten those stage payloads into a
  product-facing schema

## Optional Snapshot Utilities

Download the compressed snapshot archive for local inspection:

```bash
uv run open-dictionary download --output data/raw-wiktextract-data.jsonl.gz
```

Extract the JSONL file when you need to inspect the raw records directly:

```bash
uv run open-dictionary extract \
  --input data/raw-wiktextract-data.jsonl.gz \
  --output data/raw-wiktextract-data.jsonl
```

The rewrite pipeline itself should use `raw-ingest`, `curated-build`,
`llm-enrich`, and `export-audit-jsonl` rather than the legacy table-mutation
workflow.
