# Open Dictionary

Open Dictionary is a staged dictionary-production pipeline built on top of
Wiktionary / Wiktextract data.

The current rewrite line is PostgreSQL-first and contract-driven:

- raw source snapshots are ingested into tracked PostgreSQL tables
- curated entries are assembled as word-centric learner-entry skeletons
- LLM enrichment generates Chinese learner-facing explanatory fields
- final read-only artifacts are exported as distribution JSONL and audit JSONL

## Prerequisites

- Install project dependencies: `uv sync`
- Configure a `.env` file with `DATABASE_URL`
- Ensure a PostgreSQL database is reachable via that URL
- Configure an LLM env file with `LLM_API`, `LLM_KEY`, and `LLM_MODEL` when
  running `llm-enrich` or any pipeline command that reaches the LLM stage

## Pipeline Flow

The rewrite pipeline is explicitly staged:

```text
database foundation
  -> db-init

source snapshot acquisition
  -> raw-ingest

raw PostgreSQL rows
  -> curated-build

curated learner-entry skeletons
  -> llm-enrich

curated structure + generated explanations
  -> export-distribution-jsonl
  -> validate-distribution-jsonl

optional debug artifact
  -> export-audit-jsonl
```

The one-command wrapper for this flow is `pipeline-run`, which still calls the
stage contracts in order instead of hiding them behind implicit side effects.

## CLI Conventions

All CLI commands now follow the same output conventions:

- `stdout`: one structured JSON result object
- `stderr`: progress events and warnings

Example progress lines:

```text
[progress] stage=llm.enrich event=enrich_progress processed=150 queued_entries=742 succeeded=150 failed=0
[progress] stage=export.distribution_jsonl.validate event=validate_complete validated_entries=741
```

## Initialize The Rewrite Foundation

Apply the rewrite schemas and metadata tables:

```bash
uv run open-dictionary db-init
```

This creates the initial `meta`, `raw`, `curated`, `llm`, and `export` schemas.

## Run The Full Pipeline

Run the full staged pipeline from one CLI command:

```bash
uv run open-dictionary pipeline-run \
  --archive-path fixtures/wiktionary/raw.jsonl \
  --llm-env-file .env \
  --distribution-output data/export/distribution.jsonl \
  --validate-distribution
```

Recommended real-model run with adaptive concurrency tiers and both export
artifacts:

```bash
uv run open-dictionary pipeline-run \
  --archive-path fixtures/wiktionary/raw.jsonl \
  --llm-env-file .env \
  --worker-tiers 50 12 4 1 \
  --distribution-output data/export/distribution.jsonl \
  --audit-output data/export/audit.jsonl \
  --validate-distribution
```

Useful pipeline flags:

- `--skip-db-init`
- `--lang-codes en zh`
- `--limit-groups 100`
- `--limit-entries 50`
- `--worker-tiers 50 12 4 1`
- `--distribution-output data/export/distribution.jsonl`
- `--audit-output data/export/audit.jsonl`
- `--validate-distribution`

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

This stage writes to:

- `llm.prompt_versions`
- `llm.entry_enrichments`

Useful flags:

```bash
uv run open-dictionary llm-enrich --limit-entries 50
uv run open-dictionary llm-enrich --env-file .env
uv run open-dictionary llm-enrich --max-workers 50
uv run open-dictionary llm-enrich --max-retries 3
uv run open-dictionary llm-enrich --recompute-existing
```

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
- the learner-facing export is a separate command and schema
- `export-jsonl` is a deprecated alias for this command

## Export Distribution JSONL

Export the learner-facing final JSONL artifact:

```bash
uv run open-dictionary export-distribution-jsonl --output data/export/distribution.jsonl
```

Useful flags:

```bash
uv run open-dictionary export-distribution-jsonl --model Qwen/Qwen3.5-122B-A10B-FP8
uv run open-dictionary export-distribution-jsonl --prompt-version curated_v1_distribution_fields_v1
```

This export:

- requires successful LLM enrichments with the distribution-field prompt contract
- flattens curated structure and generated explanatory fields into
  `distribution_entry_v1`
- skips entries that do not contain any distributable meanings after merge
- keeps model, prompt, retries, and provenance in artifact metadata rather than
  leaking them into each distribution row

Validate an existing distribution JSONL file:

```bash
uv run open-dictionary validate-distribution-jsonl \
  --input data/export/distribution.jsonl
```

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

## Command Reference

The main commands are:

- `db-init`
- `download`
- `extract`
- `raw-ingest`
- `curated-build`
- `llm-enrich`
- `export-audit-jsonl`
- `export-distribution-jsonl`
- `validate-distribution-jsonl`
- `pipeline-run`
