# Open Dictionary

Open Dictionary is a staged dictionary-production pipeline built on top of
Wiktionary / Wiktextract data.

The current rewrite line is PostgreSQL-first and contract-driven:

- raw source snapshots are ingested into tracked PostgreSQL tables
- assembled entries are produced as word-centric learner-entry skeletons
- definition generation produces learner-facing explanatory fields in an explicit target definition language
- final read-only artifacts are exported as distribution JSONL and audit JSONL

## Prerequisites

- Install project dependencies: `uv sync`
- Configure a `.env` file with `DATABASE_URL`
- Ensure a PostgreSQL database is reachable via that URL
- Configure a model env file with `LLM_API`, `LLM_KEY`, and `LLM_MODEL` when
  running `generate-definitions` or any pipeline command that reaches the definition-generation stage

## Pipeline Flow

The rewrite pipeline is explicitly staged:

```text
database foundation
  -> init-db

source snapshot acquisition
  -> ingest-snapshot

raw PostgreSQL rows
  -> assemble-entries

curated learner-entry skeletons
  -> generate-definitions

curated structure + generated explanations
  -> export-distribution
  -> validate-distribution

optional debug artifact
  -> export-audit
```

The one-command wrapper for this flow is `run`, which still calls the
stage contracts in order instead of hiding them behind implicit side effects.

## CLI Conventions

All CLI commands now follow the same output conventions:

- `stdout`: one structured JSON result object
- `stderr`: progress events and warnings

Example progress lines:

```text
[progress] stage=definitions.generate event=generate_progress processed=150 queued_entries=742 succeeded=150 failed=0
[progress] stage=distribution.validate event=validate_complete validated_entries=741
```

## Initialize The Rewrite Foundation

Apply the rewrite schemas and metadata tables:

```bash
uv run opend init-db
```

This creates the initial `meta`, `raw`, `curated`, `llm`, and `export` schemas.

## Run The Full Pipeline

Run the full staged pipeline from one CLI command:

```bash
uv run opend run \
  --archive-path fixtures/wiktionary/raw.jsonl \
  --model-env-file .env \
  --distribution-output data/export/distribution.jsonl \
  --validate-distribution
```

Recommended real-model run with adaptive concurrency tiers and both export
artifacts:

```bash
uv run opend run \
  --archive-path fixtures/wiktionary/raw.jsonl \
  --model-env-file .env \
  --worker-tiers 50 12 4 1 \
  --distribution-output data/export/distribution.jsonl \
  --audit-output data/export/audit.jsonl \
  --validate-distribution
```

Example with an explicit non-default definition language:

```bash
uv run opend run \
  --archive-path fixtures/wiktionary/raw.jsonl \
  --lang-codes en \
  --definition-language-code fr \
  --definition-language-name French \
  --model-env-file .env \
  --distribution-output data/export/en-headwords-fr-definitions.jsonl \
  --validate-distribution
```

Useful pipeline flags:

- `--skip-init-db`
- `--lang-codes en zh`
- `--limit-groups 100`
- `--limit-entries 50`
- `--worker-tiers 50 12 4 1`
- `--distribution-output data/export/distribution.jsonl`
- `--audit-output data/export/audit.jsonl`
- `--validate-distribution`
- `--definition-language-code fr`
- `--definition-language-name French`

## Run The First Rewrite Stage

Ingest a Wiktionary snapshot into the tracked raw tables:

```bash
uv run opend ingest-snapshot --workdir data/raw
```

Or ingest from an already downloaded local archive:

```bash
uv run opend ingest-snapshot \
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

The rewrite ingest-snapshot stage reads `.jsonl.gz` archives directly and does not
require a fully materialized extracted JSONL file.

## Build Curated Entries

Transform raw Wiktionary records into word-centric assembled entries:

```bash
uv run opend assemble-entries
```

Useful flags:

```bash
uv run opend assemble-entries --limit-groups 100
uv run opend assemble-entries --lang-codes en zh
uv run opend assemble-entries --replace-existing
```

This stage writes to:

- `curated.entries`
- `curated.entry_relations`
- `curated.triage_queue`

## Generate Definitions

Generate structured learner-facing definitions from assembled entries:

```bash
uv run opend generate-definitions
```

This stage writes to:

- `llm.prompt_versions`
- `llm.entry_enrichments`

Useful flags:

```bash
uv run opend generate-definitions --limit-entries 50
uv run opend generate-definitions --model-env-file .env
uv run opend generate-definitions --max-workers 50
uv run opend generate-definitions --max-retries 3
uv run opend generate-definitions --recompute-existing
uv run opend generate-definitions --definition-language-code en --definition-language-name English
```

## Export Audit JSONL

Export the current merged entries-plus-definitions audit artifact:

```bash
uv run opend export-audit --output data/export/audit.jsonl
```

Useful flags:

```bash
uv run opend export-audit --include-unenriched
uv run opend export-audit --model Qwen/Qwen3.5-35B-A3B-FP8
uv run opend export-audit --prompt-version curated_v1_distribution_fields_v2
uv run opend export-audit --definition-language-code en --definition-language-name English
```

This stage records metadata in:

- `export.artifacts`

Important:

- this audit artifact is not the final learner-facing distribution contract
- it intentionally preserves the internal `curated` and `definitions` stage split for
  debugging, replay, and auditability
- the learner-facing export is a separate command and schema

## Export Distribution JSONL

Export the learner-facing final JSONL artifact:

```bash
uv run opend export-distribution --output data/export/distribution.jsonl
```

Useful flags:

```bash
uv run opend export-distribution --model Qwen/Qwen3.5-35B-A3B-FP8
uv run opend export-distribution --prompt-version curated_v1_distribution_fields_v2
uv run opend export-distribution --definition-language-code en --definition-language-name English
```

This export:

- requires successful definition-generation rows with the distribution-field prompt contract
- flattens curated structure and generated explanatory fields into
  `distribution_entry_v1`
- skips entries that do not contain any distributable meanings after merge
- keeps model, prompt, retries, and provenance in artifact metadata rather than
  leaking them into each distribution row

Validate an existing distribution JSONL file:

```bash
uv run opend validate-distribution \
  --input data/export/distribution.jsonl
```

## Optional Snapshot Utilities

Download the compressed snapshot archive for local inspection:

```bash
uv run opend fetch-snapshot --output data/raw-wiktextract-data.jsonl.gz
```

Extract the JSONL file when you need to inspect the raw records directly:

```bash
uv run opend unpack-snapshot \
  --input data/raw-wiktextract-data.jsonl.gz \
  --output data/raw-wiktextract-data.jsonl
```

## Command Reference

The main commands are:

- `init-db`
- `fetch-snapshot`
- `unpack-snapshot`
- `ingest-snapshot`
- `assemble-entries`
- `generate-definitions`
- `export-audit`
- `export-distribution`
- `validate-distribution`
- `run`
