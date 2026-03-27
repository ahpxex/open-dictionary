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

## Export Final JSONL

Export curated entries and their latest successful enrichments into one merged
JSONL artifact:

```bash
uv run open-dictionary export-jsonl --output data/export/final.jsonl
```

Useful flags:

```bash
uv run open-dictionary export-jsonl --include-unenriched
uv run open-dictionary export-jsonl --model Qwen/Qwen3.5-122B-A10B-FP8
uv run open-dictionary export-jsonl --prompt-version curated_v1_enrichment_v1
```

This stage records metadata in:

- `export.artifacts`

## Run The Wiktionary Workflow

Download the compressed dump:

```bash
uv run open-dictionary download --output data/raw-wiktextract-data.jsonl.gz
```

Extract the JSONL file:

```bash
uv run open-dictionary extract \
  --input data/raw-wiktextract-data.jsonl.gz \
  --output data/raw-wiktextract-data.jsonl
```

Stream the JSONL into PostgreSQL (`dictionary_all.data` is JSONB):

```bash
uv run open-dictionary load data/raw-wiktextract-data.jsonl \
  --table dictionary_all \
  --column data \
  --truncate
```

Split rows by language code into per-language tables when needed:

```bash
uv run open-dictionary partition \
  --table dictionary_all \
  --column data \
  --lang-field lang_code
```

Materialize a smaller set of languages into dedicated tables with a custom prefix:

```bash
uv run open-dictionary filter en zh \
  --table dictionary_all \
  --column data \
  --table-prefix dictionary_filtered
```

Pass `all` to emit every language into its own table:

```bash
uv run open-dictionary filter all --table dictionary_all --column data
```

Populate the `common_score` column with word frequency data (re-run with `--recompute-existing` to refresh scores):

```bash
uv run open-dictionary db-commonness --table dictionary_filtered_en
```

Normalize raw Wiktionary payloads into a slimmer JSONB column without invoking LLMs (writes to `process` by default):

_Optionally convert to TOON format for compact downstream processing (stores as TEXT instead of JSONB):_

```bash
uv run open-dictionary pre-process \
  --table dictionary_filtered_en \
  --source-column data \
  --target-column processed \
  --toon
```

Remove low-quality rows (zero common score, numeric tokens, legacy tags) directly in PostgreSQL:

```bash
uv run open-dictionary db-clean --table dictionary_filtered_en
```

Each command streams data in chunks to handle the 10M+ line dataset efficiently.
