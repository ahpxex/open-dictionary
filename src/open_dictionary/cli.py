"""Command-line entry point for the rewrite pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg

from .config import load_settings
from .db.bootstrap import LATEST_FOUNDATION_VERSION, apply_foundation
from .db.connection import get_connection
from .llm.prompt import PROMPT_VERSION
from .sources.wiktionary import (
    DEFAULT_WIKTIONARY_SOURCE_URL,
    download_wiktionary_dump,
    extract_wiktionary_dump,
)
from .stages.curated_build import (
    CURATED_BUILD_STAGE,
    DEFAULT_CURATED_TABLE,
    run_curated_build_stage,
)
from .stages.export_jsonl import EXPORT_JSONL_STAGE, run_export_jsonl_stage
from .stages.llm_enrich import LLM_ENRICH_STAGE, run_llm_enrich_stage
from .stages.raw_ingest import DEFAULT_RAW_TABLE, RAW_INGEST_STAGE, run_raw_ingest_stage


def _add_database_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the .env file containing the database URL (default: .env).",
    )
    parser.add_argument(
        "--database-url-var",
        default="DATABASE_URL",
        help="Environment variable name holding the connection string.",
    )


def _get_settings(args: argparse.Namespace):
    try:
        return load_settings(
            env_file=getattr(args, "env_file", ".env"),
            database_url_var=getattr(args, "database_url_var", "DATABASE_URL"),
        )
    except RuntimeError as exc:
        args._parser.error(str(exc))


def _cmd_download(args: argparse.Namespace) -> int:
    try:
        destination = download_wiktionary_dump(
            args.output,
            url=args.url,
            overwrite=args.overwrite,
        )
    except RuntimeError as exc:  # pragma: no cover - network failure guard
        args._parser.error(str(exc))
    except OSError as exc:
        args._parser.error(str(exc))

    print(f"Downloaded file to {destination}")  # type: ignore[func-returns-value]
    return 0


def _cmd_db_init(args: argparse.Namespace) -> int:
    settings = _get_settings(args)

    with get_connection(settings) as conn:
        applied_versions = apply_foundation(conn)

    if applied_versions:
        print("Applied database migrations: " + ", ".join(applied_versions))
    else:
        print(f"Database foundation {LATEST_FOUNDATION_VERSION} is already applied")
    return 0


def _cmd_curated_build(args: argparse.Namespace) -> int:
    settings = _get_settings(args)

    try:
        result = run_curated_build_stage(
            settings=settings,
            source_table=args.source_table,
            target_table=args.target_table,
            relations_table=args.relations_table,
            triage_table=args.triage_table,
            lang_codes=args.lang_codes,
            limit_groups=args.limit_groups,
            replace_existing=args.replace_existing,
        )
    except (psycopg.Error, ValueError) as exc:
        args._parser.error(f"Database error: {exc}")

    print(
        "Curated build completed "
        f"stage={CURATED_BUILD_STAGE} "
        f"run_id={result.run_id} "
        f"groups_processed={result.groups_processed} "
        f"entries_written={result.entries_written} "
        f"relations_written={result.relations_written} "
        f"triage_written={result.triage_written}"
    )
    return 0


def _cmd_llm_enrich(args: argparse.Namespace) -> int:
    settings = _get_settings(args)

    try:
        result = run_llm_enrich_stage(
            settings=settings,
            env_file=args.env_file,
            source_table=args.source_table,
            target_table=args.target_table,
            prompt_version=args.prompt_version,
            limit_entries=args.limit_entries,
            max_workers=args.max_workers,
            max_retries=args.max_retries,
            recompute_existing=args.recompute_existing,
        )
    except (psycopg.Error, ValueError, RuntimeError) as exc:
        args._parser.error(str(exc))

    print(
        "LLM enrichment completed "
        f"stage={LLM_ENRICH_STAGE} "
        f"run_id={result.run_id} "
        f"processed={result.processed} "
        f"succeeded={result.succeeded} "
        f"failed={result.failed}"
    )
    return 0


def _cmd_export_jsonl(args: argparse.Namespace) -> int:
    settings = _get_settings(args)

    try:
        result = run_export_jsonl_stage(
            settings=settings,
            output_path=args.output,
            curated_table=args.curated_table,
            llm_table=args.llm_table,
            artifact_table=args.artifact_table,
            model=args.model,
            prompt_version=args.prompt_version,
            include_unenriched=args.include_unenriched,
        )
    except (psycopg.Error, ValueError, RuntimeError) as exc:
        args._parser.error(str(exc))

    print(
        "JSONL export completed "
        f"stage={EXPORT_JSONL_STAGE} "
        f"run_id={result.run_id} "
        f"entry_count={result.entry_count} "
        f"output_path={result.output_path} "
        f"output_sha256={result.output_sha256}"
    )
    return 0


def _cmd_extract(args: argparse.Namespace) -> int:
    try:
        output = extract_wiktionary_dump(
            args.input,
            args.output,
            overwrite=args.overwrite,
        )
    except (FileNotFoundError, IsADirectoryError) as exc:
        args._parser.error(str(exc))
    except OSError as exc:
        args._parser.error(str(exc))

    print(f"Extracted archive to {output}")  # type: ignore[func-returns-value]
    return 0


def _cmd_raw_ingest(args: argparse.Namespace) -> int:
    settings = _get_settings(args)

    try:
        result = run_raw_ingest_stage(
            settings=settings,
            workdir=args.workdir,
            source_url=(
                args.source_url or DEFAULT_WIKTIONARY_SOURCE_URL
                if args.archive_path is None
                else None
            ),
            archive_path=args.archive_path,
            target_table=args.target_table,
            overwrite_download=args.overwrite_download,
        )
    except FileNotFoundError as exc:
        args._parser.error(str(exc))
    except RuntimeError as exc:  # pragma: no cover - network failure guard
        args._parser.error(str(exc))
    except (psycopg.Error, ValueError) as exc:
        args._parser.error(f"Database error: {exc}")

    print(
        "Raw ingestion completed "
        f"stage={RAW_INGEST_STAGE} "
        f"run_id={result.run_id} "
        f"snapshot_id={result.snapshot_id} "
        f"rows_loaded={result.rows_loaded} "
        f"anomalies_logged={result.anomalies_logged} "
        f"snapshot_preexisting={result.snapshot_preexisting} "
        f"archive_sha256={result.archive_sha256}"
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Tracked rewrite pipeline for Wiktionary-derived dictionary builds.",
    )
    subparsers = parser.add_subparsers(dest="command")

    db_init_parser = subparsers.add_parser(
        "db-init",
        help="Apply the rewrite foundation schemas and tables.",
    )
    _add_database_options(db_init_parser)
    db_init_parser.set_defaults(func=_cmd_db_init, _parser=db_init_parser)

    curated_build_parser = subparsers.add_parser(
        "curated-build",
        help="Build curated word-centric entries from raw Wiktionary records.",
    )
    curated_build_parser.add_argument(
        "--source-table",
        default=DEFAULT_RAW_TABLE,
        help="Source raw table to read from (default: %(default)s).",
    )
    curated_build_parser.add_argument(
        "--target-table",
        default=DEFAULT_CURATED_TABLE,
        help="Target curated entries table (default: %(default)s).",
    )
    curated_build_parser.add_argument(
        "--relations-table",
        default="curated.entry_relations",
        help="Target table for normalized relations (default: %(default)s).",
    )
    curated_build_parser.add_argument(
        "--triage-table",
        default="curated.triage_queue",
        help="Target table for triage items (default: %(default)s).",
    )
    curated_build_parser.add_argument(
        "--lang-codes",
        nargs="+",
        help="Optional subset of language codes to process.",
    )
    curated_build_parser.add_argument(
        "--limit-groups",
        type=int,
        help="Optional limit on grouped headwords for debugging.",
    )
    curated_build_parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Clear curated output tables before rebuilding them.",
    )
    _add_database_options(curated_build_parser)
    curated_build_parser.set_defaults(func=_cmd_curated_build, _parser=curated_build_parser)

    llm_enrich_parser = subparsers.add_parser(
        "llm-enrich",
        help="Generate structured enrichment payloads from curated entries.",
    )
    llm_enrich_parser.add_argument(
        "--source-table",
        default="curated.entries",
        help="Source curated entries table (default: %(default)s).",
    )
    llm_enrich_parser.add_argument(
        "--target-table",
        default="llm.entry_enrichments",
        help="Target enrichment table (default: %(default)s).",
    )
    llm_enrich_parser.add_argument(
        "--prompt-version",
        default=PROMPT_VERSION,
        help="Prompt version identifier to persist with this run (default: %(default)s).",
    )
    llm_enrich_parser.add_argument(
        "--limit-entries",
        type=int,
        help="Optional limit on entries processed during this run.",
    )
    llm_enrich_parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Number of concurrent worker threads used for LLM calls (default: %(default)s).",
    )
    llm_enrich_parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retries per entry before marking enrichment as failed (default: %(default)s).",
    )
    llm_enrich_parser.add_argument(
        "--recompute-existing",
        action="store_true",
        help="Re-enrich entries even if a successful enrichment already exists.",
    )
    _add_database_options(llm_enrich_parser)
    llm_enrich_parser.set_defaults(func=_cmd_llm_enrich, _parser=llm_enrich_parser)

    export_jsonl_parser = subparsers.add_parser(
        "export-jsonl",
        help="Export curated entries and optional LLM enrichments into a merged JSONL artifact.",
    )
    export_jsonl_parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/export/final.jsonl"),
        help="Output JSONL path (default: %(default)s).",
    )
    export_jsonl_parser.add_argument(
        "--curated-table",
        default="curated.entries",
        help="Source curated entries table (default: %(default)s).",
    )
    export_jsonl_parser.add_argument(
        "--llm-table",
        default="llm.entry_enrichments",
        help="Source LLM enrichments table (default: %(default)s).",
    )
    export_jsonl_parser.add_argument(
        "--artifact-table",
        default="export.artifacts",
        help="Export artifact metadata table (default: %(default)s).",
    )
    export_jsonl_parser.add_argument(
        "--model",
        help="Optional model filter when choosing the latest successful enrichment.",
    )
    export_jsonl_parser.add_argument(
        "--prompt-version",
        help="Optional prompt version filter when choosing the latest successful enrichment.",
    )
    export_jsonl_parser.add_argument(
        "--include-unenriched",
        action="store_true",
        help="Include curated entries even when no successful enrichment exists.",
    )
    _add_database_options(export_jsonl_parser)
    export_jsonl_parser.set_defaults(func=_cmd_export_jsonl, _parser=export_jsonl_parser)

    download_parser = subparsers.add_parser(
        "download",
        help="Download a Wiktionary snapshot archive for local inspection or staged ingest.",
    )
    download_parser.add_argument(
        "--url",
        default=DEFAULT_WIKTIONARY_SOURCE_URL,
        help="Source URL for the Wiktionary dump (default: official raw dataset).",
    )
    download_parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw-wiktextract-data.jsonl.gz"),
        help="Where to store the downloaded archive (default: data/raw-wiktextract-data.jsonl.gz).",
    )
    download_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the existing archive if it already exists.",
    )
    download_parser.set_defaults(func=_cmd_download, _parser=download_parser)

    extract_parser = subparsers.add_parser(
        "extract",
        help="Extract a snapshot archive to a plain JSONL file for local inspection.",
    )
    extract_parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/raw-wiktextract-data.jsonl.gz"),
        help="Path to the .jsonl.gz archive (default: data/raw-wiktextract-data.jsonl.gz).",
    )
    extract_parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw-wiktextract-data.jsonl"),
        help="Where to write the decompressed JSONL file (default: data/raw-wiktextract-data.jsonl).",
    )
    extract_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the extracted JSONL if it already exists.",
    )
    extract_parser.set_defaults(func=_cmd_extract, _parser=extract_parser)

    raw_ingest_parser = subparsers.add_parser(
        "raw-ingest",
        help="Run the tracked raw Wiktionary ingestion stage into the rewrite tables.",
    )
    raw_ingest_parser.add_argument(
        "--workdir",
        type=Path,
        default=Path("data/raw"),
        help="Working directory for archive and extracted JSONL files (default: %(default)s).",
    )
    source_group = raw_ingest_parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--source-url",
        help="Source URL for the Wiktionary snapshot (default: official raw dataset).",
    )
    source_group.add_argument(
        "--archive-path",
        type=Path,
        help="Use an existing local .jsonl or .jsonl.gz archive instead of downloading.",
    )
    raw_ingest_parser.add_argument(
        "--target-table",
        default=DEFAULT_RAW_TABLE,
        help="Destination raw table for imported entries (default: %(default)s).",
    )
    raw_ingest_parser.add_argument(
        "--overwrite-download",
        action="store_true",
        help="Force re-download or overwrite an acquired archive in workdir.",
    )
    _add_database_options(raw_ingest_parser)
    raw_ingest_parser.set_defaults(func=_cmd_raw_ingest, _parser=raw_ingest_parser)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()

    if argv is None:
        argv_list = sys.argv[1:]
    else:
        argv_list = list(argv)

    args = parser.parse_args(argv_list)

    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 1

    return func(args)


if __name__ == "__main__":  # pragma: no cover - CLI entry guard
    sys.exit(main())


__all__ = ["main"]
