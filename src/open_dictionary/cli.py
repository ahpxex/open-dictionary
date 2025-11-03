"""Command-line entry point for the Open Dictionary toolkit."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from .db import cleaner as db_cleaner
from .db import mark_commonness as db_commonness
from .llm import define_enricher as llm_define_enricher
from .wikitionary.downloader import DEFAULT_WIKTIONARY_URL, download_wiktionary_dump
from .wikitionary.extract import extract_wiktionary_dump
from .wikitionary.filter import filter_languages
from .wikitionary import pre_process as wiktionary_pre_process
from .wikitionary.pipeline import run_pipeline
from .wikitionary.transform import (
    JsonlProcessingError,
    copy_jsonl_to_postgres,
    partition_dictionary_by_language,
)


DEFAULT_DICTIONARY_TABLE = "dictionary_en"


COMMAND_NAMES = {
    "download",
    "extract",
    "filter",
    "load",
    "partition",
    "pipeline",
    "db-clean",
    "db-commonness",
    "llm-define",
    "pre-process",
}


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


def _get_conninfo(args: argparse.Namespace) -> str:
    env_file = getattr(args, "env_file", None)
    if env_file:
        load_dotenv(env_file)

    var_name = getattr(args, "database_url_var", "DATABASE_URL")
    if not var_name:
        raise RuntimeError("Database URL environment variable name cannot be empty")

    conninfo = os.getenv(var_name)  # type: ignore[arg-type]
    if not conninfo:
        raise RuntimeError(
            f"Environment variable {var_name} is not set. Ensure your .env file is loaded."
        )

    return conninfo


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


def _cmd_load(args: argparse.Namespace) -> int:
    try:
        conninfo = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    try:
        rows_copied = copy_jsonl_to_postgres(
            jsonl_path=args.input,
            conninfo=conninfo,  # type: ignore[arg-type]
            table_name=args.table,
            column_name=args.column,
            truncate=args.truncate,
        )
    except (FileNotFoundError, JsonlProcessingError) as exc:
        args._parser.error(str(exc))
    except (psycopg.Error, ValueError) as exc:
        args._parser.error(f"Database error: {exc}")

    print(f"Copied {rows_copied} rows into {args.table}.{args.column}")  # type: ignore[misc]
    return 0


def _cmd_partition(args: argparse.Namespace) -> int:
    try:
        conninfo = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    try:
        created = partition_dictionary_by_language(
            conninfo,  # type: ignore[arg-type]
            source_table=args.table,
            column_name=args.column,
            lang_field=args.lang_field,
            table_prefix=args.prefix,
            target_schema=args.target_schema,
            drop_existing=args.drop_existing,
        )
    except (psycopg.Error, ValueError) as exc:
        args._parser.error(f"Database error: {exc}")

    if created:  # type: ignore[truthy-bool]
        print("Created/updated tables:")
        for table in created:
            print(f"- {table}")
    else:
        print("No language-specific tables were created.")
    return 0


def _cmd_pipeline(args: argparse.Namespace) -> int:
    try:
        conninfo = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    try:
        run_pipeline(
            workdir=args.workdir,
            conninfo=conninfo,  # type: ignore[arg-type]
            table_name=args.table,
            column_name=args.column,
            url=args.url,
            truncate=args.truncate,
            skip_download=args.skip_download,
            skip_extract=args.skip_extract,
            skip_partition=args.skip_partition,
            overwrite_download=args.overwrite_download,
            overwrite_extract=args.overwrite_extract,
            lang_field=args.lang_field,
            table_prefix=args.prefix,
            target_schema=args.target_schema,
            drop_existing_partitions=args.drop_existing_partitions,
        )
    except (FileNotFoundError, JsonlProcessingError) as exc:
        args._parser.error(str(exc))
    except RuntimeError as exc:  # pragma: no cover - network failure guard
        args._parser.error(str(exc))
    except (psycopg.Error, ValueError) as exc:
        args._parser.error(f"Database error: {exc}")

    print("Pipeline completed successfully.")
    return 0


def _cmd_filter(args: argparse.Namespace) -> int:
    try:
        conninfo = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    try:
        created = filter_languages(
            conninfo,  # type: ignore[arg-type]
            source_table=args.table,
            column_name=args.column,
            languages=args.languages,
            lang_field=args.lang_field,
            table_prefix=args.table_prefix,
            target_schema=args.target_schema,
            drop_existing=args.drop_existing,
        )
    except ValueError as exc:
        args._parser.error(str(exc))
    except psycopg.Error as exc:
        args._parser.error(f"Database error: {exc}")

    if created:  # type: ignore[truthy-bool]
        print("Created/updated tables:")
        for table in created:
            print(f"- {table}")
    else:
        print("No tables were created.")
    return 0


def _cmd_db_clean(args: argparse.Namespace) -> int:
    try:
        _ = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    db_cleaner.clean_dictionary_data(
        table_name=args.table,
        fetch_batch_size=args.fetch_batch_size,
        delete_batch_size=args.delete_batch_size,
        progress_every_rows=args.progress_every_rows,
        progress_every_seconds=args.progress_every_seconds,
    )
    return 0


def _cmd_db_commonness(args: argparse.Namespace) -> int:
    try:
        _ = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    db_commonness.enrich_common_score(
        table_name=args.table,
        fetch_batch_size=args.fetch_batch_size,
        update_batch_size=args.update_batch_size,
        progress_every_rows=args.progress_every_rows,
        progress_every_seconds=args.progress_every_seconds,
        recompute_existing=args.recompute_existing,
    )
    return 0


def _cmd_llm_define(args: argparse.Namespace) -> int:
    try:
        _ = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    llm_define_enricher.enrich_definitions(
        table_name=args.table,
        source_column=args.source_column,
        target_column=args.target_column,
        fetch_batch_size=args.fetch_batch_size,
        llm_batch_size=args.llm_batch_size,
        max_workers=args.max_workers,
        max_retries=args.max_retries,
        initial_backoff_seconds=args.initial_backoff_seconds,
        max_backoff_seconds=args.max_backoff_seconds,
        progress_every_rows=args.progress_every_rows,
        progress_every_seconds=args.progress_every_seconds,
        recompute_existing=args.recompute_existing,
    )
    return 0


def _cmd_pre_process(args: argparse.Namespace) -> int:
    try:
        _ = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    wiktionary_pre_process.preprocess_entries(
        table_name=args.table,
        source_column=args.source_column,
        target_column=args.target_column,
        fetch_batch_size=args.fetch_batch_size,
        update_batch_size=args.update_batch_size,
        progress_every_rows=args.progress_every_rows,
        progress_every_seconds=args.progress_every_seconds,
        recompute_existing=args.recompute_existing,
        use_toon=args.toon,
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Utilities for downloading, extracting, and loading Wiktionary dumps.",
    )
    subparsers = parser.add_subparsers(dest="command")

    download_parser = subparsers.add_parser(
        "download",
        help="Download the raw Wiktionary dump (.jsonl.gz).",
    )
    download_parser.add_argument(
        "--url",
        default=DEFAULT_WIKTIONARY_URL,
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
        help="Extract the downloaded .jsonl.gz archive to a plain JSONL file.",
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

    load_parser = subparsers.add_parser(
        "load",
        help="Load a JSONL file into PostgreSQL using COPY.",
    )
    load_parser.add_argument("input", type=Path, help="Path to the JSONL file to load.")
    load_parser.add_argument(
        "--table",
        default="dictionary_all",
        help="Target table name (default: dictionary_all).",
    )
    load_parser.add_argument(
        "--column",
        default="data",
        help="Target JSON/JSONB column name (default: data).",
    )
    load_parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate the destination table before inserting new rows.",
    )
    _add_database_options(load_parser)
    load_parser.set_defaults(func=_cmd_load, _parser=load_parser)

    partition_parser = subparsers.add_parser(
        "partition",
        help="Split the main dictionary table into per-language tables.",
    )
    partition_parser.add_argument(
        "--table",
        default="dictionary_all",
        help="Source table containing the JSONB data (default: dictionary_all).",
    )
    partition_parser.add_argument(
        "--column",
        default="data",
        help="JSONB column to inspect for language codes (default: data).",
    )
    partition_parser.add_argument(
        "--lang-field",
        default="lang_code",
        help="JSON key inside each entry that stores the language code (default: lang_code).",
    )
    partition_parser.add_argument(
        "--prefix",
        default="dictionary_lang",
        help="Prefix for generated tables (default: dictionary_lang).",
    )
    partition_parser.add_argument(
        "--target-schema",
        help="Optional schema to place the generated tables in (default: current search_path).",
    )
    partition_parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop and recreate each language table before inserting rows.",
    )
    _add_database_options(partition_parser)
    partition_parser.set_defaults(func=_cmd_partition, _parser=partition_parser)

    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Run the full download → extract → load → partition workflow.",
    )
    pipeline_parser.add_argument(
        "--workdir",
        type=Path,
        default=Path("data"),
        help="Working directory for downloaded/extracted files (default: data).",
    )
    pipeline_parser.add_argument(
        "--url",
        default=DEFAULT_WIKTIONARY_URL,
        help="Source URL for the Wiktionary dump (default: official raw dataset).",
    )
    pipeline_parser.add_argument(
        "--table",
        default="dictionary_all",
        help="Destination table for the raw entries (default: dictionary_all).",
    )
    pipeline_parser.add_argument(
        "--column",
        default="data",
        help="Destination JSONB column name (default: data).",
    )
    pipeline_parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate the destination table before inserting new rows.",
    )
    pipeline_parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading if the archive is already present.",
    )
    pipeline_parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip extraction if the JSONL file already exists.",
    )
    pipeline_parser.add_argument(
        "--skip-partition",
        action="store_true",
        help="Skip creating per-language tables after loading.",
    )
    pipeline_parser.add_argument(
        "--overwrite-download",
        action="store_true",
        help="Force re-download even if the archive already exists.",
    )
    pipeline_parser.add_argument(
        "--overwrite-extract",
        action="store_true",
        help="Force re-extraction even if the JSONL already exists.",
    )
    pipeline_parser.add_argument(
        "--lang-field",
        default="lang_code",
        help="JSON key inside each entry that stores the language code (default: lang_code).",
    )
    pipeline_parser.add_argument(
        "--prefix",
        default="dictionary_lang",
        help="Prefix for generated language tables (default: dictionary_lang).",
    )
    pipeline_parser.add_argument(
        "--target-schema",
        help="Optional schema to place generated tables in (default: current search_path).",
    )
    pipeline_parser.add_argument(
        "--drop-existing-partitions",
        action="store_true",
        help="Drop existing language tables before rebuilding them.",
    )
    _add_database_options(pipeline_parser)
    pipeline_parser.set_defaults(func=_cmd_pipeline, _parser=pipeline_parser)

    filter_parser = subparsers.add_parser(
        "filter",
        help="Filter existing dictionary entries into language-specific tables.",
    )
    filter_parser.add_argument(
        "languages",
        nargs="+",
        help="Language codes to materialize (e.g. en zh fr, or 'all').",
    )
    filter_parser.add_argument(
        "--table",
        default="dictionary_all",
        help="Source table containing the raw entries (default: dictionary_all).",
    )
    filter_parser.add_argument(
        "--column",
        default="data",
        help="JSONB column storing the dictionary payloads (default: data).",
    )
    filter_parser.add_argument(
        "--lang-field",
        default="lang_code",
        help="JSON key containing the language code (default: lang_code).",
    )
    filter_parser.add_argument(
        "--table-prefix",
        default="dictionary_lang",
        help="Base name for materialized tables; language code is appended (default: dictionary_lang).",
    )
    filter_parser.add_argument(
        "--target-schema",
        help="Optional schema for the materialized tables (default: current search_path).",
    )
    filter_parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop existing destination tables before inserting rows.",
    )
    _add_database_options(filter_parser)
    filter_parser.set_defaults(func=_cmd_filter, _parser=filter_parser)

    pre_process_parser = subparsers.add_parser(
        "pre-process",
        help="Trim Wiktionary entries to the subset needed by downstream workflows.",
    )
    pre_process_parser.add_argument(
        "--table",
        default="dictionary_all",
        help="Source table containing raw Wiktionary entries (default: %(default)s).",
    )
    pre_process_parser.add_argument(
        "--source-column",
        default="data",
        help="Column storing the original Wiktionary JSON (default: %(default)s).",
    )
    pre_process_parser.add_argument(
        "--target-column",
        default="process",
        help="Column to store the normalized JSON (default: %(default)s).",
    )
    pre_process_parser.add_argument(
        "--fetch-batch-size",
        type=int,
        default=wiktionary_pre_process.FETCH_BATCH_SIZE,
        help="Rows fetched per streaming batch (default: %(default)s).",
    )
    pre_process_parser.add_argument(
        "--update-batch-size",
        type=int,
        default=wiktionary_pre_process.UPDATE_BATCH_SIZE,
        help="Rows updated per write batch (default: %(default)s).",
    )
    pre_process_parser.add_argument(
        "--progress-every-rows",
        type=int,
        default=wiktionary_pre_process.PROGRESS_EVERY_ROWS,
        help="Emit progress after this many processed rows (default: %(default)s).",
    )
    pre_process_parser.add_argument(
        "--progress-every-seconds",
        type=float,
        default=wiktionary_pre_process.PROGRESS_EVERY_SECONDS,
        help="Emit progress at least this often in seconds (default: %(default)s).",
    )
    pre_process_parser.add_argument(
        "--recompute-existing",
        action="store_true",
        help="Regenerate payloads even if the target column is already populated.",
    )
    pre_process_parser.add_argument(
        "--toon",
        action="store_true",
        help="Convert processed payloads to TOON format (reduces token usage for LLMs).",
    )
    _add_database_options(pre_process_parser)
    pre_process_parser.set_defaults(func=_cmd_pre_process, _parser=pre_process_parser)

    db_clean_parser = subparsers.add_parser(
        "db-clean",
        help="Remove low-quality entries from a dictionary table.",
    )
    db_clean_parser.add_argument(
        "--table",
        default=DEFAULT_DICTIONARY_TABLE,
        help="Source table containing JSONB entries (default: %(default)s).",
    )
    db_clean_parser.add_argument(
        "--fetch-batch-size",
        type=int,
        default=db_cleaner.FETCH_BATCH_SIZE,
        help="Number of rows to fetch per batch (default: %(default)s).",
    )
    db_clean_parser.add_argument(
        "--delete-batch-size",
        type=int,
        default=db_cleaner.DELETE_BATCH_SIZE,
        help="Number of rows to delete per batch (default: %(default)s).",
    )
    db_clean_parser.add_argument(
        "--progress-every-rows",
        type=int,
        default=db_cleaner.PROGRESS_EVERY_ROWS,
        help="Emit progress after this many processed rows (default: %(default)s).",
    )
    db_clean_parser.add_argument(
        "--progress-every-seconds",
        type=float,
        default=db_cleaner.PROGRESS_EVERY_SECONDS,
        help="Emit progress at least this often in seconds (default: %(default)s).",
    )
    _add_database_options(db_clean_parser)
    db_clean_parser.set_defaults(func=_cmd_db_clean, _parser=db_clean_parser)

    db_common_parser = subparsers.add_parser(
        "db-commonness",
        help="Populate the common_score column using word frequency data.",
    )
    db_common_parser.add_argument(
        "--table",
        default=DEFAULT_DICTIONARY_TABLE,
        help="Target dictionary table (default: %(default)s).",
    )
    db_common_parser.add_argument(
        "--fetch-batch-size",
        type=int,
        default=db_commonness.FETCH_BATCH_SIZE,
        help="Number of rows to fetch per batch (default: %(default)s).",
    )
    db_common_parser.add_argument(
        "--update-batch-size",
        type=int,
        default=db_commonness.UPDATE_BATCH_SIZE,
        help="Number of rows to update per batch (default: %(default)s).",
    )
    db_common_parser.add_argument(
        "--progress-every-rows",
        type=int,
        default=db_commonness.PROGRESS_EVERY_ROWS,
        help="Emit progress after this many processed rows (default: %(default)s).",
    )
    db_common_parser.add_argument(
        "--progress-every-seconds",
        type=float,
        default=db_commonness.PROGRESS_EVERY_SECONDS,
        help="Emit progress at least this often in seconds (default: %(default)s).",
    )
    db_common_parser.add_argument(
        "--recompute-existing",
        action="store_true",
        help="Recalculate scores even if a value already exists.",
    )
    _add_database_options(db_common_parser)
    db_common_parser.set_defaults(func=_cmd_db_commonness, _parser=db_common_parser)

    llm_define_parser = subparsers.add_parser(
        "llm-define",
        help="Generate enriched dictionary entries via the LLM define workflow.",
    )
    llm_define_parser.add_argument(
        "--table",
        default=llm_define_enricher.DEFAULT_TABLE_NAME,
        help="Source table containing JSONB entries (default: %(default)s).",
    )
    llm_define_parser.add_argument(
        "--source-column",
        default=llm_define_enricher.DEFAULT_SOURCE_COLUMN,
        help="Column containing original Wiktionary payloads (default: %(default)s).",
    )
    llm_define_parser.add_argument(
        "--target-column",
        default=llm_define_enricher.DEFAULT_TARGET_COLUMN,
        help="Column to store LLM-enriched JSONB (default: %(default)s).",
    )
    llm_define_parser.add_argument(
        "--fetch-batch-size",
        type=int,
        default=llm_define_enricher.DEFAULT_FETCH_BATCH_SIZE,
        help="Rows fetched from PostgreSQL per server-side batch (default: %(default)s).",
    )
    llm_define_parser.add_argument(
        "--llm-batch-size",
        type=int,
        default=llm_define_enricher.DEFAULT_LLM_BATCH_SIZE,
        help="Number of requests dispatched to the LLM at once (default: %(default)s).",
    )
    llm_define_parser.add_argument(
        "--max-workers",
        type=int,
        help="Maximum concurrent worker threads for LLM calls (default: llm-batch-size).",
    )
    llm_define_parser.add_argument(
        "--max-retries",
        type=int,
        default=llm_define_enricher.DEFAULT_MAX_RETRIES,
        help="Attempts per row before giving up (default: %(default)s).",
    )
    llm_define_parser.add_argument(
        "--initial-backoff-seconds",
        type=float,
        default=llm_define_enricher.DEFAULT_INITIAL_BACKOFF_SECONDS,
        help="Initial retry backoff in seconds (default: %(default)s).",
    )
    llm_define_parser.add_argument(
        "--max-backoff-seconds",
        type=float,
        default=llm_define_enricher.DEFAULT_MAX_BACKOFF_SECONDS,
        help="Maximum retry backoff in seconds (default: %(default)s).",
    )
    llm_define_parser.add_argument(
        "--progress-every-rows",
        type=int,
        default=llm_define_enricher.DEFAULT_PROGRESS_EVERY_ROWS,
        help="Emit progress after processing this many rows (default: %(default)s).",
    )
    llm_define_parser.add_argument(
        "--progress-every-seconds",
        type=float,
        default=llm_define_enricher.DEFAULT_PROGRESS_EVERY_SECONDS,
        help="Emit progress at least this often in seconds (default: %(default)s).",
    )
    llm_define_parser.add_argument(
        "--recompute-existing",
        action="store_true",
        help="Recreate target-column payloads even if already populated.",
    )
    _add_database_options(llm_define_parser)
    llm_define_parser.set_defaults(func=_cmd_llm_define, _parser=llm_define_parser)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()

    if argv is None:
        argv_list = sys.argv[1:]
    else:
        argv_list = list(argv)

    if argv_list and not argv_list[0].startswith("-") and argv_list[0] not in COMMAND_NAMES:
        argv_list = ["load", *argv_list]

    args = parser.parse_args(argv_list)

    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 1

    return func(args)


if __name__ == "__main__":  # pragma: no cover - CLI entry guard
    sys.exit(main())


__all__ = ["main"]
