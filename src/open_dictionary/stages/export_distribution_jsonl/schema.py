from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from open_dictionary.pipeline import ProgressCallback, ThrottledProgressReporter, emit_progress


DISTRIBUTION_SCHEMA_VERSION = "distribution_entry_v1"


@dataclass(frozen=True)
class DistributionJSONLValidationResult:
    output_path: Path
    entry_count: int


def validate_distribution_document(document: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise ValueError("Distribution document must be a JSON object")

    forbidden_keys = {"curated", "llm", "source_summary", "raw_record_refs", "generation_metadata", "model", "prompt_version"}
    present_forbidden = sorted(forbidden_keys & document.keys())
    if present_forbidden:
        raise ValueError(f"Distribution document contains forbidden keys: {present_forbidden}")

    required = {
        "schema_version",
        "entry_id",
        "headword",
        "normalized_headword",
        "headword_language",
        "definition_language",
        "entry_type",
        "headword_summary",
        "study_notes",
        "etymology_note",
        "etymologies",
        "pos_groups",
    }
    missing = required - document.keys()
    if missing:
        raise ValueError(f"Distribution document is missing required keys: {sorted(missing)}")

    if document["schema_version"] != DISTRIBUTION_SCHEMA_VERSION:
        raise ValueError(f"Distribution document schema_version must be {DISTRIBUTION_SCHEMA_VERSION}")
    _require_non_empty_string(document["entry_id"], "entry_id")
    _require_non_empty_string(document["headword"], "headword")
    _require_non_empty_string(document["normalized_headword"], "normalized_headword")
    _validate_language(document["headword_language"], field_name="headword_language")
    _validate_language(document["definition_language"], field_name="definition_language")

    if document["entry_type"] not in {"standard", "proverb", "affix"}:
        raise ValueError("Distribution document entry_type must be one of standard/proverb/affix")
    _require_non_empty_string(document["headword_summary"], "headword_summary")
    document["study_notes"] = _normalize_string_list(document["study_notes"], field_name="study_notes")
    document["etymology_note"] = _normalize_optional_text(document["etymology_note"], field_name="etymology_note")

    if not isinstance(document["etymologies"], list):
        raise ValueError("Distribution document etymologies must be an array")
    for index, item in enumerate(document["etymologies"], start=1):
        _validate_etymology(item, index=index)

    if not isinstance(document["pos_groups"], list):
        raise ValueError("Distribution document pos_groups must be an array")
    pos_group_ids: set[str] = set()
    for index, item in enumerate(document["pos_groups"], start=1):
        pos_group_id = _validate_pos_group(item, index=index)
        if pos_group_id in pos_group_ids:
            raise ValueError(f"Distribution document contains duplicate pos_group_id: {pos_group_id}")
        pos_group_ids.add(pos_group_id)

    return document


def _validate_language(language: Any, *, field_name: str) -> None:
    if not isinstance(language, dict):
        raise ValueError(f"{field_name} must be an object")
    for key in ("code", "name"):
        if key not in language:
            raise ValueError(f"{field_name} is missing {key}")
        _require_non_empty_string(language[key], f"{field_name}.{key}")


def _validate_etymology(item: Any, *, index: int) -> None:
    if not isinstance(item, dict):
        raise ValueError(f"etymologies[{index}] must be an object")
    _require_non_empty_string(item.get("etymology_id"), f"etymologies[{index}].etymology_id")
    _normalize_optional_text(item.get("text"), field_name=f"etymologies[{index}].text")
    _normalize_string_list(item.get("pos_members"), field_name=f"etymologies[{index}].pos_members")


def _validate_pos_group(item: Any, *, index: int) -> str:
    if not isinstance(item, dict):
        raise ValueError(f"pos_groups[{index}] must be an object")
    pos_group_id = _require_non_empty_string(item.get("pos_group_id"), f"pos_groups[{index}].pos_group_id")
    _require_non_empty_string(item.get("pos"), f"pos_groups[{index}].pos")
    _normalize_optional_text(item.get("etymology_id"), field_name=f"pos_groups[{index}].etymology_id")
    _require_non_empty_string(item.get("summary"), f"pos_groups[{index}].summary")
    _normalize_optional_text(item.get("usage_notes"), field_name=f"pos_groups[{index}].usage_notes")

    if not isinstance(item.get("forms"), list):
        raise ValueError(f"pos_groups[{index}].forms must be an array")
    for form_index, form in enumerate(item["forms"], start=1):
        _validate_form(form, field_name=f"pos_groups[{index}].forms[{form_index}]")

    if not isinstance(item.get("pronunciations"), list):
        raise ValueError(f"pos_groups[{index}].pronunciations must be an array")
    for pronunciation_index, pronunciation in enumerate(item["pronunciations"], start=1):
        _validate_pronunciation(pronunciation, field_name=f"pos_groups[{index}].pronunciations[{pronunciation_index}]")

    if not isinstance(item.get("meanings"), list) or not item["meanings"]:
        raise ValueError(f"pos_groups[{index}].meanings must be a non-empty array")
    meaning_ids: set[str] = set()
    for meaning_index, meaning in enumerate(item["meanings"], start=1):
        meaning_id = _validate_meaning(meaning, field_name=f"pos_groups[{index}].meanings[{meaning_index}]")
        if meaning_id in meaning_ids:
            raise ValueError(f"pos_groups[{index}] contains duplicate meaning_id: {meaning_id}")
        meaning_ids.add(meaning_id)

    if not isinstance(item.get("relations"), list):
        raise ValueError(f"pos_groups[{index}].relations must be an array")
    for relation_index, relation in enumerate(item["relations"], start=1):
        _validate_relation(
            relation,
            field_name=f"pos_groups[{index}].relations[{relation_index}]",
            allowed_types={"derived_term", "related_term", "synonym", "antonym", "descendant"},
        )

    return pos_group_id


def _validate_form(item: Any, *, field_name: str) -> None:
    if not isinstance(item, dict):
        raise ValueError(f"{field_name} must be an object")
    _require_non_empty_string(item.get("text"), f"{field_name}.text")
    _normalize_string_list(item.get("tags"), field_name=f"{field_name}.tags")
    _normalize_optional_text(item.get("roman"), field_name=f"{field_name}.roman")


def _validate_pronunciation(item: Any, *, field_name: str) -> None:
    if not isinstance(item, dict):
        raise ValueError(f"{field_name} must be an object")
    ipa = _normalize_optional_text(item.get("ipa"), field_name=f"{field_name}.ipa")
    text = _normalize_optional_text(item.get("text"), field_name=f"{field_name}.text")
    audio_url = _normalize_optional_text(item.get("audio_url"), field_name=f"{field_name}.audio_url")
    _normalize_string_list(item.get("tags"), field_name=f"{field_name}.tags")
    if not any((ipa, text, audio_url)):
        raise ValueError(f"{field_name} must contain at least one of ipa/text/audio_url")


def _validate_meaning(item: Any, *, field_name: str) -> str:
    if not isinstance(item, dict):
        raise ValueError(f"{field_name} must be an object")
    meaning_id = _require_non_empty_string(item.get("meaning_id"), f"{field_name}.meaning_id")
    _normalize_optional_text(item.get("short_gloss"), field_name=f"{field_name}.short_gloss")
    _require_non_empty_string(item.get("learner_explanation"), f"{field_name}.learner_explanation")
    _normalize_optional_text(item.get("usage_note"), field_name=f"{field_name}.usage_note")
    _normalize_string_list(item.get("labels"), field_name=f"{field_name}.labels")
    _normalize_string_list(item.get("topics"), field_name=f"{field_name}.topics")

    if not isinstance(item.get("examples"), list):
        raise ValueError(f"{field_name}.examples must be an array")
    for example_index, example in enumerate(item["examples"], start=1):
        _validate_example(example, field_name=f"{field_name}.examples[{example_index}]")

    if not isinstance(item.get("relations"), list):
        raise ValueError(f"{field_name}.relations must be an array")
    for relation_index, relation in enumerate(item["relations"], start=1):
        _validate_relation(
            relation,
            field_name=f"{field_name}.relations[{relation_index}]",
            allowed_types={"form_of", "alternative_of", "compound_of"},
        )

    return meaning_id


def _validate_example(item: Any, *, field_name: str) -> None:
    if not isinstance(item, dict):
        raise ValueError(f"{field_name} must be an object")
    _require_non_empty_string(item.get("source_text"), f"{field_name}.source_text")
    _normalize_optional_text(item.get("translation"), field_name=f"{field_name}.translation")
    _normalize_optional_text(item.get("note"), field_name=f"{field_name}.note")
    _normalize_optional_text(item.get("ref"), field_name=f"{field_name}.ref")
    _normalize_optional_text(item.get("type"), field_name=f"{field_name}.type")


def _validate_relation(item: Any, *, field_name: str, allowed_types: set[str]) -> None:
    if not isinstance(item, dict):
        raise ValueError(f"{field_name} must be an object")
    relation_type = _require_non_empty_string(item.get("type"), f"{field_name}.type")
    if relation_type not in allowed_types:
        raise ValueError(f"{field_name}.type must be one of {sorted(allowed_types)}")
    _require_non_empty_string(item.get("word"), f"{field_name}.word")
    _normalize_optional_text(item.get("lang_code"), field_name=f"{field_name}.lang_code")


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _normalize_optional_text(value: Any, *, field_name: str) -> str | None:
    if value == "":
        return None
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string or null")
    stripped = value.strip()
    return stripped or None


def _normalize_string_list(value: Any, *, field_name: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{field_name} must be a string array")
    return [item.strip() for item in value if item.strip()]


def validate_distribution_jsonl_file(
    path: str | Path,
    *,
    progress_callback: ProgressCallback | None = None,
) -> DistributionJSONLValidationResult:
    output_path = Path(path)
    entry_count = 0
    reporter = ThrottledProgressReporter(progress_callback, stage="export.distribution_jsonl.validate")
    emit_progress(
        progress_callback,
        stage="export.distribution_jsonl.validate",
        event="validate_start",
        input_path=str(output_path),
    )
    with output_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                document = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{output_path}:{line_number}: invalid JSON: {exc}") from exc
            try:
                validate_distribution_document(document)
            except ValueError as exc:
                raise ValueError(f"{output_path}:{line_number}: {exc}") from exc
            entry_count += 1
            reporter.report(
                event="validate_progress",
                line_number=line_number,
                validated_entries=entry_count,
            )
    emit_progress(
        progress_callback,
        stage="export.distribution_jsonl.validate",
        event="validate_complete",
        input_path=str(output_path),
        validated_entries=entry_count,
    )
    return DistributionJSONLValidationResult(output_path=output_path, entry_count=entry_count)
