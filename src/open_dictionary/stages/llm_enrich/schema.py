from __future__ import annotations

from typing import Any


def build_expected_generation_targets(entry_payload: dict[str, Any]) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for group in entry_payload.get("pos_groups", []):
        pos_group_id = str(group.get("pos_group_id") or "").strip()
        pos = str(group.get("pos") or "").strip()
        if not pos_group_id or not pos:
            continue
        sense_ids = []
        meanings = group.get("meanings") or group.get("senses") or []
        for sense in meanings:
            sense_id = str(sense.get("sense_id") or "").strip()
            if sense_id:
                sense_ids.append(sense_id)
        targets.append({"pos_group_id": pos_group_id, "pos": pos, "sense_ids": sense_ids})
    return targets


def validate_enrichment_payload(
    payload: dict[str, Any],
    *,
    expected_pos_targets: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("LLM payload must be a JSON object")

    required = {"headword_summary", "study_notes", "etymology_note", "pos_groups"}
    missing = required - payload.keys()
    if missing:
        raise ValueError(f"LLM payload is missing required keys: {sorted(missing)}")

    if not isinstance(payload["headword_summary"], str) or not payload["headword_summary"].strip():
        raise ValueError("LLM payload headword_summary must be a non-empty string")

    if payload["study_notes"] is None:
        payload["study_notes"] = []
    payload["study_notes"] = _normalize_string_list(payload["study_notes"], field_name="study_notes")
    payload["etymology_note"] = _normalize_optional_text(payload["etymology_note"], field_name="etymology_note")

    pos_groups = payload["pos_groups"]
    if not isinstance(pos_groups, list):
        raise ValueError("LLM payload pos_groups must be an array")

    expected_pos_group_index = {item["pos_group_id"]: item for item in expected_pos_targets}
    normalized_pos_groups: dict[str, dict[str, Any]] = {}

    for item in pos_groups:
        if not isinstance(item, dict):
            raise ValueError("Each pos_groups item must be an object")
        for field in ("pos_group_id", "pos", "summary", "usage_notes", "meanings"):
            if field not in item:
                raise ValueError(f"pos_groups item is missing {field}")

        pos_group_id = str(item["pos_group_id"] or "").strip()
        pos = str(item["pos"] or "").strip()
        if not pos_group_id:
            raise ValueError("pos_groups.pos_group_id must be a non-empty string")
        if not pos:
            raise ValueError("pos_groups.pos must be a non-empty string")
        if pos_group_id in normalized_pos_groups:
            raise ValueError(f"LLM payload contains duplicate pos_group_id: {pos_group_id}")
        expected_group = expected_pos_group_index.get(pos_group_id)
        if expected_group is None:
            raise ValueError(f"LLM payload contains unexpected pos_group_id: {pos_group_id}")
        if pos.casefold() != expected_group["pos"].strip().casefold():
            raise ValueError(
                f"LLM payload pos mismatch for pos_group_id {pos_group_id}: expected {expected_group['pos']}, got {pos}"
            )

        if not isinstance(item["summary"], str) or not item["summary"].strip():
            raise ValueError("pos_groups.summary must be a non-empty string")
        item["usage_notes"] = _normalize_optional_text(item["usage_notes"], field_name="pos_groups.usage_notes")
        item["meanings"] = _validate_meanings(
            item["meanings"],
            expected_sense_ids=expected_group["sense_ids"],
            pos=pos,
        )
        normalized_pos_groups[pos_group_id] = item

    missing_pos_group_ids = [item["pos_group_id"] for item in expected_pos_targets if item["pos_group_id"] not in normalized_pos_groups]
    if missing_pos_group_ids:
        raise ValueError(f"LLM payload is missing pos_group_ids: {missing_pos_group_ids}")

    payload["pos_groups"] = [
        normalized_pos_groups[item["pos_group_id"]]
        for item in expected_pos_targets
    ]
    return payload


def _validate_meanings(
    meanings: Any,
    *,
    expected_sense_ids: list[str],
    pos: str,
) -> list[dict[str, Any]]:
    if not isinstance(meanings, list):
        raise ValueError("pos_groups.meanings must be an array")

    expected_index = {sense_id: idx for idx, sense_id in enumerate(expected_sense_ids)}
    normalized_meanings: dict[str, dict[str, Any]] = {}

    for item in meanings:
        if not isinstance(item, dict):
            raise ValueError("Each meanings item must be an object")
        for field in ("sense_id", "short_gloss", "learner_explanation", "usage_note"):
            if field not in item:
                raise ValueError(f"meanings item is missing {field}")

        sense_id = str(item["sense_id"] or "").strip()
        if not sense_id:
            raise ValueError("meanings.sense_id must be a non-empty string")
        if sense_id in normalized_meanings:
            raise ValueError(f"LLM payload contains duplicate sense_id in pos {pos}: {sense_id}")
        if sense_id not in expected_index:
            raise ValueError(f"LLM payload contains unexpected sense_id in pos {pos}: {sense_id}")

        item["short_gloss"] = _normalize_optional_text(item["short_gloss"], field_name="meanings.short_gloss")
        if not isinstance(item["learner_explanation"], str) or not item["learner_explanation"].strip():
            raise ValueError("meanings.learner_explanation must be a non-empty string")
        item["usage_note"] = _normalize_optional_text(item["usage_note"], field_name="meanings.usage_note")
        normalized_meanings[sense_id] = item

    missing_sense_ids = [sense_id for sense_id in expected_sense_ids if sense_id not in normalized_meanings]
    if missing_sense_ids:
        raise ValueError(f"LLM payload is missing sense_ids in pos {pos}: {missing_sense_ids}")

    return [normalized_meanings[sense_id] for sense_id in expected_sense_ids]


def _normalize_optional_text(value: Any, *, field_name: str) -> str | None:
    if value == "":
        return None
    if isinstance(value, list):
        if not all(isinstance(item, str) for item in value):
            raise ValueError(f"{field_name} must be a string or null")
        joined = " ".join(item.strip() for item in value if item.strip())
        return joined or None
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string or null")
    stripped = value.strip()
    return stripped or None


def _normalize_string_list(value: Any, *, field_name: str) -> list[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{field_name} must be a string array")
    return [item.strip() for item in value if item.strip()]
