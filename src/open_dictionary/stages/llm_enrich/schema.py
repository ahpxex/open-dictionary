from __future__ import annotations

from typing import Any


def validate_enrichment_payload(payload: dict[str, Any], *, expected_pos: set[str]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("LLM payload must be a JSON object")

    required = {"overview", "etymology_story", "study_notes", "pos_summaries"}
    missing = required - payload.keys()
    if missing:
        raise ValueError(f"LLM payload is missing required keys: {sorted(missing)}")

    if not isinstance(payload["overview"], str) or not payload["overview"].strip():
        raise ValueError("LLM payload overview must be a non-empty string")
    if payload["etymology_story"] == "":
        payload["etymology_story"] = None
    if payload["etymology_story"] is not None and not isinstance(payload["etymology_story"], str):
        raise ValueError("LLM payload etymology_story must be a string or null")
    if isinstance(payload["study_notes"], str):
        payload["study_notes"] = [payload["study_notes"]]
    if not isinstance(payload["study_notes"], list) or not all(isinstance(item, str) for item in payload["study_notes"]):
        raise ValueError("LLM payload study_notes must be a string array")
    if not isinstance(payload["pos_summaries"], list):
        raise ValueError("LLM payload pos_summaries must be an array")

    normalized_pos = set()
    for item in payload["pos_summaries"]:
        if not isinstance(item, dict):
            raise ValueError("Each pos_summaries item must be an object")
        for field in ("pos", "learner_summary", "usage_notes", "flags"):
            if field not in item:
                raise ValueError(f"pos_summaries item is missing {field}")
        if not isinstance(item["pos"], str) or not item["pos"].strip():
            raise ValueError("pos_summaries.pos must be a non-empty string")
        if not isinstance(item["learner_summary"], str):
            raise ValueError("pos_summaries.learner_summary must be a string")
        if isinstance(item["usage_notes"], list):
            if not all(isinstance(part, str) for part in item["usage_notes"]):
                raise ValueError("pos_summaries.usage_notes must be a string or null")
            item["usage_notes"] = " ".join(part.strip() for part in item["usage_notes"] if part.strip()) or None
        if item["usage_notes"] is not None and not isinstance(item["usage_notes"], str):
            raise ValueError("pos_summaries.usage_notes must be a string or null")
        if item["flags"] is None:
            item["flags"] = []
        elif isinstance(item["flags"], str):
            item["flags"] = [item["flags"]]
        if not isinstance(item["flags"], list) or not all(isinstance(flag, str) for flag in item["flags"]):
            raise ValueError("pos_summaries.flags must be a string array")
        normalized_pos.add(item["pos"].strip().casefold())

    unexpected_pos = normalized_pos - expected_pos
    if unexpected_pos:
        raise ValueError(f"LLM payload contains unexpected pos summaries: {sorted(unexpected_pos)}")

    return payload
