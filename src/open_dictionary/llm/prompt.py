from __future__ import annotations

import json
from typing import Any


PROMPT_VERSION = "curated_v1_enrichment_v1"

OUTPUT_CONTRACT: dict[str, Any] = {
    "type": "object",
    "required": [
        "overview",
        "etymology_story",
        "study_notes",
        "pos_summaries",
    ],
}

SYSTEM_PROMPT = """
You are building a learner-friendly dictionary from curated Wiktionary data.
Return exactly one JSON object and nothing else.

The JSON object must contain:
- overview: short string summarizing the headword
- etymology_story: short string or null
- study_notes: array of short strings
- pos_summaries: array of objects with:
  - pos
  - learner_summary
  - usage_notes
  - flags

Requirements:
- keep the wording compact and explanatory
- do not invent extra parts of speech
- if a field is unknown, use null or []
- output valid JSON only
""".strip()


def build_user_prompt(entry_payload: dict[str, Any]) -> str:
    return (
        "Curated entry payload:\n"
        + json.dumps(entry_payload, ensure_ascii=False, indent=2, sort_keys=True)
    )
