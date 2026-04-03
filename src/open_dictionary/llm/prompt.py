from __future__ import annotations

import json
from typing import Any


PROMPT_VERSION = "curated_v1_distribution_fields_v1"
DEFINITION_LANGUAGE = {
    "code": "zh-Hans",
    "name": "Chinese (Simplified)",
}

OUTPUT_CONTRACT: dict[str, Any] = {
    "type": "object",
    "required": [
        "headword_summary",
        "study_notes",
        "etymology_note",
        "pos_groups",
    ],
}

SYSTEM_PROMPT = """
You are generating learner-facing dictionary explanations from curated Wiktionary data.
The final product is a Chinese learner dictionary, so all generated text must be in Simplified Chinese.

Return exactly one JSON object and nothing else.

You are only responsible for the generated explanatory fields.
Do not repeat or invent deterministic structural fields such as forms, pronunciations, provenance, or relation tables.

The JSON object must contain:
- headword_summary: non-empty Chinese summary of the whole headword
- study_notes: array of short Chinese study notes
- etymology_note: short Chinese note or null
- pos_groups: array with exactly the same pos values as the input skeleton
  - pos
  - summary: non-empty Chinese summary for this part of speech
  - usage_notes: Chinese string or null
  - meanings: array with exactly the same sense_id values as the input skeleton
    - sense_id
    - short_gloss: short Chinese cue string or null
    - learner_explanation: detailed Chinese natural-language explanation
    - usage_note: Chinese string or null

Requirements:
- do not invent or rename pos values
- do not invent or rename sense_id values
- do not omit any pos group from the input
- do not omit any sense_id from the input
- short_gloss is only a helper field; learner_explanation is the main field
- if something is uncertain, keep the explanation conservative rather than making up facts
- output valid JSON only

Example input skeleton:
{
  "headword": "sophisticated",
  "headword_language": {"code": "en", "name": "English"},
  "definition_language": {"code": "zh-Hans", "name": "Chinese (Simplified)"},
  "pos_groups": [
    {
      "pos": "adjective",
      "meanings": [
        {
          "sense_id": "s1",
          "gloss": "complex or technically refined",
          "qualifier": null,
          "labels": [],
          "topics": ["technology"]
        }
      ]
    }
  ]
}

Example output:
{
  "headword_summary": "这个词常表示某件事物经过发展后变得精细、成熟，不再是简单直接的状态。",
  "study_notes": ["不要机械地一律翻译成“复杂的”。"],
  "etymology_note": null,
  "pos_groups": [
    {
      "pos": "adjective",
      "summary": "作为形容词时，它既可以形容技术系统复杂精密，也可以形容人或品味显得成熟老练。",
      "usage_notes": "具体中文要根据搭配判断。",
      "meanings": [
        {
          "sense_id": "s1",
          "short_gloss": "复杂精密的；讲究的",
          "learner_explanation": "当它形容 system、technology、method 这类对象时，通常表示结构细、设计成熟、技术含量较高，不只是单纯的“难”。",
          "usage_note": "这一义项往往带有褒义。"
        }
      ]
    }
  ]
}
""".strip()


def build_generation_source_payload(entry_payload: dict[str, Any]) -> dict[str, Any]:
    pos_groups = []
    for group in entry_payload.get("pos_groups", []):
        senses = []
        for sense in group.get("senses", []):
            senses.append(
                {
                    "sense_id": sense.get("sense_id"),
                    "gloss": sense.get("gloss"),
                    "raw_gloss": sense.get("raw_gloss"),
                    "qualifier": sense.get("qualifier"),
                    "labels": sense.get("tags") or [],
                    "topics": sense.get("topics") or [],
                    "examples": [
                        {
                            "text": example.get("text"),
                            "translation": example.get("translation"),
                            "type": example.get("type"),
                            "ref": example.get("ref"),
                        }
                        for example in sense.get("examples", [])
                    ],
                }
            )
        pos_groups.append(
            {
                "pos": group.get("pos"),
                "etymology_id": group.get("etymology_id"),
                "meanings": senses,
            }
        )

    return {
        "entry_id": entry_payload.get("entry_id"),
        "headword": entry_payload.get("word"),
        "normalized_headword": entry_payload.get("normalized_word"),
        "headword_language": {
            "code": entry_payload.get("lang_code"),
            "name": entry_payload.get("lang"),
        },
        "definition_language": DEFINITION_LANGUAGE,
        "entry_flags": entry_payload.get("entry_flags") or [],
        "etymologies": [
            {
                "etymology_id": group.get("etymology_id"),
                "text": group.get("etymology_text"),
                "pos_members": group.get("member_pos") or [],
            }
            for group in entry_payload.get("etymology_groups", [])
        ],
        "pos_groups": pos_groups,
    }


def build_user_prompt(entry_payload: dict[str, Any]) -> str:
    return (
        "Generated-field source payload:\n"
        + json.dumps(entry_payload, ensure_ascii=False, indent=2, sort_keys=True)
    )
