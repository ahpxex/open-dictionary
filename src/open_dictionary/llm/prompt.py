from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any

from open_dictionary.contracts import DEFAULT_DEFINITION_LANGUAGE, LanguageSpec, normalize_language_spec


PROMPT_VERSION = "curated_v1_distribution_fields_v2"
DEFAULT_MAX_TOKENS = 1600
COMPACT_RETRY_MAX_TOKENS = 800

OUTPUT_CONTRACT: dict[str, Any] = {
    "type": "object",
    "required": [
        "headword_summary",
        "study_notes",
        "etymology_note",
        "pos_groups",
    ],
}


@dataclass(frozen=True)
class PromptBundle:
    template_version: str
    resolved_prompt_version: str
    definition_language: LanguageSpec
    system_prompt: str
    compact_retry_system_prompt: str
    output_contract: dict[str, Any]

    def as_metadata(self) -> dict[str, Any]:
        return {
            "template_version": self.template_version,
            "resolved_prompt_version": self.resolved_prompt_version,
            "definition_language": self.definition_language.as_dict(),
            "system_prompt": self.system_prompt,
            "compact_retry_system_prompt": self.compact_retry_system_prompt,
            "output_contract": self.output_contract,
        }


def build_prompt_bundle(
    *,
    prompt_version: str = PROMPT_VERSION,
    definition_language: LanguageSpec | dict[str, Any] = DEFAULT_DEFINITION_LANGUAGE,
) -> PromptBundle:
    language = normalize_language_spec(definition_language)
    return PromptBundle(
        template_version=prompt_version,
        resolved_prompt_version=resolve_prompt_version(
            prompt_version=prompt_version,
            definition_language=language,
        ),
        definition_language=language,
        system_prompt=build_system_prompt(language),
        compact_retry_system_prompt=build_compact_retry_system_prompt(language),
        output_contract=OUTPUT_CONTRACT,
    )


def resolve_prompt_version(
    *,
    prompt_version: str,
    definition_language: LanguageSpec | dict[str, Any],
) -> str:
    language = normalize_language_spec(definition_language)
    code_suffix = re.sub(r"[^A-Za-z0-9._-]+", "_", language.code)
    return f"{prompt_version}__deflang__{code_suffix}"


def build_system_prompt(definition_language: LanguageSpec | dict[str, Any]) -> str:
    language = normalize_language_spec(definition_language)
    language_label = f"{language.name} ({language.code})"
    return f"""
You are generating learner-facing dictionary explanations from curated Wiktionary data.
The headword language may vary between entries.
The required definition language for this run is {language_label}.
Every generated natural-language field must be written in {language.name}.
Follow the standard written register and orthography implied by the language tag `{language.code}`.

Return exactly one JSON object and nothing else.

You are only responsible for the generated explanatory fields.
Do not repeat or invent deterministic structural fields such as forms, pronunciations, provenance, or relation tables.

The JSON object must contain:
- headword_summary: non-empty learner-facing summary of the whole headword in {language.name}
- study_notes: array of short study notes in {language.name}
- etymology_note: short note in {language.name} or null
- pos_groups: array with exactly the same pos values as the input skeleton
  - pos_group_id
  - pos
  - summary: non-empty summary for this part of speech in {language.name}
  - usage_notes: {language.name} string or null
  - meanings: array with exactly the same sense_id values as the input skeleton
    - sense_id
    - short_gloss: short cue string in {language.name} or null
    - learner_explanation: detailed natural-language explanation in {language.name}
    - usage_note: {language.name} string or null

Requirements:
- do not invent or rename pos values
- do not invent or rename pos_group_id values
- do not invent or rename sense_id values
- do not omit any pos group from the input
- do not omit any sense_id from the input
- short_gloss is only a helper field; learner_explanation is the main field
- if the headword language and definition language happen to be the same, still paraphrase the curated source instead of copying it mechanically
- if something is uncertain, keep the explanation conservative rather than making up facts
- output valid JSON only

Required output shape:
{{
  "headword_summary": "<non-empty summary in {language.name}>",
  "study_notes": ["<short study note in {language.name}>"],
  "etymology_note": "<short etymology note in {language.name} or null>",
  "pos_groups": [
    {{
      "pos_group_id": "<exactly copied from input>",
      "pos": "<exactly copied from input>",
      "summary": "<non-empty part-of-speech summary in {language.name}>",
      "usage_notes": "<usage note in {language.name} or null>",
      "meanings": [
        {{
          "sense_id": "<exactly copied from input>",
          "short_gloss": "<short cue in {language.name} or null>",
          "learner_explanation": "<detailed explanation in {language.name}>",
          "usage_note": "<usage note in {language.name} or null>"
        }}
      ]
    }}
  ]
}}
""".strip()


def build_compact_retry_system_prompt(definition_language: LanguageSpec | dict[str, Any]) -> str:
    language = normalize_language_spec(definition_language)
    return f"""
You are generating compact learner-facing dictionary explanations in {language.name}.
Return exactly one complete JSON object and nothing else.

Hard constraints:
- every generated natural-language field must be in {language.name}
- follow the orthography/register implied by `{language.code}`
- keep every field short
- headword_summary must be exactly one sentence
- study_notes must be [] or a one-item string array, never null
- pos_groups[].summary must be exactly one sentence
- meanings[].learner_explanation must be exactly one sentence
- use null instead of long commentary when uncertain
- do not use quoted example phrases inside the generated strings
- do not invent or rename pos_group_id, pos, or sense_id
- output valid JSON only

Required JSON keys:
- headword_summary
- study_notes
- etymology_note
- pos_groups

Each pos_groups item must contain:
- pos_group_id
- pos
- summary
- usage_notes
- meanings

Each meanings item must contain:
- sense_id
- short_gloss
- learner_explanation
- usage_note
""".strip()


def build_generation_source_payload(
    entry_payload: dict[str, Any],
    *,
    definition_language: LanguageSpec | dict[str, Any] = DEFAULT_DEFINITION_LANGUAGE,
) -> dict[str, Any]:
    language = normalize_language_spec(definition_language)
    pos_groups = []
    for group in entry_payload.get("pos_groups", []):
        pos = group.get("pos")
        etymology_id = group.get("etymology_id")
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
                "pos_group_id": build_pos_group_id(pos=pos, etymology_id=etymology_id),
                "pos": pos,
                "etymology_id": etymology_id,
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
        "definition_language": language.as_dict(),
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


def build_pos_group_id(*, pos: Any, etymology_id: Any) -> str:
    pos_text = str(pos or "").strip() or "_"
    etymology_text = str(etymology_id or "").strip() or "_"
    return f"{pos_text}|{etymology_text}"


DEFINITION_LANGUAGE = DEFAULT_DEFINITION_LANGUAGE.as_dict()
SYSTEM_PROMPT = build_system_prompt(DEFAULT_DEFINITION_LANGUAGE)
COMPACT_RETRY_SYSTEM_PROMPT = build_compact_retry_system_prompt(DEFAULT_DEFINITION_LANGUAGE)
