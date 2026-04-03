# Export Contracts

## Status

Open Dictionary currently has one implemented JSONL export:

- `audit_jsonl`

This is an internal audit artifact.
It is **not** the final learner-facing distribution format.

The project must keep these two export classes distinct:

- audit export: preserves stage boundaries for debugging and reproducibility
- distribution export: final product contract for downstream clients

## Current Audit JSONL

The current implemented export is a merged row of:

- top-level entry identity fields
- `curated` payload
- `llm` payload

Purpose:

- inspect the current curated structure
- inspect the current LLM response payload
- debug export selection rules
- compare prompt/model variants
- retain a replayable artifact outside PostgreSQL

Non-goals:

- this format is not meant to be stable for end-user products
- this format must not be treated as the final dictionary contract

## Target Distribution Contract

The target final JSONL contract is `distribution_entry_v1`.

Each row must represent one learner-facing dictionary entry and must not expose
internal pipeline-stage wrappers such as `curated` and `llm`.

### Required top-level structure

```json
{
  "schema_version": "distribution_entry_v1",
  "entry_id": "string",
  "headword": "string",
  "normalized_headword": "string",
  "headword_language": {
    "code": "string",
    "name": "string"
  },
  "definition_language": {
    "code": "string",
    "name": "string"
  },
  "entry_type": "standard",
  "headword_summary": "string",
  "study_notes": ["string"],
  "etymology_note": "string or null",
  "etymologies": [],
  "pos_groups": []
}
```

### Meaning-level rule

The final contract must not collapse meaning content back into short traditional
glosses.

Every meaning row should have:

- optional `short_gloss` for compact indexing or quick scanning
- required `learner_explanation` as the main Chinese natural-language
  explanation
- optional `usage_note`

The learner explanation is the product field.
The short gloss is only a helper field.

### Language rule

The final contract must explicitly separate:

- `headword_language`
- `definition_language`

This avoids conflating source-language identity with explanation-language
identity.

## What Must Stay Out Of Distribution Rows

The following belong in PostgreSQL metadata or artifact manifests, not in final
distribution entry rows:

- `run_id`
- `snapshot_id`
- `raw_record_refs`
- `raw_run_ids`
- `raw_snapshot_ids`
- `model`
- `prompt_version`
- raw LLM response wrappers
- stage wrapper keys such as `curated` and `llm`

## How To Enforce The Contract In The LLM Workflow

The distribution contract should not be enforced by prompt wording alone.
It must be enforced at multiple layers.

### 1. Keep structure deterministic outside the model

The model should not be responsible for inventing the outer entry shape.

Deterministic code should own:

- entry identity fields
- language fields
- etymology grouping
- part-of-speech grouping
- forms
- pronunciations
- normalized relation edges
- example source text and references

The model should only generate learner-facing explanatory fields.

### 2. Give the model stable IDs from curated input

The LLM input should include stable identifiers from curated data, such as:

- `entry_id`
- `etymology_id`
- `pos`
- `sense_id`

The model output must attach explanatory content back to those IDs instead of
inventing a free-form structure.

### 3. Narrow the LLM output contract

The next LLM contract should generate only the fields that genuinely require
generation, for example:

- `headword_summary`
- `study_notes`
- `etymology_note`
- `pos_groups[].summary`
- `pos_groups[].usage_notes`
- `meanings[].learner_explanation`
- `meanings[].usage_note`

It should not generate:

- forms
- pronunciations
- provenance
- distribution wrappers

### 4. Validate against the curated shape

Schema validation must check more than JSON shape.
It must also verify alignment with the curated source row.

Examples:

- no extra `pos` groups beyond curated input
- no missing required sense IDs
- no unknown sense IDs
- no extra etymology IDs
- no empty learner explanations

### 5. Assemble the final distribution row in deterministic code

The export layer should merge:

- structural fields from curated data
- explanatory fields from validated LLM data

The model should never emit the final distribution row directly.

### 6. Keep auditability separate

Prompt version, model, retries, failures, input hash, and raw response stay in:

- `llm.*` tables
- `meta.pipeline_runs`
- export artifact manifests

They do not belong in final dictionary rows.

### 7. Test the contract explicitly

Tests for the future distribution export must assert:

- no `curated` key
- no `llm` key
- no provenance leakage
- presence of `headword_language`
- presence of `definition_language`
- presence of `learner_explanation`
- product rows remain reproducible from stored curated and llm runs
