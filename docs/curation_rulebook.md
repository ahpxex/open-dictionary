# Curation Rulebook

## Status

This document defines the default curation rules for the first rewrite of
Open Dictionary.

The source layer is Wiktionary / Wiktextract raw data.
The target layer is a curated, word-centric dictionary representation.

This is a production rulebook, not a loose set of ideas.
When curation behavior changes, this document must be updated.

## Core Principle

The curated layer is **not** a mirror of Wiktionary.
It is a controlled dictionary product built from Wiktionary data.

The curation objective is:

- preserve useful lexical information
- remove source-site noise
- normalize fragmented records into a stable contract
- support one headword per curated entry
- keep enough provenance to explain or revisit curation decisions later

## Entry Model

### Canonical unit

- One headword equals one curated entry.
- The primary grouping key is `(lang_code, normalized_word)`.
- Part of speech is **not** the top-level identity of the entry.
- Different parts of speech are represented as subgroups under the same entry.

### Implications

- If the same headword appears multiple times with different `pos`, they should
  usually be merged into one curated entry with multiple `pos_groups`.
- If the same headword appears multiple times with multiple etymologies, the
  curated entry should keep separate etymology groups when the distinction is
  semantically meaningful.
- If multiple raw records are obvious duplicates, they should be collapsed.

## Decision Classes

Every raw record or raw field must end up in one of these classes:

- `keep`: carry into the curated entry in normalized form
- `keep_with_flag`: keep, but mark as special or lower-confidence
- `relation`: do not keep as core content; convert into a relation or link
- `drop`: discard from curated output
- `triage`: send to agent-managed review flow for record-level handling

Important:

- `triage` does **not** mean waiting for manual user review
- `triage` is owned by the agent/system
- only rule-level ambiguity should be escalated to the user
- record-level ambiguity should be handled by agent triage queues or exception
  buckets

## POS Policy

### Keep in V1

These are core lexical categories and should be included in the curated layer:

- `noun`
- `verb`
- `adj`
- `adv`
- `pron`
- `num`
- `prep`
- `phrase`
- `intj`
- `det`

### Keep with flag in V1

These may be useful, but should be explicitly marked as special:

- `proverb`
- `name`
- `suffix`
- `prefix`

Suggested flags:

- `entry_type: proverb`
- `entry_type: proper_name`
- `entry_type: affix`

These records should not be silently mixed into ordinary lexical entries.

### Convert to relation in V1

These should generally not become top-level dictionary entries:

- `romanization`
- `soft-redirect`
- `hard-redirect`

Suggested handling:

- represent them as aliases, redirect edges, or alternate-surface mappings
- attach them to a canonical headword when possible

### Drop by default in V1

These are outside the first product boundary:

- `character`
- `symbol`

Reason:

- they belong to character dictionaries, writing-system references, or symbolic
  reference products more than to the current headword-centric lexical product

### Triage in V1

Any rare or unknown `pos` value not covered above must go to triage.

Examples:

- custom Wiktionary POS labels
- malformed POS labels
- language-specific structural POS values that do not cleanly map into the rule
  set

## Top-Level Field Policy

### Keep

These are primary fields for curated construction:

- `word`
- `lang`
- `lang_code`
- `pos`
- `senses`
- `forms`
- `sounds`
- `etymology_text`

### Keep with normalization

These are useful but must be normalized and often reduced:

- `derived`
- `related`
- `synonyms`
- `antonyms`
- `translations`
- `descendants`
- `hyphenations`
- `hyphenation`

Notes:

- `translations` should not dominate the curated model in V1
- `descendants` are usually too large and should be aggressively reduced or
  deferred
- `sounds` should be reduced to a compact curated pronunciation representation
- `forms` should be normalized into a stable inflection list rather than copied
  verbatim

### Keep only for internal provenance or debugging

These may help explain where curated values came from, but should not usually
  appear in the end-user model:

- `etymology_templates`
- `head_templates`
- `inflection_templates`
- `wikipedia`
- `original_title`
- `title`

### Drop

These should not be part of the curated product model:

- source maintenance categories
- template expansion residue not needed for semantics
- redirect-only scaffolding once canonical links are resolved

In practice this means the following fields are dropped from end-user output by
default:

- `categories`
- raw template scaffolding fields that have already been semantically absorbed

## Sense-Level Field Policy

### Keep

These are sense-defining:

- `glosses`
- `examples`
- `tags`
- `qualifier`
- `topics`
- `form_of`
- `alt_of`

### Keep with caution

These are useful but often noisy:

- `raw_glosses`
- `links`
- `categories`
- `raw_tags`
- `attestations`

Default policy:

- prefer `glosses` over `raw_glosses`
- keep `raw_glosses` only if they preserve meaning missing from `glosses`
- convert `links` into optional metadata only if they prove useful later
- do not expose sense-level `categories` in curated output
- do not expose `attestations` in V1 unless explicitly required

### Convert to relation

- `form_of`
- `alt_of`
- `compound_of`

These should usually be expressed as normalized relations:

- `form_of`
- `alternative_of`
- `compound_of`

### Triage

Sense rows must go to triage when:

- they have no usable `glosses`
- they contain only template residue
- they contain conflicting sense-level structural hints
- they look like malformed redirects or parser artifacts

## Forms Policy

### Keep

Keep forms when they represent genuine inflection or alternate written forms.

Expected fields:

- `form`
- `tags`
- optionally `roman`

### Normalize

Forms must be normalized into:

- the surface form
- a normalized tag set
- optional transliteration or ruby details when meaningful

### Drop or trim

Forms should be reduced when they are clearly excessive or product-irrelevant.

Examples:

- giant name declension tables
- forms that only restate trivial spelling variants without user value
- duplicated forms with the same normalized tag set

### Triage

Send to triage if:

- a record has an extreme number of forms
- forms dominate the record while lexical meaning is weak
- forms conflict across raw duplicates

## Sounds Policy

### Keep

Keep pronunciation data, but in reduced form.

Useful fields include:

- `ipa`
- language-specific pronunciation strings such as `zh_pron`
- `audio`
- `ogg_url`
- `mp3_url`
- `tags`
- `note`

### Normalize

The curated layer should eventually collapse sounds into a smaller model:

- phonetic representations
- language/region tags
- one or a few preferred audio files

### Drop or trim

Do not carry dozens of redundant audio variants into the main curated payload
unless there is a strong product reason.

### Triage

Send to triage if:

- sound data is huge relative to the lexical value of the entry
- the pronunciation fields are structurally irregular
- the entry is mostly pronunciation metadata with minimal lexical content

## Translation Policy

### V1 default

Translations are not core to the first curated dictionary contract.

They may be preserved internally or reduced later, but they should not shape the
core entry model in V1.

### Reason

- translation lists are often massive
- translation quality and coverage are uneven
- the first product goal is a curated headword dictionary, not a translation
  matrix

### Triage

Translation-heavy entries may be triaged if:

- the lexical core is weak and the translation payload is dominant
- translation structure carries meaning that might justify future retention

## Relation Policy

The curated layer should prefer normalized relation edges over raw dump fields.

Potential normalized relation types:

- `derived_term`
- `related_term`
- `synonym`
- `antonym`
- `descendant`
- `form_of`
- `alternative_of`
- `redirect_to`
- `romanization_of`

Relations should be deduplicated and normalized by `(lang_code, word, relation_type)`.

## Merge Policy

### Merge by default

Raw records should be merged into one curated entry when:

- `lang_code` matches
- normalized `word` matches
- the records are not obviously pure redirects

### Keep separate subgroups when needed

The curated entry may still contain separate internal groups for:

- different parts of speech
- separate etymologies
- incompatible sense clusters

### Do not split top-level entry without strong reason

The rulebook is intentionally word-centric.
The system should resist turning every `(word, pos)` pair into a separate top-level row.

## Edge Cases

### 1. Same word, many parts of speech

Default:

- merge into one entry
- store `pos_groups`

### 2. Same word, many etymologies

Default:

- keep one entry
- preserve multiple etymology groups if the senses actually depend on them

### 3. Records with only `form_of` or `alt_of`

Default:

- do not treat as fully independent lexical content
- attach as relations or reduced subordinate content

### 4. Extremely large `forms`

Default:

- keep only meaningful normalized forms
- send pathological cases to triage

### 5. Extremely large `sounds`

Default:

- trim to a compact pronunciation representation
- preserve the raw source link in provenance if needed

### 6. Extremely large `translations`

Default:

- do not include in the main curated structure
- optionally store separately for future use

### 7. Missing `word`, `lang_code`, or `senses`

Default:

- if the entry cannot support a stable curated identity, send to triage or drop
- if it is recoverable from nearby fields, agent triage may repair it

### 8. `name`

Default:

- keep with `entry_type: proper_name`
- do not silently mix with ordinary lexical nouns

### 9. `character`

Default:

- drop from V1 main product
- optionally preserve for future character-dictionary work in a side bucket

### 10. `romanization`

Default:

- convert to relation when a canonical target can be inferred
- otherwise triage

### 11. Redirects

Default:

- do not keep as top-level entries
- convert to alias or redirect relations

### 12. Language-specific pronunciation-heavy entries

Default:

- keep lexical content first
- compress pronunciation aggressively

### 13. Source parser anomalies

Examples:

- malformed tags
- empty sense objects
- duplicated sound objects
- duplicated forms with conflicting tags

Default:

- normalize when deterministic
- triage when not deterministic

## Triage Policy

### Ownership

Record-level triage is owned by the agent/system, not by the user.

The user should only be asked to decide:

- product-level policy changes
- editorial rules that affect many records
- deliberate scope changes

The user should **not** be asked to manually inspect individual raw records as a
normal workflow.

### What triage means

Triage means one of:

- resolve automatically using deterministic fallback rules
- place into a structured exception bucket for later agent processing
- exclude from curated output while preserving provenance

### Triage triggers

Send a record to triage when:

- `pos` is unknown or unsupported
- the record has no stable lexical identity
- multiple raw records conflict in a way not covered by merge rules
- the record is structurally valid but product relevance is unclear
- normalization would otherwise require inventing a new editorial rule

## V1 Curated Contract Direction

The first curated contract should likely contain:

- `word`
- `normalized_word`
- `lang`
- `lang_code`
- `entry_flags`
- `etymology_groups`
- `pos_groups`

Each `pos_group` should likely contain:

- `pos`
- normalized sense list
- normalized forms
- compact pronunciations
- compact relations

This is still a word-centric model, not a Wiktionary mirror.

## What this rulebook intentionally excludes

This document does not yet define:

- final user-facing JSON schema names
- the LLM prompt contract
- export formats
- scoring and prioritization logic

Those belong to later stages.

## Operational Rule

If implementation behavior and this rulebook conflict, the rulebook wins.

If the rulebook proves insufficient for a recurring pattern, update the
rulebook before encoding new long-lived curation behavior.
