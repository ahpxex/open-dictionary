# System Framing

## Purpose

Open Dictionary is a dictionary production system built on top of
Wiktionary / Wiktextract data.

It is not a raw dump browser and it is not a direct LLM wrapper.
Its purpose is to turn large, noisy, fragmented Wiktionary data into a stable,
curated, word-centric dictionary product.

## Product Goal

The product goal is a new dictionary that:

- uses Wiktionary / Wiktextract as the raw lexical source
- stores and processes data through PostgreSQL
- curates source records into one-headword, word-centric entries
- enriches curated entries with LLM-generated structure when needed
- exports stable distribution artifacts such as JSONL and SQLite

## Core Principle

The system is stage-based.

Each stage has:

- a clear input contract
- a clear output contract
- durable run metadata
- explicit ownership of transformation logic

No stage should smuggle in the responsibilities of another stage.

## Canonical Pipeline

The intended end-to-end pipeline is:

1. source snapshot acquisition
2. raw ingestion into PostgreSQL
3. curated normalization from raw tables
4. LLM enrichment from curated tables
5. export packaging

In short:

`source -> raw -> curated -> llm -> export`

## Why PostgreSQL Is The System Of Record

The source data is large and operationally inconvenient as a standalone dump.

The raw Wiktextract dump can be several gigabytes compressed and much larger
once decompressed. That makes direct file-only workflows fragile for normal
iteration.

PostgreSQL is used because it gives the system:

- indexed access to large datasets
- resumable processing by stage
- provenance and run tracking
- reproducible derived tables
- easier inspection during development
- better separation between raw and derived data

The dump file is an input artifact.
PostgreSQL is the working system of record.

## Stage Definitions

### Stage 1: Raw Ingestion

Input:

- a Wiktionary / Wiktextract snapshot

Output:

- source-faithful rows in PostgreSQL raw tables
- snapshot metadata
- run metadata

Responsibilities:

- download or reuse a source snapshot
- extract it if needed
- load it into PostgreSQL with minimal semantic mutation
- preserve line-level provenance and source traceability

Non-goals:

- no editorial filtering
- no user-facing normalization
- no LLM calls

### Stage 2: Curation

Input:

- raw PostgreSQL tables

Output:

- curated, normalized, word-centric entries

Responsibilities:

- merge fragmented raw records into one headword-centric model
- remove source-site noise
- normalize field structure
- transform special record types into retained content, relations, flags, or
  exclusions
- preserve enough provenance to explain how curated data was built

This is the editorial heart of the system.

### Stage 3: LLM Enrichment

Input:

- curated entries only

Output:

- structured LLM-generated data
- generation metadata
- error and retry history

Responsibilities:

- assemble prompt input from curated data
- enforce schema-constrained outputs
- handle concurrency, retries, and resumability
- record model and prompt provenance

Non-goals:

- no direct processing of raw Wiktionary records

### Stage 4: Export

Input:

- curated and/or LLM-enriched data

Output:

- build artifacts such as JSONL and SQLite

Responsibilities:

- generate downstream deliverables
- make outputs reproducible from upstream run lineage

Non-goals:

- no product logic should live only in export code

## Entry Framing

The curated layer is word-centric.

That means:

- one headword is the top-level entry
- different parts of speech are grouped inside the same entry
- different etymologies may still be represented separately inside the entry
- the system should resist splitting every `(word, pos)` into a top-level row

This choice is deliberate.
It keeps the final dictionary aligned with how users expect to encounter words.

## Review Ownership

The user is responsible for rule-level and product-level decisions.
The system is responsible for record-level triage.

In practice:

- the user decides policy
- the agent applies policy
- anomalous records go to agent-managed triage, not to a manual user review queue

This is important because the source data is too large and too irregular to
support a workflow based on manual item-by-item review.

## Full Data vs Fixture Data

The system must support two very different execution modes.

### Full data runs

These are local or dedicated-environment runs over large raw snapshots.

Use cases:

- actual ingestion
- large-scale curation checks
- realistic performance validation

These runs do not belong in normal CI.

### Fixture runs

These are small, repository-stored datasets derived from real raw data.

Use cases:

- development
- repeatable tests
- rule iteration
- CI checks

Fixtures are not fake toy data.
They are representative samples cut from real Wiktextract records.

## Why Full Dumps Do Not Belong In CI

The full data is too large for normal CI/CD.

The system should therefore separate:

- correctness verification on small fixtures
- operational validation on large local or dedicated runs

CI should prove that:

- schema bootstrap works
- stage contracts work
- curation logic behaves as expected on representative samples
- run tracking works

CI should not be responsible for proving that tens of gigabytes of raw lexical
data can be fully processed on every commit.

## Current Development Strategy

The rewrite should proceed in this order:

1. foundation and run tracking
2. raw ingestion
3. curation contract and curation implementation
4. LLM contract and enrichment implementation
5. export layer

The system should not jump directly to LLM prompting before the curated contract
exists.

## Design Constraints

The rewrite must preserve these architectural rules:

- raw data is ingested first and kept source-faithful
- curated data is derived from raw PostgreSQL tables
- LLM enrichment consumes curated data, not raw blobs
- export artifacts are downstream build outputs
- every stage records durable metadata
- full-data processing and fixture-based testing are separate concerns

## Summary

This system exists to transform very large raw Wiktionary data into a curated,
word-centric dictionary product through explicit pipeline stages.

The framing is:

- source-first
- PostgreSQL-backed
- stage-driven
- word-centric
- reproducible
- large-data aware
- fixture-supported for development and CI
