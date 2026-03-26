from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
import gzip
import hashlib
import json
from pathlib import Path
from typing import Iterable


DEFAULT_INPUT = Path("/Users/ahpx/Downloads/raw-wiktextract-data.jsonl.gz")
DEFAULT_OUTPUT = Path("fixtures/wiktionary/raw.jsonl")
DEFAULT_SUMMARY = Path("fixtures/wiktionary/raw.summary.json")
DEFAULT_LIMIT = 1000
DEFAULT_PER_BUCKET = 8
DEFAULT_SIGNATURE_POOL = 64
DEFAULT_SIGNATURE_QUOTA = 240


@dataclass(frozen=True)
class Candidate:
    line_number: int
    json_text: str
    word: str | None
    lang_code: str | None
    pos: str | None
    bucket: str
    feature_signature: str
    feature_score: int
    sample_key: int


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a diverse raw fixture from a Wiktextract JSONL(.gz) dump."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--per-bucket", type=int, default=DEFAULT_PER_BUCKET)
    parser.add_argument("--signature-pool", type=int, default=DEFAULT_SIGNATURE_POOL)
    parser.add_argument("--signature-quota", type=int, default=DEFAULT_SIGNATURE_QUOTA)
    args = parser.parse_args()

    if args.limit <= 0:
        raise SystemExit("--limit must be positive")
    if args.per_bucket <= 0:
        raise SystemExit("--per-bucket must be positive")
    if args.signature_pool <= 0:
        raise SystemExit("--signature-pool must be positive")
    if args.signature_quota < 0:
        raise SystemExit("--signature-quota must be non-negative")

    bucket_counts: Counter[str] = Counter()
    signature_counts: Counter[str] = Counter()
    candidates_by_bucket: dict[str, list[Candidate]] = defaultdict(list)
    candidates_by_signature: dict[str, list[Candidate]] = defaultdict(list)

    rows_scanned = 0

    for line_number, json_text, payload in iter_records(args.input):
        rows_scanned += 1
        candidate = build_candidate(line_number, json_text, payload)

        bucket_counts[candidate.bucket] += 1
        signature_counts[candidate.feature_signature] += 1

        bucket_items = candidates_by_bucket[candidate.bucket]
        bucket_items.append(candidate)
        trim_candidates(bucket_items, args.per_bucket)

        signature_items = candidates_by_signature[candidate.feature_signature]
        signature_items.append(candidate)
        trim_candidates(signature_items, args.signature_pool)

        if rows_scanned % 250_000 == 0:
            print(
                f"[fixture] scanned={rows_scanned:,} "
                f"buckets={len(bucket_counts):,} signatures={len(signature_counts):,}",
                flush=True,
            )

    selected = select_candidates(
        bucket_counts=bucket_counts,
        signature_counts=signature_counts,
        candidates_by_bucket=candidates_by_bucket,
        candidates_by_signature=candidates_by_signature,
        limit=args.limit,
        signature_quota=min(args.signature_quota, args.limit),
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.summary.parent.mkdir(parents=True, exist_ok=True)

    with args.output.open("w", encoding="utf-8") as handle:
        for candidate in sorted(selected, key=lambda item: item.line_number):
            handle.write(candidate.json_text)
            handle.write("\n")

    summary = build_summary(
        input_path=args.input,
        output_path=args.output,
        rows_scanned=rows_scanned,
        selected=selected,
        bucket_counts=bucket_counts,
        signature_counts=signature_counts,
    )
    args.summary.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    print(
        f"[fixture] wrote {len(selected):,} rows to {args.output} "
        f"and summary to {args.summary}",
        flush=True,
    )


def iter_records(path: Path) -> Iterable[tuple[int, str, dict]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            json_text = raw_line.strip()
            if not json_text:
                continue
            payload = json.loads(json_text)
            if not isinstance(payload, dict):
                continue
            yield line_number, json_text, payload


def build_candidate(line_number: int, json_text: str, payload: dict) -> Candidate:
    word = optional_text(payload.get("word"))
    lang_code = optional_text(payload.get("lang_code"))
    pos = optional_text(payload.get("pos"))

    bucket = f"{lang_code or '_'}::{pos or '_'}"
    feature_signature = compute_feature_signature(payload)
    feature_score = compute_feature_score(payload)
    sample_key = compute_sample_key(
        line_number=line_number,
        word=word,
        lang_code=lang_code,
        pos=pos,
        feature_signature=feature_signature,
    )

    return Candidate(
        line_number=line_number,
        json_text=json_text,
        word=word,
        lang_code=lang_code,
        pos=pos,
        bucket=bucket,
        feature_signature=feature_signature,
        feature_score=feature_score,
        sample_key=sample_key,
    )


def compute_feature_signature(payload: dict) -> str:
    tags: list[str] = []
    if has_non_empty_list(payload.get("senses")):
        tags.append("senses")
    if has_examples(payload):
        tags.append("examples")
    if has_non_empty_list(payload.get("forms")):
        tags.append("forms")
    if has_non_empty_list(payload.get("derived")):
        tags.append("derived")
    if has_non_empty_list(payload.get("related")):
        tags.append("related")
    if has_non_empty_list(payload.get("translations")):
        tags.append("translations")
    if has_non_empty_list(payload.get("sounds")):
        tags.append("sounds")
    if optional_text(payload.get("etymology_text")):
        tags.append("etymology")
    if has_non_empty_list(payload.get("categories")):
        tags.append("categories")
    if not tags:
        return "plain"
    return "+".join(tags)


def compute_feature_score(payload: dict) -> int:
    score = 0
    for key in (
        "senses",
        "forms",
        "derived",
        "related",
        "translations",
        "sounds",
        "categories",
    ):
        value = payload.get(key)
        if isinstance(value, list):
            score += min(len(value), 8)

    if optional_text(payload.get("etymology_text")):
        score += 4
    if has_examples(payload):
        score += 4

    return score


def compute_sample_key(
    *,
    line_number: int,
    word: str | None,
    lang_code: str | None,
    pos: str | None,
    feature_signature: str,
) -> int:
    raw = "|".join(
        (
            str(line_number),
            word or "",
            lang_code or "",
            pos or "",
            feature_signature,
        )
    ).encode("utf-8")
    digest = hashlib.sha1(raw).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


def trim_candidates(candidates: list[Candidate], limit: int) -> None:
    if len(candidates) <= limit * 2:
        return
    candidates.sort(key=lambda item: (item.sample_key, -item.feature_score, item.line_number))
    del candidates[limit:]


def select_candidates(
    *,
    bucket_counts: Counter[str],
    signature_counts: Counter[str],
    candidates_by_bucket: dict[str, list[Candidate]],
    candidates_by_signature: dict[str, list[Candidate]],
    limit: int,
    signature_quota: int,
) -> list[Candidate]:
    selected: list[Candidate] = []
    seen_lines: set[int] = set()

    ordered_signature_buckets = [
        sort_candidates(candidates_by_signature[signature])
        for signature, _count in signature_counts.most_common()
    ]
    ordered_feature_buckets = [
        sort_candidates(candidates_by_bucket[bucket])
        for bucket, _count in bucket_counts.most_common()
    ]

    if signature_quota:
        round_robin_select(
            pools=ordered_signature_buckets,
            target_count=signature_quota,
            selected=selected,
            seen_lines=seen_lines,
        )

    round_robin_select(
        pools=ordered_feature_buckets,
        target_count=limit,
        selected=selected,
        seen_lines=seen_lines,
    )

    return selected[:limit]


def round_robin_select(
    *,
    pools: list[list[Candidate]],
    target_count: int,
    selected: list[Candidate],
    seen_lines: set[int],
) -> None:
    if len(selected) >= target_count:
        return

    pool_indexes = [0 for _ in pools]
    made_progress = True
    while len(selected) < target_count and made_progress:
        made_progress = False
        for pool_id, pool in enumerate(pools):
            while pool_indexes[pool_id] < len(pool):
                candidate = pool[pool_indexes[pool_id]]
                pool_indexes[pool_id] += 1
                if candidate.line_number in seen_lines:
                    continue
                selected.append(candidate)
                seen_lines.add(candidate.line_number)
                made_progress = True
                break
            if len(selected) >= target_count:
                return


def sort_candidates(candidates: list[Candidate]) -> list[Candidate]:
    return sorted(
        candidates,
        key=lambda item: (item.sample_key, -item.feature_score, item.line_number),
    )


def build_summary(
    *,
    input_path: Path,
    output_path: Path,
    rows_scanned: int,
    selected: list[Candidate],
    bucket_counts: Counter[str],
    signature_counts: Counter[str],
) -> dict:
    lang_counts = Counter(candidate.lang_code or "_" for candidate in selected)
    pos_counts = Counter(candidate.pos or "_" for candidate in selected)
    signature_selected_counts = Counter(candidate.feature_signature for candidate in selected)
    bucket_selected_counts = Counter(candidate.bucket for candidate in selected)

    return {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "rows_scanned": rows_scanned,
        "rows_selected": len(selected),
        "total_buckets_seen": len(bucket_counts),
        "total_signatures_seen": len(signature_counts),
        "top_source_buckets": bucket_counts.most_common(25),
        "top_source_signatures": signature_counts.most_common(25),
        "selected_lang_counts": lang_counts.most_common(25),
        "selected_pos_counts": pos_counts.most_common(25),
        "selected_feature_signatures": signature_selected_counts.most_common(25),
        "selected_bucket_counts": bucket_selected_counts.most_common(25),
        "selected_examples": [
            {
                "line_number": candidate.line_number,
                "word": candidate.word,
                "lang_code": candidate.lang_code,
                "pos": candidate.pos,
                "feature_signature": candidate.feature_signature,
            }
            for candidate in sorted(selected[:20], key=lambda item: item.line_number)
        ],
    }


def has_non_empty_list(value: object) -> bool:
    return isinstance(value, list) and len(value) > 0


def has_examples(payload: dict) -> bool:
    senses = payload.get("senses")
    if not isinstance(senses, list):
        return False
    for sense in senses:
        if isinstance(sense, dict) and has_non_empty_list(sense.get("examples")):
            return True
    return False


def optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


if __name__ == "__main__":
    main()
