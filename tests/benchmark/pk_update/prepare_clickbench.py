#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file in the project root for terms.

"""Prepare a ClickBench sealed-segment fixture and a forward-replay oracle."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import random

import pyarrow.parquet as pq


SOURCE_SHA256 = "fa134fe101e68324e0de851146fda69624f5cbb707d387141d1c2a88a219a16d"
ROWS = 1_000_000
SEED = 20261118
LIMIT = 1000
FIRST_UPDATE = 100


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def write_lines(path, records):
    with path.open("x", encoding="utf-8") as stream:
        for item in records:
            stream.write(json.dumps(item, separators=(",", ":")) + "\n")


def write_json(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def read_rows(path):
    rows = []
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=65536, columns=["ClientIP", "URL"]
    ):
        for ip, url in zip(batch.column(0).to_pylist(), batch.column(1).to_pylist()):
            if isinstance(url, bytes):
                url = url.decode("utf-8")
            if (
                not isinstance(ip, int)
                or not -(2**63) <= ip < 2**63
                or not isinstance(url, str)
                or len(url.encode("utf-8")) > 65535
            ):
                raise ValueError("Source row is outside the non-null INT64/VARCHAR schema")
            rows.append((ip, url))
            if len(rows) == ROWS:
                return rows
    raise ValueError(f"Need {ROWS} source rows, found {len(rows)}")


def query_parameters(rows):
    ordered_ips = sorted(ip for ip, _ in rows)
    frequencies = Counter(url for _, url in rows)
    rng = random.Random(SEED + 1)
    parameters = []
    used_urls = set()
    for start in range(0, 40, 8):
        lower = ordered_ips[ROWS * start // 100]
        upper = ordered_ips[ROWS * (start + 60) // 100]
        in_range = Counter(url for ip, url in rows if lower <= ip < upper)
        # Use distinct real URL values with enough intersection hits to exercise
        # both leaves, while excluding the hottest URL values.
        eligible = sorted(
            url
            for url, count in in_range.items()
            if url and count >= 8 and frequencies[url] <= 3000 and url not in used_urls
        )
        chosen = rng.sample(eligible, 40)
        used_urls.update(chosen)
        for index in range(0, len(chosen), 2):
            parameters.append(
                {
                    "lower": lower,
                    "upper": upper,
                    "urls": chosen[index : index + 2],
                }
            )
    rng.shuffle(parameters)
    return parameters


def dispatch_updates(rows, targets, rounds):
    rng = random.Random(SEED + 2)
    current = {offset: rows[offset] for offset in targets}
    for round_id in range(rounds):
        for ordinal, offset in enumerate(targets):
            old_ip, old_url = current[offset]
            while True:
                ip, url = rows[rng.randrange(ROWS)]
                if ip != old_ip and url != old_url:
                    break
            current[offset] = (ip, url)
            yield offset, FIRST_UPDATE + round_id, ordinal, ip, url


def snapshot(rows, parameters):
    by_url = {url: [] for param in parameters for url in param["urls"]}
    for offset, (ip, url) in enumerate(rows):
        if url in by_url:
            by_url[url].append((offset, ip, url))
    queries = []
    for p in parameters:
        matching_urls = [row for url in p["urls"] for row in by_url[url]]
        hits = sorted(row for row in matching_urls if p["lower"] <= row[1] < p["upper"])
        if not hits or len(matching_urls) == ROWS:
            raise ValueError("Degenerate conjunctive query")
        queries.append({"url_matches": len(matching_urls), "total": len(hits), "rows": hits[:LIMIT]})
    return queries


def prepare(source, output, percent, rounds):
    source_sha = digest(source)
    if source_sha != SOURCE_SHA256:
        raise ValueError(f"Unexpected ClickBench source SHA-256 {source_sha}")
    rows = read_rows(source)
    parameters = query_parameters(rows)
    rng = random.Random(SEED)
    targets = rng.sample(range(ROWS), ROWS * 5 // 100)[: ROWS * percent // 100]

    # Generate mutations separately from the model that consumes them. The oracle
    # forward-replays all assignments at each tick. It uses no production index,
    # bitmaps, reverse traversal, expression tree, or storage representation.
    updates = list(dispatch_updates(rows, targets, rounds))
    oracle = list(rows)
    truth = {str(FIRST_UPDATE - 1): snapshot(oracle, parameters)}
    for round_id in range(rounds):
        for offset, _, _, ip, url in updates[round_id * len(targets) : (round_id + 1) * len(targets)]:
            oracle[offset] = (ip, url)
        truth[str(FIRST_UPDATE + round_id)] = snapshot(oracle, parameters)

    # PK timing samples both visible-delta and native-base fallback rows.
    keys = rng.sample(targets, 50)
    target_set = set(targets)
    keys.extend(rng.sample([offset for offset in range(ROWS) if offset not in target_set], 50))
    rng.shuffle(keys)
    output.mkdir(parents=True)
    write_lines(output / "base.jsonl", rows)
    write_lines(output / "mirror.jsonl", oracle)
    write_lines(output / "updates.jsonl", updates)
    write_json(
        output / "oracle.json",
        {
            "parameters": parameters,
            "snapshots": truth,
            "equality": [[key, *oracle[key]] for key in keys],
        },
    )
    update_frequencies = [Counter(update[column] for update in updates) for column in (3, 4)]
    write_json(
        output / "manifest.json",
        {
            "source_catalog": "https://github.com/ClickHouse/ClickBench",
            "source_path": str(source.resolve()),
            "source_sha256": source_sha,
            "seed": SEED,
            "columns": ["ClientIP", "URL"],
            "rows": ROWS,
            "update_percent": percent,
            "updated_rows": len(targets),
            "update_rounds": rounds,
            "update_rows": len(updates),
            "update_field_entries": len(updates) * 2,
            "update_value_distinct": [len(counts) for counts in update_frequencies],
            "max_updates_per_value": [max(counts.values()) for counts in update_frequencies],
            "query_parameter_count": len(parameters),
            "limit": LIMIT,
            "query_ts": FIRST_UPDATE + rounds - 1,
            "sha256": {
                name: digest(output / name)
                for name in ("base.jsonl", "mirror.jsonl", "updates.jsonl", "oracle.json")
            },
        },
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--percent", required=True, type=int, choices=(1, 5))
    parser.add_argument("--rounds", required=True, type=int, choices=(1, 3))
    args = parser.parse_args()
    if args.percent == 1 and args.rounds != 1:
        parser.error("the 1% fixture uses one round")
    if args.output_dir.exists():
        parser.error("use a new output directory to preserve existing fixtures")
    prepare(args.parquet, args.output_dir, args.percent, args.rounds)


if __name__ == "__main__":
    main()
