# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deterministic fixtures and an independent, disk-backed SQL oracle."""

import argparse
import hashlib
import itertools
import json
import sqlite3
from pathlib import Path

# Eight filters; output lookup is measured separately for equality only.
# SQLite independently implements the expected scalar/NULL/boolean semantics.
CASES = [
    ("eq", "age == 42", "age = 42"),
    ("in", "age in [5, 42, 97]", "age IN (5, 42, 97)"),
    ("range", "age >= 40 and age < 45", "age >= 40 AND age < 45"),
    ("prefix", 'name like "hot%"', "name LIKE 'hot%'"),
    ("null", "age is null", "age IS NULL"),
    ("arithmetic", "age % 7 == 0 and age < 10", "age % 7 = 0 AND age < 10"),
    ("suffix", 'name like "%tail"', "name LIKE '%tail'"),
    (
        "logical",
        '(age == 42 or name like "hot%") and not (age is null)',
        "(age = 42 OR name LIKE 'hot%') AND NOT (age IS NULL)",
    ),
]


def records(path):
    with Path(path).open() as source:
        for line in source:
            yield json.loads(line)


def dump_line(stream, item):
    stream.write(json.dumps(item, separators=(",", ":"), allow_nan=False) + "\n")


def vector(pk):
    return [((pk * 17 + dim * 13) % 101) / 101 for dim in range(8)]


def base_values(pk):
    return (
        None if pk % 101 == 0 else pk % 100,
        None if pk % 103 == 0 else f"user_{pk % 1000:04d}".ljust(32, "x"),
    )


def generate(directory, rows):
    """Write initial data, logical updates, mirror data and expected result files.

    20% of rows get three versions. Updates include entry/exit from matches,
    NULL transitions and restoration of an earlier value. No server timestamp
    or field ID is baked into this reproducible workload.
    """
    if rows < 100:
        raise ValueError("Use at least 100 rows to exercise every update category")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    with (directory / "base.jsonl").open("w") as out:
        for pk in range(rows):
            age, name = base_values(pk)
            dump_line(out, {"pk": pk, "age": age, "name": name, "vector": vector(pk)})

    operations = 0
    with (directory / "updates.jsonl").open("w") as out:
        for turn in range(3):
            for targets in itertools.batched(range(0, rows, 5), 256):
                groups = {}
                for pk in targets:
                    if turn == 0:
                        age, name = 42, "hot_first".ljust(32, "x")
                    elif turn == 1:
                        age, name = None, None
                    else:
                        category = (pk // 5) % 4
                        if category == 0:
                            age, name = base_values(pk)
                        else:
                            age, name = [
                                (None, None),
                                (42, None if (pk // 20) % 2 else "hot_final".ljust(32, "x")),
                                (7, "hot_" + "x" * 24 + "tail"),
                                (None, None),
                            ][category]
                    groups.setdefault((age, name), []).append(pk)
                for (age, name), pks in groups.items():
                    operations += 1
                    dump_line(
                        out, {"sequence": operations, "primary_keys": pks, "assignments": {"age": age, "name": name}}
                    )

    # Build the mirror from the written input and updates, not from the final
    # state formula used to generate updates. SQLite is independent of segcore.
    with sqlite3.connect(directory / "oracle.sqlite") as db:
        db.execute("CREATE TABLE rows (pk INTEGER PRIMARY KEY, age INTEGER, name TEXT)")
        db.executemany(
            "INSERT INTO rows VALUES (?, ?, ?)",
            ((row["pk"], row["age"], row["name"]) for row in records(directory / "base.jsonl")),
        )
        for op in records(directory / "updates.jsonl"):
            for pk in op["primary_keys"]:
                for field, value in op["assignments"].items():
                    if field not in ("age", "name"):
                        raise ValueError(f"Unexpected field: {field}")
                    db.execute(f"UPDATE rows SET {field} = ? WHERE pk = ?", (value, pk))
        with (directory / "mirror.jsonl").open("w") as out:
            for pk, age, name in db.execute("SELECT pk, age, name FROM rows ORDER BY pk"):
                dump_line(out, {"pk": pk, "age": age, "name": name, "vector": vector(pk)})
        expected = directory / "expected"
        expected.mkdir()
        counts = {}
        for case, _, sql in CASES:
            count = 0
            with (expected / f"{case}.jsonl").open("w") as out:
                for pk, age, name in db.execute(f"SELECT pk, age, name FROM rows WHERE {sql} ORDER BY pk"):
                    dump_line(out, {"pk": pk, "age": age, "name": name})
                    count += 1
            counts[case] = count
    manifest = {
        "rows": rows,
        "operations": operations,
        "row_updates": 3 * len(range(0, rows, 5)),
        "dim": 8,
        "counts": counts,
    }
    manifest["sha256"] = {}
    for path in sorted(directory.glob("*.jsonl")) + sorted(expected.glob("*.jsonl")):
        manifest["sha256"][str(path.relative_to(directory))] = digest(path)
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def validate_fixture(directory):
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    for name, expected in manifest["sha256"].items():
        if digest(directory / name) != expected:
            raise ValueError(f"Fixture changed after generation: {name}")
    return manifest


def bind_updates(directory, destination, collection_id, field_ids, base_ts):
    """Translate logical operations to the phase-4 JSONL protocol atomically."""
    directory, destination = Path(directory), Path(destination)
    manifest = validate_fixture(directory)
    temporary = destination.with_suffix(".tmp")
    with temporary.open("w") as out:
        dump_line(
            out,
            {
                "collection_id": collection_id,
                "row_count": manifest["rows"],
                "operation_count": manifest["operations"],
                "field_ids": [field_ids["age"], field_ids["name"]],
            },
        )
        for op in records(directory / "updates.jsonl"):
            dump_line(
                out,
                {
                    "timestamp": base_ts + op["sequence"],
                    "primary_keys": op["primary_keys"],
                    "assignments": [{"field_id": field_ids[f], "value": v} for f, v in op["assignments"].items()],
                },
            )
    temporary.replace(destination)
    return base_ts + manifest["operations"]


def batches(path, size):
    source = records(path)
    while batch := list(itertools.islice(source, size)):
        yield batch


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--rows", type=int, default=2000)
    args = parser.parse_args()
    print(json.dumps(generate(args.directory, args.rows), indent=2))
