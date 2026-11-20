"""Independent text fixtures and assertions for public Grep acceptance tests.

The literal oracle deliberately does not parse Grep options or reuse a regex
engine. Regex-specific cases supply independently reviewed spans instead.
"""

import random
from numbers import Integral, Real


def source_row(pk, content, repo="demo", path=None, revision=None):
    return {
        "id": pk,
        "content": content,
        "repo": repo,
        "path": path or f"src/{pk}.py",
        "revision": revision or f"revision_{pk}",
        "vector": [0.25, 0.5, 0.75, 1.0],
    }


def basic_rows():
    """D0 from the reviewed design; NULL and empty string are distinct."""
    texts = [
        "error timeout",
        "ERROR timeout",
        "terror",
        "error",
        "info\nerror timeout\nend",
        "中文 error 错误",
        "",
        None,
        "a.b",
        "info only",
        "error error\nerror",
        "x\r\nerror\r\n",
        "\n\n",
        "é中\n",
        "aXb",
    ]
    return [source_row(i, text) for i, text in enumerate(texts, 1)]


def code_rows():
    return [
        source_row(101, "legacy_call()", path="docs/a.py"),
        source_row(102, "legacy_call()", repo="other", path="src/a.py"),
        source_row(103, "legacy_call()", path="src/a.py"),
        source_row(104, "modern_call()", path="src/b.py"),
        source_row(105, "modern_call()", path="src/a.py"),
    ]


def sparse_rows():
    matches = {128, 255, 256, 383, 384}
    rows = [source_row(i, f"row {i}\nerror marker" if i in matches else f"plain {i}") for i in range(388)]
    # The approved D1 explicitly requests reproducible insertion shuffling.
    # This local RNG never modifies process-global randomness or claims scan order.
    random.Random(20261111).shuffle(rows)
    return rows


def semantics_rows():
    """Disjoint ID ranges let read-only semantic cases safely share a collection."""
    groups = {
        101: ["école", "σ", "ς", "ж", "错误 école ж", "STRASSE", "e\u0301", "É é"],
        201: [
            "中文🙂\n中文 error error\nend",
            "🙂error e\u0301 error",
            "error\nerror",
            "x\r\nerror\r\n",
            "header\ninfo\nerror then error",
            "\ufefferror\x00\terror",
            "\n\nL03 error\n\n\n\nL07 error\n\n\nL10 error\n\n\n",
        ],
        301: [None, "", "\n", "\n\n", "é中\n", "aé"],
        401: [
            "abc",
            "def",
            "hello\nERROR disk",
            "foo\nbar",
            "中文",
            "中文error",
            "Ab",
            "AB",
            "error\nerror",
            "ERROR network",
            "hello\nINFO disk",
        ],
        501: ["-C", r"a\b", "foobar", "error timeout"],
    }
    rows = basic_rows()
    for start, values in groups.items():
        rows.extend(source_row(start + i, text) for i, text in enumerate(values))
    return rows


def projected_rows():
    rows = [
        source_row(101, "header\nerror\nfooter", repo="a"),
        source_row(202, "中文 error", repo="a"),
        source_row(303, "info only", repo="b"),
    ]
    for i, row in enumerate(rows):
        row["vector"] = [i + 0.25, i + 0.5, i + 0.75, i + 1.0]
        row.update(
            {
                "meta": {"text": "error", "pk": row["id"]},
                "tags": ["error", str(row["id"])],
                "score": None if i == 0 else i,
                "parts": [{"text": "error", "rank": i + 1}],
                "dynamic_text": "error",
            }
        )
    return rows


def logical_lines(text):
    """Yield (one-based number, UTF-8 offset, content), splitting only on LF."""
    if not text:
        return
    parts = text.split("\n")
    if text.endswith("\n"):
        parts.pop()
    offset = 0
    for number, line in enumerate(parts, 1):
        yield number, offset, line
        offset += len(line.encode("utf-8")) + 1


def detail(line, number, offset, spans):
    return {
        "content": line,
        "line_number": number,
        "line_start_offset": offset,
        "starts": [start for start, _ in spans],
        "ends": [end for _, end in spans],
    }


def literal_details(text, pattern):
    """Non-overlapping literal matches, including UTF-8 boundary empty spans."""
    needle = pattern.encode("utf-8")
    result = []
    for number, offset, line in logical_lines(text):
        raw = line.encode("utf-8")
        spans = []
        if not needle:
            boundaries = [0]
            for char in line:
                boundaries.append(boundaries[-1] + len(char.encode("utf-8")))
            spans = [(i, i) for i in boundaries]
        else:
            position = 0
            while (start := raw.find(needle, position)) >= 0:
                position = start + len(needle)
                spans.append((start, position))
        if spans:
            result.append(detail(line, number, offset, spans))
    return result


def expected_literal(rows, pattern, predicate=lambda row: True):
    return {row["id"]: hits for row in rows if predicate(row) and (hits := literal_details(row["content"], pattern))}


def assert_typed_value(actual, expected):
    """Reject bool/int and float/int coercion that Python equality would hide."""
    if isinstance(expected, dict):
        assert isinstance(actual, dict) and set(actual) == set(expected)
        for key, value in expected.items():
            assert_typed_value(actual[key], value)
    elif isinstance(expected, list):
        assert isinstance(actual, list) and len(actual) == len(expected)
        for value, wanted in zip(actual, expected, strict=True):
            assert_typed_value(value, wanted)
    elif isinstance(expected, float):
        assert isinstance(actual, Real) and not isinstance(actual, Integral), (actual, type(actual))
        assert actual == expected
    else:
        assert type(actual) is type(expected), (actual, type(actual), expected, type(expected))
        assert actual == expected


def assert_results(results, rows, expected, fields, *, count=None, match_field="matches"):
    """Check every row, every span and exact output projection; ignore row order.

    count=None requires the entire independent hit set. A finite count checks
    an unordered page without assuming which eligible IDs a server chooses.
    """
    materialized = [dict(row) for row in results]
    by_id = {row["id"]: row for row in materialized}
    assert len(by_id) == len(materialized), "duplicate primary keys in one response"
    if count is None:
        assert set(by_id) == set(expected), (set(by_id), set(expected))
    else:
        assert len(materialized) == count, (len(materialized), count)
        assert set(by_id) <= set(expected), (set(by_id), set(expected))
    source = {row["id"]: row for row in rows}
    for pk, actual in by_id.items():
        wanted_fields = set(fields) | {"id"}
        if match_field is not None:
            wanted_fields.add(match_field)
        assert set(actual) == wanted_fields, (pk, set(actual), wanted_fields)
        for field in set(fields) | {"id"}:
            assert_typed_value(actual[field], source[pk][field])
        if match_field is None:
            continue
        hits = actual[match_field]
        assert isinstance(hits, list), (pk, "SDK must decode JSON details", type(hits))
        assert hits == expected[pk], (pk, hits, expected[pk])
        assert_typed_value(hits, expected[pk])
        lines = {number: (offset, line) for number, offset, line in logical_lines(source[pk]["content"])}
        field_bytes = source[pk]["content"].encode("utf-8")
        for hit in hits:
            offset, line = lines[hit["line_number"]]
            assert hit["content"] == line
            assert hit["line_start_offset"] == offset
            raw = line.encode("utf-8")
            assert len(hit["starts"]) == len(hit["ends"]) > 0
            previous_end = 0
            for start, end in zip(hit["starts"], hit["ends"], strict=True):
                assert previous_end <= start <= end <= len(raw)
                # Decoding prefixes checks boundaries, even for empty spans.
                raw[:start].decode("utf-8")
                raw[:end].decode("utf-8")
                assert raw[start:end] == field_bytes[offset + start : offset + end]
                previous_end = end
    return by_id
