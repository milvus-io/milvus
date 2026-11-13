"""Validated, relocatable Paimon data and independent batch-reader ground truth.

The ordinary pytest runner needs no Java, native storage library or source writer.
Packages are generated separately by scripts/paimon/PaimonFixtures.java.
"""

import base64
import hashlib
import io
import json
import math
import re
import time
import zipfile
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from urllib.parse import quote

import numpy as np

DEFAULT_BUNDLE = Path(__file__).resolve().parents[1] / "assets" / "paimon" / "fixtures.zip"
_DIGEST = re.compile(r"[a-f0-9]{64}\Z")
_MAX_BLOB = 64 * 1024 * 1024
_MAX_PACKAGE = 256 * 1024 * 1024
_MUTABLE_HINTS = {"snapshot/LATEST", "snapshot/EARLIEST", "schema/LATEST"}


def _safe_path(value):
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError(f"invalid fixture path: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in ("", ".", "..") for part in value.split("/")):
        raise ValueError(f"unsafe fixture path: {value!r}")
    return value


def snapshot_expectation(value, available):
    """Classify the Go-parser/property/planner boundaries without conflating them."""
    if value is None:
        return "latest"
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("snapshot_id must be an int64 or decimal string")
    if isinstance(value, str) and not re.fullmatch(r"[+-]?\d+", value):
        raise ValueError("snapshot_id must be a decimal string")
    parsed = int(value)
    if not -(2**63) <= parsed < 2**63:
        raise ValueError("snapshot_id outside int64")
    if parsed == -1:
        return "latest"
    if parsed < -1:
        return "invalid-property"
    return "pinned" if parsed in available else "missing"


class FixtureBundle:
    """Read and validate an entire content-addressed package before uploading."""

    def __init__(self, path=DEFAULT_BUNDLE):
        self.path = Path(path)
        if not self.path.is_file():
            raise FileNotFoundError(f"Paimon fixture package missing: {self.path}; see scripts/paimon/README.md")
        self.blobs = {}
        with zipfile.ZipFile(self.path) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)) or "manifest.json" not in names:
                raise ValueError("duplicate entries or missing fixture manifest")
            if sum(entry.file_size for entry in archive.infolist()) > _MAX_PACKAGE:
                raise ValueError("fixture package exceeds size limit")
            for entry in archive.infolist():
                if entry.file_size > _MAX_BLOB:
                    raise ValueError("fixture blob exceeds size limit")
                if entry.filename == "manifest.json":
                    continue
                if not entry.filename.startswith("blobs/") or not _DIGEST.fullmatch(entry.filename[6:]):
                    raise ValueError("unsafe fixture archive path")
                digest = entry.filename[6:]
                data = archive.read(entry)
                if hashlib.sha256(data).hexdigest() != digest:
                    raise ValueError(f"fixture digest mismatch: {digest}")
                self.blobs[digest] = data
            self.manifest = json.loads(archive.read("manifest.json"))
        if self.manifest.get("version") != 1 or self.manifest.get("producer") != "PaimonFixtures.java":
            raise ValueError("unsupported fixture manifest or producer")
        self.fixtures = self.manifest["fixtures"]
        self._oracles = {}
        normalized_rows = {}
        for name, fixture in self.fixtures.items():
            _safe_path(name)
            for version, metadata in fixture["versions"].items():
                _safe_path(version)
                snapshot = metadata["snapshot_id"]
                if isinstance(snapshot, bool) or not isinstance(snapshot, int) or snapshot < 0:
                    raise ValueError("fixture snapshot must be a nonnegative integer")
                for relative, digest in metadata["files"].items():
                    _safe_path(relative)
                    self.blob(digest)
                snapshot_path = f"snapshot/snapshot-{snapshot}"
                if snapshot_path not in metadata["files"]:
                    raise ValueError("source snapshot missing from fixture files")
                source_snapshot = json.loads(self.blob(metadata["files"][snapshot_path]))
                if source_snapshot.get("id") != snapshot:
                    raise ValueError("source snapshot ID does not match manifest")
                oracle = json.loads(self.blob(metadata["oracle"]))
                if oracle.get("reader") != "Paimon Java 1.2.0 pinned batch reader":
                    raise ValueError("fixture has no independent pinned batch-reader oracle")
                if oracle.get("snapshot_id") != snapshot:
                    raise ValueError("oracle snapshot does not match source snapshot")
                # Equal content-addressed GT is immutable and need only be decoded
                # once per worker. Still validate every snapshot/provenance above.
                rows_digest = oracle.get("rows_blob", metadata["oracle"])
                self.blob(rows_digest)
                if rows_digest in normalized_rows:
                    self._oracles[name, version] = normalized_rows[rows_digest]
                    continue
                raw_rows = json.loads(self.blob(oracle["rows_blob"])) if "rows_blob" in oracle else oracle["rows"]
                rows = _decode_oracle(raw_rows)
                for row in rows:
                    if "geo" in row:
                        row["geo"] = _output_value("geo", row["geo"])
                    if "json_value" in row and row["json_value"] is not None:
                        row["json_value"] = json.loads(row["json_value"])
                    if "int8_vec" in row and row["int8_vec"] is not None:
                        row["int8_vec"] = np.frombuffer(row["int8_vec"], dtype=np.int8).tolist()
                    if "half_vec" in row and row["half_vec"] is not None:
                        row["half_vec"] = np.frombuffer(row["half_vec"], dtype="<f2").astype(float).tolist()
                    if "bf16_vec" in row and row["bf16_vec"] is not None:
                        bits = np.frombuffer(row["bf16_vec"], dtype="<u2").astype(np.uint32) << 16
                        row["bf16_vec"] = bits.view(np.float32).astype(float).tolist()
                    if "sparse" in row and row["sparse"] is not None:
                        row["sparse"] = {int(key): value for key, value in row["sparse"].items()}
                self._oracles[name, version] = PaimonOracle(rows)
                normalized_rows[rows_digest] = self._oracles[name, version]

    def blob(self, digest):
        if not isinstance(digest, str) or not _DIGEST.fullmatch(digest) or digest not in self.blobs:
            raise ValueError("missing or invalid fixture blob digest")
        return self.blobs[digest]

    def metadata(self, name, version):
        return self.fixtures[name]["versions"][version]

    def oracle(self, name, version):
        return self._oracles[name, version]


class PaimonSource:
    """Publish only validated dependencies, then snapshots, then mutable hints."""

    def __init__(self, bundle, name, minio_client, config, prefix):
        self.prefix = _safe_path(prefix)
        self.bundle = bundle
        self.name = name
        self.client = minio_client
        self.config = dict(config)
        self.fixture = bundle.fixtures[name]
        self._published = {}
        self._snapshot = -1
        self.version = None

    @property
    def source(self):
        address = self.config.get("external_source_address", self.config["address"])
        # Paimon receives a table root, not an object-listing prefix. The pinned
        # reader appends /snapshot itself and rejects a doubled path separator.
        return f"s3://{address}/{self.config['bucket']}/{quote(self.prefix, safe='/')}"

    @property
    def fields(self):
        return self.fixture["fields"]

    @property
    def dimension(self):
        return self.fixture["dimension"]

    def snapshot(self, version):
        return self.bundle.metadata(self.name, version)["snapshot_id"]

    def oracle(self, version=None):
        return self.bundle.oracle(self.name, version or self.version)

    def spec(self, version=None, scan_mode="auto", **extra):
        from common.external_table_common import build_external_spec

        params = {"scan_mode": scan_mode}
        if version is not None:
            params["snapshot_id"] = str(self.snapshot(version))
        params.update(extra)
        return build_external_spec(self.config, fmt="paimon-table", **params)

    def publish(self, version):
        metadata = self.bundle.metadata(self.name, version)
        if metadata["snapshot_id"] < self._snapshot:
            raise ValueError("cannot publish an earlier snapshot over a later commit")
        files = metadata["files"]
        # Validate every immutable destination before performing any writes.
        for relative, digest in files.items():
            previous = self._published.get(relative)
            if previous is not None and previous != digest and relative not in _MUTABLE_HINTS:
                raise ValueError(f"refusing to overwrite immutable source path: {relative}")

        def order(relative):
            if relative in _MUTABLE_HINTS:
                return (2, 0, relative)
            if re.fullmatch(r"snapshot/snapshot-\d+", relative):
                return (1, int(relative.rsplit("-", 1)[1]), relative)
            return (0, 0, relative)

        for relative in sorted(files, key=order):
            digest = files[relative]
            if self._published.get(relative) == digest:
                continue
            data = self.bundle.blob(digest)
            if re.fullmatch(r"snapshot/snapshot-\d+", relative):
                # A PUT can commit remotely and then lose its response. Prevent
                # rollback conservatively once a commit publication is attempted.
                self._snapshot = max(self._snapshot, int(relative.rsplit("-", 1)[1]))
            self.client.put_object(self.config["bucket"], f"{self.prefix}/{relative}", io.BytesIO(data), len(data))
            self._published[relative] = digest
        self._snapshot = metadata["snapshot_id"]
        self.version = version

    def delete_owned(self, relative):
        relative = _safe_path(relative)
        if relative not in self._published:
            raise ValueError("object does not belong to this source publication")
        self.client.remove_object(self.config["bucket"], f"{self.prefix}/{relative}")
        del self._published[relative]


def _decode_oracle(value):
    if isinstance(value, list):
        return [_decode_oracle(item) for item in value]
    if isinstance(value, dict):
        if set(value) == {"__bytes__"}:
            return base64.b64decode(value["__bytes__"], validate=True)
        if set(value) == {"__timestamp_micros__"}:
            return int(value["__timestamp_micros__"])
        return {key: _decode_oracle(item) for key, item in value.items()}
    return value


def _same_value(actual, expected):
    if expected is None:
        return actual is None
    if isinstance(expected, dict):
        return (
            isinstance(actual, dict)
            and set(actual) == set(expected)
            and all(_same_value(actual[key], value) for key, value in expected.items())
        )
    if isinstance(expected, (list, tuple)):
        return (
            isinstance(actual, (list, tuple, np.ndarray))
            and len(actual) == len(expected)
            and all(_same_value(a, b) for a, b in zip(actual, expected, strict=True))
        )
    if isinstance(expected, float):
        return isinstance(actual, (int, float, np.number)) and math.isclose(
            actual, expected, rel_tol=1e-5, abs_tol=1e-6
        )
    return actual == expected


def _output_value(field, value):
    """Normalize public SDK representations without changing logical values."""
    if value is None:
        return None
    if field == "geo" and isinstance(value, str):
        number = r"([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)"
        match = re.fullmatch(r"POINT\s*\(\s*" + number + r"\s+" + number + r"\s*\)", value)
        if not match:
            raise ValueError(f"fixture expects a 2D WKT POINT, got {value!r}")
        return [float(match[1]), float(match[2])]
    if field in {"half_vec", "bf16_vec", "int8_vec", "binary_vec", "mh"}:
        if isinstance(value, list) and len(value) == 1 and isinstance(value[0], bytes):
            value = value[0]
    if field == "ts" and isinstance(value, str):
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            raise ValueError("TIMESTAMPTZ output must include a timezone")
        delta = timestamp.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
        return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
    if field in {"half_vec", "bf16_vec", "int8_vec"} and isinstance(value, bytes):
        if field == "half_vec":
            return np.frombuffer(value, dtype="<f2").astype(float).tolist()
        if field == "bf16_vec":
            bits = np.frombuffer(value, dtype="<u2").astype(np.uint32) << 16
            return bits.view(np.float32).astype(float).tolist()
        return np.frombuffer(value, dtype=np.int8).tolist()
    return value


class PaimonOracle:
    """Compare complete row multisets and exact/ANN search against source rows."""

    def __init__(self, rows):
        self.rows = rows

    def filtered(self, predicate):
        return PaimonOracle([row for row in self.rows if predicate(row)])

    def assert_rows(self, actual, fields=None):
        fields = tuple(fields or (self.rows[0].keys() if self.rows else []))
        expected_counts = Counter(row["id"] for row in self.rows)
        assert Counter(row["id"] for row in actual) == expected_counts, "business-row multiset mismatch"
        candidates = defaultdict(list)
        for row in actual:
            candidates[row["id"]].append(row)
        for expected in self.rows:
            group = candidates[expected["id"]]
            for index, row in enumerate(group):
                if all(
                    field in row and _same_value(_output_value(field, row[field]), expected[field]) for field in fields
                ):
                    group.pop(index)
                    break
            else:
                raise AssertionError(f"source row/field mismatch for business id={expected['id']}")

    def topk(self, query, metric, limit, field="embedding"):
        rows = [row for row in self.rows if row.get(field) is not None]
        if isinstance(query, dict):
            rows = [
                row for row in rows if any(value != 0 and row[field].get(key, 0) != 0 for key, value in query.items())
            ]
        if not rows:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        if metric in {"HAMMING", "JACCARD"}:
            matrix = np.stack([np.unpackbits(np.frombuffer(row[field], dtype=np.uint8)) for row in rows])
            query = np.unpackbits(np.frombuffer(query, dtype=np.uint8))
            if metric == "HAMMING":
                distances = (matrix != query).sum(axis=1).astype(float)
            else:
                union = np.logical_or(matrix, query).sum(axis=1)
                intersection = np.logical_and(matrix, query).sum(axis=1)
                distances = 1 - np.divide(intersection, union, out=np.ones(len(rows)), where=union != 0)
            order = np.argsort(distances, kind="stable")
        elif isinstance(query, dict) and metric == "IP":
            distances = np.array([sum(row[field].get(key, 0) * value for key, value in query.items()) for row in rows])
            order = np.argsort(-distances, kind="stable")
        else:
            matrix = np.asarray([row[field] for row in rows], dtype=np.float64)
            query = np.asarray(query, dtype=np.float64)
        if metric == "L2":
            distances = np.square(matrix - query).sum(axis=1)
            order = np.argsort(distances, kind="stable")
        elif metric in ("IP", "COSINE") and not isinstance(query, dict):
            distances = matrix @ query
            if metric == "COSINE":
                denominators = np.linalg.norm(matrix, axis=1) * np.linalg.norm(query)
                if np.any(denominators == 0):
                    raise ValueError("COSINE ground truth requires nonzero vectors")
                distances /= denominators
            order = np.argsort(-distances, kind="stable")
        elif metric not in {"HAMMING", "JACCARD", "IP"}:
            raise ValueError(f"unsupported oracle metric {metric}")
        selected = order[:limit]
        return np.asarray([rows[index]["id"] for index in selected]), distances[selected]

    def assert_search(self, results, queries, metric, limit, *, output_fields, field="embedding", min_recall=1.0):
        assert len(results) == len(queries), "search nq mismatch"
        recalls = []
        for hits, query in zip(results, queries, strict=True):
            all_ids, all_scores = self.topk(query, metric, len(self.rows), field)
            ids, scores = all_ids[:limit], all_scores[:limit]
            business_ids = [hit["entity"]["id"] for hit in hits]
            assert len(hits) == len(ids), "search limit mismatch"
            assert len(set(business_ids)) == len(business_ids), "duplicate search result"
            if len(ids):
                descending = metric in {"IP", "COSINE"}
                cutoff = scores[-1]
                strict = {
                    int(row_id)
                    for row_id, score in zip(all_ids, all_scores, strict=True)
                    if (score > cutoff + 1e-6 if descending else score < cutoff - 1e-6)
                }
                tied = {
                    int(row_id)
                    for row_id, score in zip(all_ids, all_scores, strict=True)
                    if abs(score - cutoff) <= 1e-6
                }
                actual = set(business_ids)
                credit = len(strict & actual) + min(len(tied & actual), len(ids) - len(strict))
                recalls.append(credit / len(ids))
            actual_scores = [hit["distance"] for hit in hits]
            assert actual_scores == sorted(actual_scores, reverse=metric in {"IP", "COSINE"}), "distance ordering"
            for hit in hits:
                assert set(output_fields) <= hit["entity"].keys(), "missing requested search output fields"
                matching = self.filtered(lambda row: row["id"] == hit["entity"]["id"])
                assert matching.rows, "search returned business id absent from source snapshot"
                _, expected_score = matching.topk(query, metric, 1, field)
                np.testing.assert_allclose(hit["distance"], expected_score[0], rtol=1e-4, atol=1e-5)
                expected = matching.rows[0]
                for output, value in hit["entity"].items():
                    if output in expected:
                        assert _same_value(_output_value(output, value), expected[output]), (
                            f"stale search output {output}"
                        )
        assert not recalls or float(np.mean(recalls)) >= min_recall, f"recall below target: {recalls}"


def progress_field(progress, name, default=None):
    return progress.get(name, default) if isinstance(progress, dict) else getattr(progress, name, default)


def wait_refresh(client, job_id, timeout=180, expected="RefreshCompleted"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.get_refresh_external_collection_progress(job_id=job_id, timeout=30)
        state = progress_field(last, "state")
        if state in ("RefreshCompleted", "RefreshFailed"):
            allowed = expected if isinstance(expected, tuple) else (expected,)
            assert state in allowed, (
                f"refresh {job_id}: expected {expected}, got {state}: {progress_field(last, 'reason')}"
            )
            if state == "RefreshFailed":
                assert progress_field(last, "reason"), "failed refresh without reason"
            return last
        time.sleep(1)
    raise AssertionError(f"refresh {job_id} did not terminate in {timeout}s; last={last}")


def wait_actual_index(client, collection, index_name, expected_rows, timeout=180):
    """Require public row statistics, not only a Finished/create-index response."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.describe_index(collection_name=collection, index_name=index_name, timeout=30)
        if int(last.get("indexed_rows", -1)) == expected_rows and int(last.get("pending_index_rows", 0)) == 0:
            return last
        time.sleep(1)
    raise AssertionError(f"index coverage never reached {expected_rows} rows: {last}")
