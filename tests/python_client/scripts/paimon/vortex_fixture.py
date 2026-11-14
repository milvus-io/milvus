"""Offline, lossless physical-format conversion of a Java append-table fixture.

Paimon Java 1.2.0 has no Vortex plugin. Keep its independently read logical GT,
convert each Parquet file with the Python Vortex writer and verify every Arrow
value after reading it back. Rewrite typed Avro metadata (never binary search
and replace), including all transitive file sizes. This is a converted fixture,
not evidence of a Java Vortex writer or a Paimon-Java Vortex read.
"""

import hashlib
import io
import json
import shutil

import fastavro
import pyarrow.parquet as pq
import vortex
import vortex.io as vortex_io


def _avro(data, mutate):
    reader = fastavro.reader(io.BytesIO(data))
    records = list(reader)
    for record in records:
        mutate(record)
    output = io.BytesIO()
    fastavro.writer(
        output, reader.writer_schema, records, codec="deflate", sync_marker=hashlib.sha256(data).digest()[:16]
    )
    return output.getvalue()


def convert_append_fixture(source, destination):
    shutil.copytree(source, destination)
    converted = {}
    for stage in sorted(destination.iterdir()):
        if not (stage / "oracle.json").is_file():
            continue
        table = stage / "table"
        files = {}
        for path in sorted(table.rglob("*.parquet")):
            old = path.read_bytes()
            digest = hashlib.sha256(old).digest()
            target = path.with_suffix(".vortex")
            arrow = pq.read_table(path)
            if digest not in converted:
                vortex_io.write(arrow, str(target))
                # Independent physical roundtrip, including nulls and raw vectors.
                recovered = vortex.open(str(target)).to_arrow().read_all()
                # Vortex emits Arrow view types; cast to the original logical schema.
                assert arrow.equals(recovered.cast(arrow.schema)), f"Vortex physical conversion changed rows: {path}"
                converted[digest] = target.read_bytes()
            else:
                target.write_bytes(converted[digest])
            files[path.name] = (target.name, target.stat().st_size)
            path.unlink()
        for schema_path in (table / "schema").glob("schema-*"):
            schema = json.loads(schema_path.read_bytes())
            assert not schema.get("primaryKeys"), "conversion supports append-only fixtures"
            schema["options"]["file.format"] = "vortex"
            schema_path.write_text(json.dumps(schema), encoding="utf8")

        def update_data(record):
            data = record["_FILE"]
            name, size = files[data["_FILE_NAME"]]
            data["_FILE_NAME"], data["_FILE_SIZE"] = name, size

        sizes = {}
        for path in sorted((table / "manifest").glob("manifest-*")):
            if path.name.startswith("manifest-list-"):
                continue
            rewritten = _avro(path.read_bytes(), update_data)
            path.write_bytes(rewritten)
            sizes[path.name] = len(rewritten)

        def update_manifest(record):
            record["_FILE_SIZE"] = sizes[record["_FILE_NAME"]]

        for path in sorted((table / "manifest").glob("manifest-list-*")):
            rewritten = _avro(path.read_bytes(), update_manifest)
            path.write_bytes(rewritten)
            sizes[path.name] = len(rewritten)
        for path in (table / "snapshot").glob("snapshot-*"):
            snapshot = json.loads(path.read_bytes())
            for key in ["baseManifestList", "deltaManifestList", "changelogManifestList"]:
                if snapshot.get(key):
                    snapshot[key + "Size"] = sizes[snapshot[key]]
            path.write_text(json.dumps(snapshot), encoding="utf8")
        oracle_path = stage / "oracle.json"
        oracle = json.loads(oracle_path.read_bytes())
        oracle["physical_conversion"] = {
            "source": "Java Paimon 1.2.0 append table, Parquet",
            "target": "Vortex 0.56.0",
            "roundtrip": "Arrow table.equals for every physical file",
            "reader_boundary": "logical oracle was read before physical conversion",
        }
        oracle_path.write_text(json.dumps(oracle), encoding="utf8")
