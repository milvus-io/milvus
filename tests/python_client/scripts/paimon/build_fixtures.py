"""Build real Paimon tables and package their independent Java-reader oracles.

Offline preparation only. Run with JDK17 and build-requirements.txt installed;
the generated zip is all the ordinary Python E2E runner consumes.
"""

import argparse
import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile
from pathlib import Path

ARTIFACTS = dict(
    [
        (
            "org/apache/paimon/paimon-bundle/1.2.0/paimon-bundle-1.2.0.jar",
            "7528a034115d4100c4fffdab65c358dcbc8df30a915dd48a38be19314e5375c0",
        ),
        (
            "org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar",
            "f3d2347a6e1c6885d5bcfd4f60c3ac3810ec11068fc161e04329baabf412d963",
        ),
        (
            "org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar",
            "15f01bc804294df06d2effc87de363a83cf589f50558bdbf48f72541ad8de854",
        ),
        (
            "org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar",
            "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0",
        ),
        (
            "org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar",
            "2f39bed943d624dfa8f4102d0571283a10870b6aa36f197a8a506f147010c10f",
        ),
    ]
)
SCENARIOS = {
    "smoke": ("append", 100, 1),
    "append": ("append", 4096, 1),
    "mor": ("mor", 4096, 1),
    "dv": ("dv", 4096, 1),
    "mixed": ("mixed", 4096, 1),
    "multi_bucket": ("mor", 4096, 4),
    "partitioned": ("partitioned", 100, 1),
    "wide": ("wide", 128, 1),
    "one": ("append", 1, 1),
    "bitmap64": ("bitmap64", 100, 1),
}
CORE_FIELDS = ["id", "label", "value", "embedding", "score", "new_embedding", "doc", "group_id"]


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def dependencies(cache):
    cache.mkdir(parents=True, exist_ok=True)
    for relative, expected in ARTIFACTS.items():
        target = cache / relative.rsplit("/", 1)[1]
        if not target.exists():
            with urllib.request.urlopen(f"https://repo.maven.apache.org/maven2/{relative}", timeout=120) as response:
                data = response.read()
            if sha256(data) != expected:
                raise ValueError(f"artifact digest mismatch: {relative}")
            temporary = target.with_suffix(".download")
            temporary.write_bytes(data)
            temporary.replace(target)
        if sha256(target.read_bytes()) != expected:
            raise ValueError(f"cached artifact digest mismatch: {target}")


def bundle(root, output):
    blobs = {}
    provenance = json.loads((root / "provenance.json").read_bytes())
    if provenance.get("artifacts") != ARTIFACTS:
        raise ValueError("generated fixture provenance has different Java artifacts")
    manifest = {
        "version": 1,
        "producer": "PaimonFixtures.java",
        "artifacts": ARTIFACTS,
        "generator_sha256": provenance["generator_sha256"],
        "provenance": provenance,
        "fixtures": {},
    }

    def put(data):
        digest = sha256(data)
        blobs[digest] = data
        return digest

    for scenario in sorted(root.iterdir()):
        if not scenario.is_dir():
            continue
        versions = {}
        fields = None
        for stage in sorted(scenario.iterdir()):
            if not (stage / "oracle.json").is_file():
                continue
            raw = (stage / "oracle.json").read_bytes()
            oracle = json.loads(raw)
            if oracle.get("generator_sha256") != provenance["generator_sha256"]:
                raise ValueError(f"oracle generator provenance mismatch: {stage}")
            current_fields = list(oracle["rows"][0]) if oracle["rows"] else fields or CORE_FIELDS
            fields = list(dict.fromkeys((fields or []) + current_fields))
            files = {}
            for path in sorted((stage / "table").rglob("*")):
                if not path.is_file():
                    continue
                data = path.read_bytes()
                # Table references must be relative; never rewrite binary metadata blindly.
                if str(root).encode() in data or b"file:/" in data:
                    raise ValueError(f"non-relocatable fixture file: {path}")
                files[path.relative_to(stage / "table").as_posix()] = put(data)
            # Deduplicate independently read row sets across writers/table layouts.
            # No row is regenerated from a formula or from Milvus output.
            oracle["rows_blob"] = put(json.dumps(oracle.pop("rows"), separators=(",", ":")).encode())
            versions[stage.name] = {
                "snapshot_id": oracle["snapshot_id"],
                "files": files,
                "oracle": put(json.dumps(oracle, separators=(",", ":")).encode()),
            }
        if versions:
            manifest["fixtures"][scenario.name] = {"dimension": 128, "fields": fields, "versions": versions}
    output.parent.mkdir(parents=True, exist_ok=True)
    # TCUS distributes the entire test tree in a <=32 MiB gzip source bundle.
    # LZMA keeps these pre-compressed assets within budget without removing GT.
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_LZMA) as archive:
        archive.writestr("manifest.json", json.dumps(manifest, sort_keys=True, separators=(",", ":")))
        for digest, data in sorted(blobs.items()):
            entry = zipfile.ZipInfo(f"blobs/{digest}", date_time=(2026, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_LZMA
            archive.writestr(entry, data)
    print(
        f"Wrote {output}: {len(manifest['fixtures'])} fixtures, "
        f"{len(blobs)} verified blobs, {output.stat().st_size} bytes"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java-home", type=Path, default=os.environ.get("PAIMON_JAVA_HOME"))
    parser.add_argument("--cache", type=Path, default=Path.home() / ".cache" / "milvus-paimon-fixtures")
    parser.add_argument("--output", type=Path, default=Path(__file__).parents[2] / "assets/paimon/fixtures.zip")
    parser.add_argument("--reuse-generated", type=Path, help="Package an already independently verified output tree")
    parser.add_argument(
        "--save-generated", type=Path, help="Keep Java output and raw oracles outside the temporary build"
    )
    args = parser.parse_args()
    if args.reuse_generated:
        bundle(args.reuse_generated, args.output)
        return
    dependencies(args.cache)
    java = str(args.java_home / "bin/java") if args.java_home else shutil.which("java")
    javac = str(args.java_home / "bin/javac") if args.java_home else shutil.which("javac")
    if not java or not javac:
        parser.error("JDK17 required for OFFLINE generation; specify --java-home")
    source = Path(__file__).with_name("PaimonFixtures.java")
    with tempfile.TemporaryDirectory(prefix="paimon-fixtures-") as temporary:
        work = Path(temporary)
        classes = work / "classes"
        classes.mkdir()
        classpath = os.pathsep.join(str(args.cache / relative.rsplit("/", 1)[1]) for relative in ARTIFACTS)
        subprocess.run([javac, "-cp", classpath, "-d", str(classes), str(source)], check=True)
        generated = work / "generated"
        generated.mkdir()
        provenance = {
            "generator_sha256": sha256(source.read_bytes()),
            "packager_sha256": sha256(Path(__file__).read_bytes()),
            "converter_sha256": sha256(Path(__file__).with_name("vortex_fixture.py").read_bytes()),
            "artifacts": ARTIFACTS,
            "python_packages": {
                name: importlib.metadata.version(name)
                for name in ["pyarrow", "vortex-data", "fastavro", "backports.zstd"]
            },
        }
        (generated / "provenance.json").write_text(json.dumps(provenance, sort_keys=True), encoding="utf8")
        for name, (mode, rows, buckets) in SCENARIOS.items():
            command = [
                java,
                "-Xmx1g",
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=error",
                "-Dpaimon.fixture.generator.sha256=" + provenance["generator_sha256"],
                "-cp",
                classpath + os.pathsep + str(classes),
                "PaimonFixtures",
                str(generated / name),
                mode,
                str(rows),
                str(buckets),
            ]
            subprocess.run(command, check=True, timeout=300)
        from vortex_fixture import convert_append_fixture

        convert_append_fixture(generated / "append", generated / "vortex")
        if args.save_generated:
            shutil.copytree(generated, args.save_generated)
        bundle(generated, args.output)


if __name__ == "__main__":
    main()
