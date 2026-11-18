"""Generate real Paimon tables and snapshot-pinned reader ground truth for this test run."""

import argparse
import fcntl
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
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


FORMAT_PACKAGES = {"pyarrow": "23.0.1", "vortex-data": "0.56.0", "fastavro": "1.13.1", "backports.zstd": "1.8.0"}
JDK_VERSION = "17.0.21.0.11"
JDK_SHA256 = {
    "x86_64": ("x64", "8f9a2ddb131ec5317419f2b0769240fe22ce27f8a0ae43f5346d53e1c8aec6a0"),
    "aarch64": ("aarch64", "33412bba53b912b21528150df7cf60c161d10fc3b7dae89cd5383d8f3c0362c0"),
}


def dependency(url, digest, target):
    if not target.exists():
        with (
            urllib.request.urlopen(url, timeout=120) as response,
            tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as output,
        ):
            temporary = Path(output.name)
            try:
                size = 0
                while chunk := response.read(1024 * 1024):
                    size += len(chunk)
                    if size > 512 * 1024 * 1024:
                        raise ValueError("dependency download exceeds size bound")
                    output.write(chunk)
                output.flush()
                with temporary.open("rb") as stream:
                    actual = hashlib.file_digest(stream, "sha256").hexdigest()
                if actual != digest:
                    raise ValueError(f"dependency digest mismatch: {url}")
                temporary.replace(target)
            finally:
                temporary.unlink(missing_ok=True)
    with target.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != digest:
            raise ValueError(f"cached dependency digest mismatch: {target}")


def jdk17(home):
    try:
        result = subprocess.run([str(home / "bin/javac"), "-version"], capture_output=True, text=True, timeout=15)
        return result.returncode == 0 and re.search(r"javac 17(?:\.|\s)", result.stdout + result.stderr) is not None
    except (OSError, subprocess.TimeoutExpired):
        return False


def dependencies(cache, requested_home=None):
    actual = {name: importlib.metadata.version(name) for name in FORMAT_PACKAGES}
    if actual != FORMAT_PACKAGES:
        raise RuntimeError(
            f"Paimon generation needs pinned format packages {FORMAT_PACKAGES}; installed {actual}. Install tests/python_client/requirements.txt"
        )
    cache.mkdir(parents=True, exist_ok=True)
    with (cache / ".dependencies.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if requested_home:
            home = Path(requested_home)
            if not jdk17(home):
                raise RuntimeError(f"JDK17 with javac is required at {home}")
        else:
            compiler = shutil.which("javac")
            home = Path(compiler).resolve().parent.parent if compiler else Path("/nonexistent-jdk")
            if not jdk17(home):
                machine = platform.machine().lower()
                if platform.system() != "Linux" or machine not in JDK_SHA256:
                    raise RuntimeError(
                        "Install JDK17 or set PAIMON_JAVA_HOME; automatic dependency provisioning supports Linux x86_64/aarch64"
                    )
                arch, digest = JDK_SHA256[machine]
                home = cache / f"corretto-{JDK_VERSION}-{arch}"
                if not home.exists():
                    archive = cache / f"corretto-{JDK_VERSION}-{arch}.tar.gz"
                    dependency(
                        f"https://corretto.aws/downloads/resources/{JDK_VERSION}/amazon-corretto-{JDK_VERSION}-linux-{arch}.tar.gz",
                        digest,
                        archive,
                    )
                    with tempfile.TemporaryDirectory(dir=cache, prefix=".jdk-") as temporary:
                        with tarfile.open(archive) as tar:
                            tar.extractall(temporary, filter="data")
                        candidates = list(Path(temporary).glob("*/bin/javac"))
                        if len(candidates) != 1 or not jdk17(candidates[0].parent.parent):
                            raise RuntimeError("Downloaded JDK does not contain a working JDK17")
                        candidates[0].parent.parent.replace(home)
                if not jdk17(home):
                    raise RuntimeError(f"Cached JDK17 is invalid: {home}")
        for relative, expected in ARTIFACTS.items():
            dependency(f"https://repo.maven.apache.org/maven2/{relative}", expected, cache / relative.rsplit("/", 1)[1])
    return home


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
    # Compress this run-owned intermediate bundle; all rows came from the pinned reader.
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
    parser.add_argument(
        "--cache",
        type=Path,
        default=Path(os.environ.get("PAIMON_DEPENDENCY_CACHE", "~/.cache/milvus-paimon-dependencies")).expanduser(),
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    started = time.monotonic()
    home = dependencies(args.cache, args.java_home)
    dependency_seconds = time.monotonic() - started
    java, javac = str(home / "bin/java"), str(home / "bin/javac")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).with_name("PaimonFixtures.java")
    with tempfile.TemporaryDirectory(dir=args.output.parent, prefix=".generation-") as temporary:
        work = Path(temporary)
        classes = work / "classes"
        classes.mkdir()
        classpath = os.pathsep.join(str(args.cache / relative.rsplit("/", 1)[1]) for relative in ARTIFACTS)
        subprocess.run([javac, "-cp", classpath, "-d", str(classes), str(source)], check=True, timeout=120)
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
        bundle(generated, args.output)
    print(
        json.dumps({"dependency_seconds": dependency_seconds, "total_seconds": time.monotonic() - started}), flush=True
    )


if __name__ == "__main__":
    main()
