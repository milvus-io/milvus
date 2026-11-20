"""Build pinned native workers; does not deploy or change a Milvus cluster.

Requires Go, JDK17 and Maven. Set JAVA_HOME, MAVEN_BIN, GO_BIN as needed.
SDK checkout/cache is outside the test output and never contains credentials.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
JAVA_COMMIT = "617fd7967ad22420a84d492c831ccfc8c61f0406"


def run(*args, cwd=None):
    subprocess.run(list(map(str, args)), cwd=cwd, check=True)


def main():
    go = os.environ.get("GO_BIN") or shutil.which("go")
    java_home = Path(os.environ["JAVA_HOME"])
    maven = os.environ.get("MAVEN_BIN") or shutil.which("mvn")
    if not go or not maven:
        raise SystemExit("Go and Maven must be installed or specified with GO_BIN/MAVEN_BIN")
    run(go, "build", "-o", ROOT / "go-worker", ".", cwd=ROOT / "go")
    sdk = Path(os.environ.get("RR_JAVA_SDK_SOURCE", str(ROOT / "java-sdk-source")))
    if not sdk.exists():
        run(
            "git",
            "clone",
            "--no-checkout",
            "--filter=blob:none",
            "https://github.com/milvus-io/milvus-sdk-java.git",
            sdk,
        )
        run("git", "checkout", JAVA_COMMIT, cwd=sdk)
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=sdk, text=True).strip()
    if actual != JAVA_COMMIT:
        raise SystemExit(f"SDK source must be pinned to {JAVA_COMMIT}, got {actual}; refusing to overwrite checkout")
    run("git", "submodule", "update", "--init", "--recursive", cwd=sdk)
    run(maven, "-B", "-ntp", "-pl", "sdk-core", "-am", "-Dmaven.test.skip=true", "install", cwd=sdk)
    deps = sdk / "native-dependencies.classpath"
    run(maven, "-B", "-pl", "sdk-core", "dependency:build-classpath", f"-Dmdep.outputFile={deps}", cwd=sdk)
    classes = ROOT / "java/classes"
    classes.mkdir(parents=True, exist_ok=True)
    cp = str(sdk / "sdk-core/target/classes") + os.pathsep + deps.read_text().strip()
    run(java_home / "bin/javac", "-cp", cp, "-d", classes, ROOT / "java/src/test/java/rr/Worker.java")
    (ROOT / "java-runtime.json").write_text(
        json.dumps(dict(java=str(java_home / "bin/java"), classpath=str(classes) + os.pathsep + cp), indent=2)
    )
    (ROOT / "versions.json").write_text(
        json.dumps(
            dict(java_sdk_commit=actual, go_sdk="v3.0.0-20261112010801-61eef6cdbe40", java_runtime=str(java_home)),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
