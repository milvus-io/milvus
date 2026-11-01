import os
import pathlib
import shutil
import subprocess
import tempfile

repo = pathlib.Path(__file__).resolve().parents[2]


def run(args, **kwargs):
    p = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)
    assert p.returncode == 0, p.stdout
    return p.stdout


with tempfile.TemporaryDirectory(prefix="cargo-backport-") as tmp:
    root = pathlib.Path(tmp)
    source = root / "source"
    source.mkdir()
    target = source / "tantivy"
    target.mkdir()
    original = repo / "internal/core/thirdparty/tantivy"
    shutil.copytree(original / "tantivy-binding/include", target / "tantivy-binding/include")
    for header in original.glob("*.h"):
        shutil.copy(header, target / header.name)
    fake = root / "fake-cargo"
    fake.write_text("""#!/bin/bash
set -eu
mkdir -p "$CARGO_TARGET_DIR/release"
printf 'called\n' >> "$CARGO_TARGET_DIR/calls"
printf '%s' "$FIXTURE_CONTENT" > "$CARGO_TARGET_DIR/release/libtantivy_binding.a"
""")
    fake.chmod(0o755)
    # Replace only the compiler executable; exercise the actual CMake graph.
    (target / "CMakeLists.txt").write_text(
        (original / "CMakeLists.txt").read_text().replace("set(CARGO_CMD cargo ", f"set(CARGO_CMD {fake} ")
    )
    (source / "CMakeLists.txt").write_text(
        "cmake_minimum_required(VERSION 3.20)\nproject(cache_test NONE)\nset(CMAKE_STATIC_LIBRARY_SUFFIX .a)\nadd_subdirectory(tantivy)\n"
    )
    build, cache, install = root / "build", root / "cache", root / "install"
    env = dict(os.environ, FIXTURE_CONTENT="first")
    run(
        [
            "cmake",
            "-S",
            str(source),
            "-B",
            str(build),
            "-G",
            "Ninja",
            "-DCMAKE_BUILD_TYPE=Release",
            f"-DCMAKE_INSTALL_PREFIX={install}",
            f"-DMILVUS_CARGO_TARGET_ROOT={cache}",
        ],
        env=env,
    )
    command = ["cmake", "--build", str(build), "--target", "install_tantivy_target"]
    run(command, env=env)
    lib = install / "lib/libtantivy_binding.a"
    first_mtime = lib.stat().st_mtime_ns
    run(command, env=env)
    assert lib.stat().st_mtime_ns == first_mtime
    assert (cache / "tantivy/calls").read_text().count("called") == 2
    env["FIXTURE_CONTENT"] = "changed"
    run(command, env=env)
    assert lib.read_text() == "changed"
    assert (cache / "tantivy/calls").read_text().count("called") == 3
    print(
        "PASS: repeated Cargo invocation, persistent output, unchanged install mtime, changed content propagation (mock compiler / real CMake+Ninja)"
    )

    core = (repo / "scripts/core_build.sh").read_text()
    block = core[core.index('if [[ -n "${MILVUS_CARGO_TARGET_ROOT:-}"') : core.index("# build with diskann index")]
    cargo_build = root / "cargo-build"
    cargo_build.mkdir()
    (cargo_build / "cargo").mkdir()
    env = dict(os.environ, BUILD_OUTPUT_DIR=str(cargo_build), MILVUS_CARGO_TARGET_ROOT=str(cache))
    run(["bash", "-eu", "-c", block], env=env)
    assert (cargo_build / "cargo").is_symlink()
    assert (cargo_build / "cargo").resolve() == (cache / "milvus-storage").resolve()
    run(["bash", "-eu", "-c", block], env=env)
    (cargo_build / "cargo").unlink()
    (cargo_build / "cargo").write_text("preserve")
    p = subprocess.run(["bash", "-eu", "-c", block], env=env, capture_output=True)
    assert p.returncode != 0 and (cargo_build / "cargo").read_text() == "preserve"
    env["MILVUS_CARGO_TARGET_ROOT"] = "relative"
    p = subprocess.run(["bash", "-eu", "-c", block], env=env, capture_output=True)
    assert p.returncode != 0
    print("PASS: Corrosion directory migration, repeated symlink use, file collision and relative-root rejection")
