#!/usr/bin/env python3
"""Linux C/C++ ABI smoke test with an rlib -> staticlib -> shared-library link.

Uses a temporary aggregation crate with Release/LTO and the core lockfile.
Pass --archive to additionally test an already-built tantivy-binding archive.
No production static library is added to the Grep core crate.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile


def run(*args, **kwargs):
    subprocess.run([str(a) for a in args], check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path)
    args = parser.parse_args()
    core = Path(__file__).resolve().parents[1]
    tantivy = core.parent / "tantivy/tantivy-binding"
    symbols = ["mg_grep_compile", "mg_grep_plan_free", "mg_grep_session_create",
               "mg_grep_session_cancel", "mg_grep_session_free", "mg_grep_execute_batch",
               "mg_grep_result_view", "mg_grep_result_free"]
    with tempfile.TemporaryDirectory(prefix="milvus-grep-native-") as tmp:
        tmp = Path(tmp)
        if args.archive:
            archive = args.archive.resolve()
        else:
            (tmp / "src").mkdir()
            (tmp / "src/lib.rs").write_text("pub use milvus_grep_core::ffi;\n")
            (tmp / "Cargo.toml").write_text(
                '[package]\nname="grep-abi-aggregate"\nversion="0.1.0"\nedition="2021"\n'
                '[lib]\ncrate-type=["staticlib"]\n'
                '[dependencies]\nmilvus-grep-core={path=' + json.dumps(str(core)) + '}\n'
                '[profile.release]\nlto=true\ncodegen-units=4\npanic="unwind"\n')
            (tmp / "Cargo.lock").write_bytes((core / "Cargo.lock").read_bytes())
            target = Path(os.environ.get("GREP_ABI_TARGET_DIR", "/tmp/milvus-grep-abi-target"))
            env = dict(os.environ, CARGO_TARGET_DIR=str(target))
            run("cargo", "+1.89", "build", "--release", "--offline", cwd=tmp, env=env)
            archive = target / "release/libgrep_abi_aggregate.a"
        shared = tmp / "libgrep_abi.so"
        run("c++", "-shared", "-Wl,--no-undefined", "-o", shared,
            *("-Wl,-u," + name for name in symbols), archive,
            "-ldl", "-lpthread", "-lm", "-lrt", "-lutil")
        exported = subprocess.check_output(["nm", "-D", "--defined-only", shared], text=True)
        names = {line.split()[-1] for line in exported.splitlines()}
        assert set(symbols) <= names, set(symbols) - names
        for compiler, std, source, exe in [
            ("cc", "c11", "native_abi.c", "c-test"),
            ("c++", "c++17", "native_abi.cpp", "cpp-test"),
        ]:
            output = tmp / exe
            run(compiler, "-std=" + std, "-Wall", "-Wextra", "-Werror", "-pedantic",
                "-I" + str(core / "include"), "-I" + str(tantivy / "include"),
                core / "tests" / source, "-L" + str(tmp), "-lgrep_abi",
                "-Wl,-rpath," + str(tmp), "-o", output)
            run(output)
        print("Eight exported symbols and native C11/C++17 ABI calls passed.")


if __name__ == "__main__":
    main()
