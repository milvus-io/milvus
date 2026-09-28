"""Conan profile and artifact verification for native TSan dependencies."""

import argparse
import fnmatch
import hashlib
import json
import subprocess
from pathlib import Path

PACKAGES = {
    "folly": ["libfolly.so*"],
    "milvus-common": ["libmilvus-common.so*"],
    "libevent": ["libevent_core*.so*", "libevent_pthreads*.so*"],
    "onetbb": ["libtbb.so*"],
    "geos": ["libgeos.so*", "libgeos_c.so*"],
    "gtest": ["libgtest.so*", "libgtest_main.so*"],
}
FLAGS = {
    "tools.build:cflags": ["-g", "-fno-omit-frame-pointer", "-fsanitize=thread"],
    "tools.build:cxxflags": ["-g", "-fno-omit-frame-pointer", "-fsanitize=thread"],
    "tools.build:sharedlinkflags": ["-fsanitize=thread"],
    "tools.build:exelinkflags": ["-fsanitize=thread"],
}
MANIFEST = "tsan-dependencies.json"
POLICY_CONF = "user.milvus:tsan_dependency_policy"


def profile():
    lines = ["[options]", "geos/*:shared=True", "gtest/*:shared=True", "", "[conf]"]
    # Compiler flags normally do not affect Conan package IDs. Include them
    # explicitly so a prebuilt non-TSan package cannot satisfy this profile.
    lines.append("user.milvus:sanitizer=thread")
    lines.append("user.milvus:tsan_runtime_dependencies=True")
    lines.append("tools.info.package_id:confs=" + json.dumps([*FLAGS, POLICY_CONF]))
    for package in PACKAGES:
        lines.append(f"{package}/*:{POLICY_CONF}=2")
        for key, flags in FLAGS.items():
            lines.append(f"{package}/*:{key}+=" + json.dumps(flags))
        lines.append(f"{package}/*:tools.build:skip_test=True")
    return "\n".join(lines) + "\n"


def post_generate(conanfile, **kwargs):
    """Preserve flags overwritten by Folly's recipe after CMakeToolchain.generate()."""
    if conanfile.name != "folly" or conanfile.context != "host":
        return
    if conanfile.conf.get(POLICY_CONF) != 2:
        return
    toolchain = Path(conanfile.generators_folder) / "conan_toolchain.cmake"
    if not toolchain.is_file():
        raise ValueError("TSan Folly build requires a generated CMake toolchain")
    # The recipe sets CMAKE_<LANG>_FLAGS directly, hiding the *_FLAGS_INIT
    # values from tools.build:*flags. Append after those recipe definitions
    # so architecture flags such as -msse4.2 remain intact.
    with toolchain.open("a") as output:
        output.write(
            "\n# Preserve TSan instrumentation after Folly recipe overrides.\n"
        )
        for language, key in [
            ("C", "tools.build:cflags"),
            ("CXX", "tools.build:cxxflags"),
        ]:
            flags = " ".join(FLAGS[key])
            output.write(
                f'set(CMAKE_{language}_FLAGS "${{CMAKE_{language}_FLAGS}} {flags}" '
                'CACHE STRING "TSan dependency flags" FORCE)\n'
            )


def policy_hash():
    return hashlib.sha256(profile().encode()).hexdigest()


def check_library(path):
    symbols = subprocess.check_output(
        ["nm", "-D", "--undefined-only", str(path)], text=True
    )
    if "__tsan_" not in symbols:
        raise ValueError(f"TSan instrumentation missing from {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dependency_manifest(dependencies):
    packages = {}
    for dep in dependencies:
        name = dep.ref.name
        if name not in PACKAGES:
            continue
        for key, flags in FLAGS.items():
            actual = dep.conf.get(key, default=[], check_type=list)
            if not set(flags).issubset(actual):
                raise ValueError(f"TSan dependency {name} is missing {key}")
        libraries = {}
        for pattern in PACKAGES[name]:
            paths = {
                path.resolve()
                for path in (Path(dep.package_folder) / "lib").glob(pattern)
            }
            if not paths:
                raise ValueError(
                    f"TSan dependency {name} has no library matching {pattern}"
                )
            for path in sorted(paths):
                libraries[path.name] = check_library(path)
        packages[name] = {"reference": dep.pref.repr_notime(), "libraries": libraries}
    if set(packages) != set(PACKAGES):
        raise ValueError(
            f"TSan dependencies missing: {sorted(set(PACKAGES) - set(packages))}"
        )
    return {"schema": 1, "policy": policy_hash(), "packages": packages}


def verify(lib_dir):
    manifest = json.loads((lib_dir / MANIFEST).read_text())
    if manifest.get("schema") != 1 or manifest.get("policy") != policy_hash():
        raise ValueError(
            "TSan dependency manifest does not match the current build policy"
        )
    packages = manifest["packages"]
    if set(packages) != set(PACKAGES):
        raise ValueError("TSan dependency manifest has an incomplete package set")
    for name, package in packages.items():
        libraries = package["libraries"]
        if not libraries:
            raise ValueError(f"TSan dependency {name} has no recorded libraries")
        for pattern in PACKAGES[name]:
            if not any(fnmatch.fnmatch(filename, pattern) for filename in libraries):
                raise ValueError(f"TSan dependency {name} is missing {pattern}")
        for filename, expected in libraries.items():
            if Path(filename).name != filename:
                raise ValueError(f"Invalid dependency library filename: {filename}")
            if check_library(lib_dir / filename) != expected:
                raise ValueError(
                    f"TSan dependency {name}/{filename} changed after verification"
                )
        print(f"Verified TSan dependency: {name} ({package['reference']})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["profile", "verify"])
    parser.add_argument("lib_dir", type=Path, nargs="?")
    args = parser.parse_args()
    if args.command == "profile":
        print(profile(), end="")
    elif args.lib_dir is None:
        parser.error("verify requires a library directory")
    else:
        verify(args.lib_dir)
