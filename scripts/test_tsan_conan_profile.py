# Licensed under the Apache License, Version 2.0.
"""Check sanitizer package identities offline, without compiling dependencies."""

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class TSanProfileTest(unittest.TestCase):
    def test_host_binary_identity_and_build_tool_isolation(self):
        conan = shutil.which("conan")
        self.assertIsNotNone(conan, "Install the repository's Conan version first")
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = dict(os.environ, CONAN_HOME=str(root / "conan-home"))

            def run(*args):
                return subprocess.run(
                    [conan, *args],
                    env=env,
                    check=True,
                    text=True,
                    capture_output=True,
                ).stdout

            profiles = root / "conan-home" / "profiles"
            profiles.mkdir(parents=True)
            (profiles / "default").write_text(
                "[settings]\n"
                "os=Linux\narch=x86_64\nbuild_type=Release\n"
                "compiler=gcc\ncompiler.version=13\ncompiler.libcxx=libstdc++11\n"
            )
            # Export empty recipes to exercise Conan's graph and package-ID rules.
            # No source build, network remote, or user's cache is involved.
            for name, kind in (
                ("probe-lib", "static-library"),
                ("probe-tool", "application"),
            ):
                recipe = root / name
                recipe.mkdir()
                (recipe / "conanfile.py").write_text(
                    "from conan import ConanFile\n"
                    "class Probe(ConanFile):\n"
                    f"    name = {name!r}\n"
                    "    version = '0.1'\n"
                    f"    package_type = {kind!r}\n"
                    "    settings = 'os', 'arch', 'compiler', 'build_type'\n"
                )
                run("export", str(recipe))

            consumer = root / "consumer"
            consumer.mkdir()
            (consumer / "conanfile.py").write_text(
                "from conan import ConanFile\n"
                "class Consumer(ConanFile):\n"
                "    settings = 'os', 'arch', 'compiler', 'build_type'\n"
                "    requires = 'probe-lib/0.1'\n"
                "    tool_requires = 'probe-tool/0.1'\n"
            )

            def graph(profile):
                data = json.loads(
                    run(
                        "graph",
                        "info",
                        str(consumer),
                        "--no-remote",
                        "--format=json",
                        "-pr:h",
                        str(profile),
                        "-pr:b",
                        "default",
                    )
                )
                return {
                    node["name"]: node
                    for node in data["graph"]["nodes"].values()
                    if node.get("name")
                }

            ordinary = graph("default")
            tsan = graph(repo / "internal/core/conan/profiles/tsan")
            self.assertNotEqual(
                ordinary["probe-lib"]["package_id"], tsan["probe-lib"]["package_id"]
            )
            self.assertEqual(
                ordinary["probe-tool"]["package_id"], tsan["probe-tool"]["package_id"]
            )
            self.assertEqual(tsan["probe-tool"]["context"], "build")


if __name__ == "__main__":
    unittest.main()
