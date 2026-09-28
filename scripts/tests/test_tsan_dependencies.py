"""Check sanitizer package identity and rejection of stale dependency artifacts."""

import importlib.util
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "milvus_tsan", ROOT / "internal/core/milvus_tsan.py"
)
tsan = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tsan)


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.lib = Path(self.temp.name)
        self.manifest = {"schema": 1, "policy": tsan.policy_hash(), "packages": {}}
        for name, patterns in tsan.PACKAGES.items():
            files = {}
            for pattern in patterns:
                filename = pattern.replace("*", "")
                path = self.lib / filename
                path.write_bytes(b"instrumented library")
                files[filename] = tsan.hashlib.sha256(path.read_bytes()).hexdigest()
            self.manifest["packages"][name] = {"reference": name, "libraries": files}
        self.write_manifest()

    def write_manifest(self):
        (self.lib / tsan.MANIFEST).write_text(json.dumps(self.manifest))

    def test_reject_uninstrumented_binary(self):
        with (
            patch.object(tsan.subprocess, "check_output", return_value=" U malloc\n"),
            self.assertRaisesRegex(ValueError, "instrumentation missing"),
        ):
            tsan.verify(self.lib)

    def test_accept_complete_artifacts_and_reject_tampering(self):
        with patch.object(
            tsan.subprocess, "check_output", return_value=" U __tsan_read8\n"
        ):
            tsan.verify(self.lib)
            (self.lib / "libfolly.so").write_bytes(b"different library")
            with self.assertRaisesRegex(ValueError, "changed after verification"):
                tsan.verify(self.lib)

    def test_reject_stale_policy(self):
        self.manifest["policy"] = "old"
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "current build policy"):
            tsan.verify(self.lib)

    def test_reject_incomplete_manifest(self):
        del self.manifest["packages"]["folly"]
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "incomplete package set"):
            tsan.verify(self.lib)


@unittest.skipUnless(
    shutil.which("conan"), "Conan 2 is required for package identity checks"
)
class ConanIdentityTests(unittest.TestCase):
    def test_manifest_preserves_recipe_and_package_revisions(self):
        from conan.api.model import PkgReference, RecipeReference

        with tempfile.TemporaryDirectory() as directory:
            lib = Path(directory) / "lib"
            lib.mkdir()
            dependencies = []
            for name, patterns in tsan.PACKAGES.items():
                for pattern in patterns:
                    (lib / pattern.replace("*", "")).write_bytes(b"instrumented")
                ref = RecipeReference.loads(f"{name}/0.1#recipe-revision")
                dependencies.append(
                    SimpleNamespace(
                        ref=ref,
                        pref=PkgReference(ref, "package-id", "package-revision"),
                        package_folder=directory,
                        conf=SimpleNamespace(get=lambda key, **kwargs: tsan.FLAGS[key]),
                    )
                )
            with patch.object(
                tsan.subprocess, "check_output", return_value=" U __tsan_read8\n"
            ):
                manifest = tsan.dependency_manifest(dependencies)
            self.assertEqual(
                manifest["packages"]["folly"]["reference"],
                "folly/0.1#recipe-revision:package-id#package-revision",
            )

    @unittest.skipUnless(
        shutil.which("cmake"), "CMake is required for toolchain checks"
    )
    def test_folly_recipe_override_retains_architecture_and_sanitizer_flags(self):
        from conan.internal.model.conf import ConfDefinition

        with tempfile.TemporaryDirectory() as directory:
            config = ConfDefinition()
            config.loads(f"{tsan.POLICY_CONF}=2")
            recipe = SimpleNamespace(
                name="folly",
                context="host",
                generators_folder=directory,
                conf=config.get_conanfile_conf("folly/0.1"),
            )
            toolchain = Path(directory) / "conan_toolchain.cmake"
            original = "\n".join(
                f'set(CMAKE_{language}_FLAGS "-msse4.2" CACHE STRING "Recipe flags")'
                for language in ["C", "CXX"]
            )
            toolchain.write_text(original)
            tsan.post_generate(recipe)
            with toolchain.open("a") as output:
                for language in ["C", "CXX"]:
                    output.write(
                        f'if(NOT CMAKE_{language}_FLAGS MATCHES "-msse4.2.*-fsanitize=thread")\n'
                        'message(FATAL_ERROR "Missing architecture or sanitizer flags")\nendif()\n'
                    )
            subprocess.run(
                ["cmake", "-P", str(toolchain)], check=True, capture_output=True
            )
            toolchain.write_text(original)
            recipe.context = "build"
            tsan.post_generate(recipe)
            self.assertEqual(toolchain.read_text(), original)

    def test_host_sanitizer_identity_is_distinct_and_build_tools_are_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = {**os.environ, "CONAN_HOME": str(root / "cache")}

            def conan(*args):
                return subprocess.run(
                    ["conan", *args],
                    env=env,
                    text=True,
                    check=True,
                    capture_output=True,
                ).stdout

            conan("profile", "detect")
            package = root / "package"
            package.mkdir()
            (package / "conanfile.py").write_text(
                "from conan import ConanFile\nclass Package(ConanFile):\n"
                ' name="folly"\n version="0.1"\n package_type="shared-library"\n'
                ' settings="os", "arch", "compiler", "build_type"\n'
            )
            conan("export", str(package))
            consumer = root / "consumer"
            consumer.mkdir()
            (consumer / "conanfile.py").write_text(
                "from conan import ConanFile\nclass Consumer(ConanFile):\n"
                ' settings="os", "arch", "compiler", "build_type"\n'
                ' requires="folly/0.1"\n tool_requires="folly/0.1"\n'
            )
            profile = root / "tsan.profile"
            profile.write_text(tsan.profile())

            def identities(extra):
                data = json.loads(
                    conan(
                        "graph", "info", str(consumer), "-nr", "--format=json", *extra
                    )
                )
                return {
                    node["context"]: node["package_id"]
                    for node in data["graph"]["nodes"].values()
                    if (node.get("ref") or "").startswith("folly/0.1")
                }

            normal = identities([])
            sanitized = identities(["-pr:h", "default", "-pr:h", str(profile)])
            self.assertNotEqual(normal["host"], sanitized["host"])
            self.assertEqual(normal["build"], sanitized["build"])


if __name__ == "__main__":
    unittest.main()
