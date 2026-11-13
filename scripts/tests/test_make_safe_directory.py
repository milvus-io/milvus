"""Exercise Makefile Git ownership handling with isolated real repositories."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class MakeSafeDirectoryTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.repo = self.root / "checkout with spaces"
        self.repo.mkdir()
        self.config = self.root / "gitconfig"
        self.config.touch()
        self.env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("GIT_", "MAKE"))
        }
        self.env.update(
            GIT_CONFIG_GLOBAL=str(self.config),
            GIT_CONFIG_NOSYSTEM="1",
            GIT_CONFIG_SYSTEM=os.devnull,
            USE_TSAN="OFF",
            USE_ASAN="OFF",
        )
        shutil.copyfile(ROOT / "Makefile", self.repo / "Makefile")
        self.run_command("git", "init", "-q")
        self.run_command(
            "git", "-c", "user.name=Test", "-c", "user.email=test@example.com",
            "commit", "--allow-empty", "-qm", "Initial commit",
        )
        self.run_command("git", "tag", "v1.2.3")
        # Observe the eagerly computed version without modifying its definition.
        (self.repo / "inspect.mk").write_text(
            'include Makefile\n.PHONY: inspect-version\n'
            'inspect-version:\n\t@echo "VERSION=$(MILVUS_VERSION)"\n'
        )

    def run_command(self, *args, check=True):
        return subprocess.run(
            args,
            cwd=self.repo,
            env=self.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=check,
        )

    def make_repeatedly(self):
        for options in [[], ["-n"], []]:
            result = self.run_command(
                "make", "-s", *options, "-f", "inspect.mk",
                "print-build-info", "inspect-version",
            )
            self.assertIn("VERSION=1.2.3", result.stdout)

    def safe_directories(self):
        return self.run_command(
            "git", "config", "--global", "--get-all", "safe.directory", check=False
        ).stdout.splitlines()

    def test_trusted_checkout_does_not_change_global_config(self):
        self.make_repeatedly()
        self.assertEqual(self.config.read_text(), "", "trusted checkout changed global config")

    def test_dubious_checkout_is_trusted_once_and_only_for_this_path(self):
        self.env["GIT_TEST_ASSUME_DIFFERENT_OWNER"] = "1"
        refused = self.run_command("git", "rev-parse", "--git-dir", check=False)
        self.assertIn("detected dubious ownership", refused.stderr)
        self.make_repeatedly()
        self.assertEqual(self.safe_directories(), [str(self.repo)])
        self.run_command("git", "rev-parse", "--git-dir")

    def test_existing_trust_is_preserved_without_duplicates(self):
        self.env["GIT_TEST_ASSUME_DIFFERENT_OWNER"] = "1"
        self.run_command("git", "config", "--global", "--add", "safe.directory", str(self.repo))
        before = self.config.read_bytes()
        self.make_repeatedly()
        self.assertEqual(self.config.read_bytes(), before)

    def test_non_repository_does_not_change_global_config(self):
        shutil.rmtree(self.repo / ".git")
        self.run_command("make", "-s", "-n", "print-build-info")
        self.assertEqual(self.config.read_text(), "", "non-repository changed global config")


if __name__ == "__main__":
    unittest.main()
