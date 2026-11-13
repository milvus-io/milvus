"""Guard against duplicating the CUDA runtime through a blanket APT upgrade."""

import re
import shlex
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "build/docker/milvus/gpu/ubuntu22.04/Dockerfile"
OS_PACKAGES = {
    "apt", "openssl", "libssl3", "curl", "libcurl4",
    "tar", "zlib1g", "libc6", "libc-bin",
}


def apt_commands(source):
    source = source.replace("\\\n", " ")
    source = "\n".join(line for line in source.splitlines()
                       if not line.lstrip().startswith("#"))
    return [shlex.split(match) for match in
            re.findall(r"\bapt-get\s+([^&;\n]+)", source)]


class GpuRuntimeDockerfileTests(unittest.TestCase):
    def test_no_blanket_upgrade(self):
        for command in apt_commands(DOCKERFILE.read_text()):
            self.assertFalse({"upgrade", "dist-upgrade", "full-upgrade"} & set(command))

    def test_targeted_os_updates_are_preserved(self):
        commands = [command for command in apt_commands(DOCKERFILE.read_text())
                    if "--only-upgrade" in command]
        self.assertEqual(len(commands), 1)
        command = commands[0]
        self.assertEqual(command[0], "install")
        self.assertIn("--no-install-recommends", command)
        self.assertEqual({word for word in command[1:] if not word.startswith("-")},
                         OS_PACKAGES)

    def test_parser_recognizes_original_regression(self):
        command = apt_commands("RUN apt-get upgrade -y apt openssl && true")[0]
        self.assertIn("upgrade", command)


if __name__ == "__main__":
    unittest.main()
