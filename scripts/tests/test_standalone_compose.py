"""Check the message queue passed to Milvus by standalone Docker Compose."""

import json
import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "deployments/docker/standalone/docker-compose.yml"


class StandaloneComposeTests(unittest.TestCase):
    def test_message_queue(self):
        for override, expected in (
            (None, "woodpecker"),
            ("", "woodpecker"),
            ("woodpecker", "woodpecker"),
            ("rocksmq", "rocksmq"),
        ):
            with self.subTest(override=override):
                env = os.environ.copy()
                env.pop("MQ_TYPE", None)
                if override is not None:
                    env["MQ_TYPE"] = override
                config = subprocess.check_output(
                    [
                        "docker", "compose", "--env-file", os.devnull,
                        "-f", str(COMPOSE_FILE), "config", "--format", "json",
                    ],
                    env=env,
                    text=True,
                )
                standalone = json.loads(config)["services"]["standalone"]
                self.assertEqual(
                    expected, standalone["environment"].get("MQ_TYPE"),
                    "standalone service must pass MQ_TYPE to Milvus",
                )
