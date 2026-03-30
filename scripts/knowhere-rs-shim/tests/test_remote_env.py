import json
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]
REMOTE_ENV_PATH = ROOT / "scripts/knowhere-rs-shim/remote_env.sh"


class RemoteEnvTest(unittest.TestCase):
    def test_autoindex_build_env_is_valid_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            command = (
                f"source {REMOTE_ENV_PATH} >/dev/null 2>&1; "
                f"source {REMOTE_ENV_PATH} >/dev/null 2>&1; "
                "printf '%s' \"$MILVUS_CONF_AUTOINDEX_PARAMS_BUILD\""
            )
            env = {
                "MILVUS_RS_INTEG_ROOT": tmpdir,
                "MILVUS_RS_GO_ROOT": f"{tmpdir}/go",
                "MILVUS_RS_GO_WORK_ROOT": f"{tmpdir}/go-work",
                "MILVUS_RS_VAR_ROOT": f"{tmpdir}/milvus-var",
            }
            result = subprocess.run(
                ["bash", "-lc", command],
                check=True,
                capture_output=True,
                text=True,
                cwd=ROOT,
                env=env,
            )

        payload = json.loads(result.stdout)

        self.assertEqual(payload["index_type"], "HNSW")
        self.assertEqual(payload["metric_type"], "COSINE")
        self.assertEqual(payload["M"], 18)
        self.assertEqual(payload["efConstruction"], 240)

    def test_ci_log_path_uses_var_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            command = (
                f"source {REMOTE_ENV_PATH} >/dev/null 2>&1; "
                "printf '%s' \"${CI_LOG_PATH:-}\""
            )
            env = {
                "MILVUS_RS_INTEG_ROOT": tmpdir,
                "MILVUS_RS_GO_ROOT": f"{tmpdir}/go",
                "MILVUS_RS_GO_WORK_ROOT": f"{tmpdir}/go-work",
                "MILVUS_RS_VAR_ROOT": f"{tmpdir}/milvus-var",
            }
            result = subprocess.run(
                ["bash", "-lc", command],
                check=True,
                capture_output=True,
                text=True,
                cwd=ROOT,
                env=env,
            )

        self.assertEqual(result.stdout, f"{tmpdir}/milvus-var/ci-logs")


if __name__ == "__main__":
    unittest.main()
