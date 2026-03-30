import importlib.util
import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[3]
LIB_PATH = ROOT / "scripts/knowhere-rs-shim/standalone_subset_lib.py"
COLLECTOR_PATH = ROOT / "scripts/knowhere-rs-shim/collect_subset_results.py"
MANIFEST_PATH = ROOT / "scripts/knowhere-rs-shim/standalone_test_subset.json"


def load_module(path: pathlib.Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class StandaloneSubsetLibTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lib = load_module(LIB_PATH, "standalone_subset_lib")

    def test_flatten_manifest_preserves_case_count(self):
        cases = self.lib.flatten_manifest(MANIFEST_PATH)
        self.assertEqual(len(cases), 142)

        go_cases = [case for case in cases if case["suite"] == "go_client"]
        self.assertEqual(len(go_cases), 70)
        self.assertTrue(any(case["case"] == "TestSearchDefault" for case in go_cases))

    def test_build_go_client_command_uses_exact_run_regex(self):
        cases = self.lib.flatten_manifest(MANIFEST_PATH)
        case = next(
            item
            for item in cases
            if item["suite"] == "go_client" and item["case"] == "TestSearchDefault"
        )

        command = self.lib.build_case_command(
            case,
            root_dir=ROOT,
            endpoint="http://127.0.0.1:19530",
            token="root:Milvus",
            minio_host="127.0.0.1",
        )

        self.assertEqual(command["cwd"], str(ROOT / "tests/go_client"))
        self.assertEqual(
            command["argv"],
            [
                "go",
                "test",
                "./testcases",
                "-run",
                "^TestSearchDefault$",
                "-count=1",
                "-timeout=20m",
                "-args",
                "-addr",
                "http://127.0.0.1:19530",
            ],
        )

    def test_build_python_client_selector_is_working_dir_relative(self):
        cases = self.lib.flatten_manifest(MANIFEST_PATH)
        case = next(
            item
            for item in cases
            if item["suite"] == "python_client"
            and item["case"] == "TestE2e::test_milvus_default"
        )

        with mock.patch.dict(os.environ, {"CI_LOG_PATH": "/tmp/standalone-ci"}, clear=False):
            command = self.lib.build_case_command(
                case,
                root_dir=ROOT,
                endpoint="http://127.0.0.1:19530",
                token="root:Milvus",
                minio_host="127.0.0.1",
            )

        self.assertEqual(command["cwd"], str(ROOT / "tests/python_client"))
        self.assertEqual(
            command["argv"],
            [
                "python3",
                "-W",
                "ignore",
                "-m",
                "pytest",
                "-q",
                "-s",
                "-o",
                "addopts=--html=/tmp/standalone-ci/report.html --self-contained-html -v",
                "testcases/test_e2e.py",
                "-k",
                "TestE2e and test_milvus_default",
                "--host",
                "127.0.0.1",
                "--port",
                "19530",
                "--uri",
                "http://127.0.0.1:19530",
                "--token",
                "root:Milvus",
            ],
        )


class CollectSubsetResultsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.collector = load_module(COLLECTOR_PATH, "collect_subset_results")

    def test_collect_results_rolls_up_suite_counts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            go_dir = root / "go_client"
            py_dir = root / "python_client"
            go_dir.mkdir(parents=True)
            py_dir.mkdir(parents=True)

            (go_dir / "case-pass.json").write_text(
                json.dumps(
                    {
                        "suite": "go_client",
                        "case": "TestSearchDefault",
                        "status": "PASS",
                        "exit_code": 0,
                    }
                )
            )
            (py_dir / "case-fail.json").write_text(
                json.dumps(
                    {
                        "suite": "python_client",
                        "case": "TestE2e::test_milvus_default",
                        "status": "FAIL",
                        "exit_code": 1,
                    }
                )
            )

            summary = self.collector.collect_results(root)

        self.assertEqual(summary["totals"]["cases"], 2)
        self.assertEqual(summary["totals"]["PASS"], 1)
        self.assertEqual(summary["totals"]["FAIL"], 1)
        self.assertEqual(summary["suites"]["go_client"]["PASS"], 1)
        self.assertEqual(summary["suites"]["python_client"]["FAIL"], 1)


if __name__ == "__main__":
    unittest.main()
