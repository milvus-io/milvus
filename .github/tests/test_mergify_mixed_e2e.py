"""Local condition-regression tests; not a substitute for Mergify service validation.

Run: python3 -B .github/tests/test_mergify_mixed_e2e.py
Requires PyYAML. Set MERGIFY_TEST_BASELINE=1 to reproduce failures on HEAD.
"""

import itertools
import os
from pathlib import Path
import re
import subprocess
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]
TEXT = (
    subprocess.check_output(
        ["git", "-C", str(ROOT), "show", "HEAD:.github/mergify.yml"], text=True
    )
    if os.environ.get("MERGIFY_TEST_BASELINE")
    else (ROOT / ".github/mergify.yml").read_text()
)
RULES = yaml.safe_load(TEXT)["pull_request_rules"]
PR_55243_FILES = [
    "tests/go_client/testcases/struct_array_column_schema_test.go",
    "tests/go_client/testcases/struct_array_dml_search_test.go",
    "tests/python_client/base/struct_array.py",
    "tests/python_client/common/common_func.py",
    "tests/python_client/common/struct_array_concurrent_workload.py",
    "tests/python_client/common/struct_array_data.py",
    "tests/python_client/common/struct_array_wire.py",
    "tests/python_client/conftest.py",
    "tests/python_client/milvus_client/test_milvus_client_struct_array_bulk_import.py",
    "tests/python_client/milvus_client/test_milvus_client_struct_array_concurrent_mutations.py",
    "tests/python_client/milvus_client/test_milvus_client_struct_array_dml.py",
    "tests/python_client/milvus_client/test_milvus_client_struct_array_nullable.py",
    "tests/python_client/milvus_client/test_milvus_client_struct_array_query.py",
    "tests/python_client/milvus_client/test_milvus_client_struct_array_schema.py",
    "tests/python_client/milvus_client/test_milvus_client_struct_array_search.py",
    "tests/python_client/requirements.txt",
    "tests/restful_client_v2/base/struct_array.py",
    "tests/restful_client_v2/requirements.txt",
    "tests/restful_client_v2/testcases/test_struct_array_dml.py",
    "tests/restful_client_v2/testcases/test_struct_array_element_null.py",
    "tests/restful_client_v2/testcases/test_struct_array_import_rejection.py",
    "tests/restful_client_v2/testcases/test_struct_array_subfield_evolution.py",
    "tests/restful_client_v2/utils/struct_array_data.py",
    "tests/restful_client_v2/utils/utils.py",
]


def evaluate(condition, attributes):
    if isinstance(condition, dict):
        operator, terms = next(iter(condition.items()))
        if operator == "and":
            return all(evaluate(term, attributes) for term in terms)
        if operator == "or":
            return any(evaluate(term, attributes) for term in terms)
        if operator == "not":
            return not evaluate(terms, attributes)
        raise AssertionError(condition)
    if condition.startswith("-"):
        return not evaluate(condition[1:], attributes)
    parsed = re.fullmatch(r"([^!=~]+)(~=|!=|=)(.*)", condition)
    if not parsed:
        raise AssertionError(condition)
    key, operator, wanted = (value.strip() for value in parsed.groups())
    actual = attributes.get(key, [])
    actual = actual if isinstance(actual, list) else [actual]
    if operator == "~=":
        return any(re.search(wanted, value) for value in actual)
    matches = wanted in actual
    return not matches if operator == "!=" else matches


def matching_actions(
    files, success=(), manual=False, base="3.0", title="test: regression"
):
    attributes = {
        "base": base,
        "files": files,
        "title": title,
        "label": ["ci-passed"] + (["manual-pass"] if manual else []),
        "status-success": list(success),
    }
    actions = {"add": [], "remove": []}
    for rule in RULES:
        labels = rule["actions"].get("label", {})
        if not any("ci-passed" in labels.get(action, []) for action in actions):
            continue
        if all(evaluate(term, attributes) for term in rule["conditions"]):
            for action in actions:
                if "ci-passed" in labels.get(action, []):
                    actions[action].append(rule["name"])
    return actions


class MergifyMixedE2ETest(unittest.TestCase):
    def assert_gate(self, files, passed, success=(), **kwargs):
        actions = matching_actions(files, success, **kwargs)
        self.assertEqual(bool(actions["add"]), passed, actions)
        self.assertEqual(bool(actions["remove"]), not passed, actions)

    def test_actual_pr_55243(self):
        self.assert_gate(PR_55243_FILES, True, ["ci-v2/e2e-amd", "ci-v2/go-sdk"])

    def test_mixed_suite_status_matrix(self):
        for sdk in ["a_test.go", "helper.go", "config.yaml"]:
            for suite in ["python_client", "restful_client_v2"]:
                files = [
                    "tests/go_client/" + sdk,
                    "tests/" + suite + "/test_a.py",
                    "README.md",
                ]
                for e2e, gosdk in itertools.product(
                    ["missing", "pending", "failure", "error", "success"], repeat=2
                ):
                    success = (["ci-v2/e2e-amd"] if e2e == "success" else []) + (
                        ["ci-v2/go-sdk"] if gosdk == "success" else []
                    )
                    with self.subTest(files=files, e2e=e2e, gosdk=gosdk):
                        # Non-success states all lack status-success in Mergify.
                        self.assert_gate(files, e2e == gosdk == "success", success)

    def test_manual_pass_preserves_label(self):
        for success in [[], ["ci-v2/e2e-amd"], ["ci-v2/go-sdk"]]:
            actions = matching_actions(PR_55243_FILES, success, manual=True)
            self.assertFalse(actions["remove"], actions)

    def test_mixed_production_still_requires_build_ut(self):
        for extra in [
            "internal/proxy/a.go",
            "pkg/a.go",
            "client/a.go",
            "cmd/a.go",
            "internal/a.cpp",
            "go.mod",
            "Makefile",
            "configs/milvus.yaml",
            "tests/integration/a_test.go",
            "scripts/test.sh",
        ]:
            with self.subTest(extra=extra):
                actions = matching_actions(
                    PR_55243_FILES + [extra], ["ci-v2/e2e-amd", "ci-v2/go-sdk"]
                )
                self.assertFalse(actions["add"], actions)
                self.assertFalse(any("mixed E2E" in name for name in actions["remove"]))
                if extra.endswith((".go", ".cpp")) or extra == "go.mod":
                    self.assertTrue(actions["remove"], actions)
        self.assert_gate(
            PR_55243_FILES + ["internal/proxy/a.go"],
            True,
            ["ci-v2/e2e-amd", "ci-v2/go-sdk", "ci-v2/build-ut-cov"],
        )

    def test_pure_sdk_and_unit_tests_unchanged(self):
        self.assert_gate(
            ["tests/go_client/a_test.go", "README.md"], True, ["ci-v2/go-sdk"]
        )
        self.assert_gate(["tests/go_client/a_test.go"], False)
        self.assert_gate(
            ["internal/proxy/a_test.go", "README.md"], True, ["ci-v2/build-ut-cov"]
        )

    def test_pure_python_and_docs_unchanged(self):
        self.assert_gate(["tests/python_client/a.py"], True, ["ci-v2/e2e-amd"])
        self.assert_gate(["README.md"], True)

    def test_directory_boundaries_and_other_branches(self):
        for files in [
            ["tests/go_client/README.md", "tests/python_client/a.py"],
            ["tests/go_client/a_test.go", "tests/python_client/README.md"],
            ["tests/go_client/a_test.go", "tests/python_client_extra/a.py"],
            ["tests/go_client/a_test.go", "tests/restful_client_v2_extra/a.py"],
        ]:
            actions = matching_actions(files, ["ci-v2/e2e-amd", "ci-v2/go-sdk"])
            self.assertFalse(
                any(
                    "mixed E2E" in name for names in actions.values() for name in names
                ),
                actions,
            )
        for branch in ["master", "2.6"]:
            actions = matching_actions(PR_55243_FILES, base=branch)
            self.assertFalse(
                any(
                    "mixed E2E" in name for names in actions.values() for name in names
                ),
                actions,
            )

    def test_rule_names_are_unique(self):
        names = [rule["name"] for rule in RULES]
        self.assertEqual(len(names), len(set(names)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
