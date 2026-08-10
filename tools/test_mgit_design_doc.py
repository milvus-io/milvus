import importlib.util
import pathlib
import sys
import unittest


MGIT_PATH = pathlib.Path(__file__).with_name("mgit.py")
spec = importlib.util.spec_from_file_location("mgit", MGIT_PATH)
mgit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mgit)

POLICY_PATH = (
    pathlib.Path(__file__).resolve().parents[1]
    / ".github/scripts/check_design_doc_policy.py"
)
sys.path.insert(0, str(POLICY_PATH.parent))
policy_spec = importlib.util.spec_from_file_location(
    "check_design_doc_policy_for_mgit_test", POLICY_PATH
)
policy = importlib.util.module_from_spec(policy_spec)
sys.modules[policy_spec.name] = policy
policy_spec.loader.exec_module(policy)


class DesignDocRefTest(unittest.TestCase):
    def test_accepts_in_repo_markdown_path(self):
        accepted = [
            "docs/design-docs/design_docs/20260128-vector-compression.md",
            "docs/design-docs/design_docs/cdc/20260128-vector_compression.md",
            "docs/design-docs/design_docs/README.md",
            "docs/design-docs/design_docs/segcore/Search.md",
            "docs/design-docs/design_docs/Legacy Topic/Old Design.md",
            r"docs\design-docs\design_docs\20260128-vector-compression.md",
        ]
        for path in accepted:
            with self.subTest(path=path):
                self.assertTrue(mgit.is_valid_design_doc_ref(path))

    def test_normalizes_windows_path_for_downstream_use(self):
        normalized = mgit.normalize_design_doc_ref(
            r"docs\design-docs\design_docs\20260128-vector-compression.md"
        )
        self.assertEqual(
            "docs/design-docs/design_docs/20260128-vector-compression.md",
            normalized,
        )
        self.assertEqual(
            [normalized],
            policy.extract_design_doc_references(f"design doc: {normalized}"),
        )

    def test_rejects_github_url_even_when_it_contains_the_path(self):
        self.assertFalse(
            mgit.is_valid_design_doc_ref(
                "https://github.com/milvus-io/milvus/blob/master/docs/design-docs/design_docs/20260128-vector-compression.md"
            )
        )

    def test_rejects_external_design_doc_repo(self):
        self.assertFalse(
            mgit.is_valid_design_doc_ref(
                "https://github.com/milvus-io/milvus-design-docs/blob/main/design_docs/20260128-vector-compression.md"
            )
        )

    def test_rejects_directory_or_non_markdown_path(self):
        rejected = [
            "docs/design-docs/design_docs/",
            "docs/design-docs/design_docs/20260128-vector-compression.txt",
            "docs/design-docs/design_docs/../20260128-vector-compression.md",
            "docs/design-docs/design_docs//20260128-vector-compression.md",
        ]
        for path in rejected:
            with self.subTest(path=path):
                self.assertFalse(mgit.is_valid_design_doc_ref(path))
                self.assertIsNone(mgit.normalize_design_doc_ref(path))


if __name__ == "__main__":
    unittest.main()
