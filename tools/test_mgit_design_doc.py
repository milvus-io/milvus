import importlib.util
import pathlib
import unittest


MGIT_PATH = pathlib.Path(__file__).with_name("mgit.py")
spec = importlib.util.spec_from_file_location("mgit", MGIT_PATH)
mgit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mgit)


class DesignDocRefTest(unittest.TestCase):
    def test_accepts_in_repo_markdown_path(self):
        self.assertTrue(
            mgit.is_valid_design_doc_ref(
                "docs/design-docs/design_docs/20260128-vector-compression.md"
            )
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
        self.assertFalse(mgit.is_valid_design_doc_ref("docs/design-docs/design_docs/"))
        self.assertFalse(
            mgit.is_valid_design_doc_ref(
                "docs/design-docs/design_docs/20260128-vector-compression.txt"
            )
        )


if __name__ == "__main__":
    unittest.main()
