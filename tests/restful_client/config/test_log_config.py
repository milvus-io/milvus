import os
from pathlib import Path
import runpy
import tempfile
import unittest
from unittest.mock import patch


class LogConfigTest(unittest.TestCase):
    def load_config(self, path):
        with patch.dict(os.environ, {"CI_LOG_PATH": str(path)}):
            return runpy.run_path(str(Path(__file__).with_name("log_config.py")))

    def test_concurrent_directory_creation_during_import(self):
        with tempfile.TemporaryDirectory() as root:
            target = Path(root) / "client-logs"
            makedirs = os.makedirs

            def concurrent_makedirs(path, *args, **kwargs):
                # Another worker creates the directory after our isdir check.
                makedirs(path)
                return makedirs(path, *args, **kwargs)

            with patch("os.makedirs", side_effect=concurrent_makedirs):
                self.load_config(target)
            self.assertTrue(target.is_dir())
            self.load_config(target)

    def test_existing_file_is_not_silently_accepted(self):
        with tempfile.TemporaryDirectory() as root:
            target = Path(root) / "client-logs"
            target.write_text("preserve")
            with self.assertRaises(FileExistsError):
                self.load_config(target)
            self.assertEqual(target.read_text(), "preserve")


if __name__ == "__main__":
    unittest.main()
