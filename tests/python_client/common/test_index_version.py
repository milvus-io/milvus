import unittest

from common.index_version import get_resolved_scalar_index_version, get_segment_max_size


class IndexVersionTestCase(unittest.TestCase):
    def test_get_resolved_scalar_index_version(self):
        nodes = [
            {
                "infos": {
                    "type": "datacoord",
                    "system_configurations": {
                        "segment_max_size": 1024,
                        "resolved_scalar_index_version": 3,
                    },
                }
            }
        ]

        self.assertEqual(3, get_resolved_scalar_index_version(nodes))

    def test_get_resolved_scalar_index_version_returns_none_when_unavailable(self):
        nodes = [
            {
                "infos": {
                    "type": "datacoord",
                    "system_configurations": {"segment_max_size": 1024},
                }
            }
        ]

        self.assertIsNone(get_resolved_scalar_index_version(nodes))

    def test_get_segment_max_size(self):
        nodes = [
            {
                "infos": {
                    "type": "datacoord",
                    "system_configurations": {"segment_max_size": "64"},
                }
            }
        ]

        self.assertEqual(64, get_segment_max_size(nodes))

    def test_get_segment_max_size_returns_none_when_unavailable(self):
        nodes = [{"infos": {"type": "querynode", "system_configurations": {}}}]

        self.assertIsNone(get_segment_max_size(nodes))


if __name__ == "__main__":
    unittest.main()
