"""
CDC topology setup and configuration test cases.
"""

import time
import pytest
from .base import TestCDCSyncBase


@pytest.mark.skip(reason="Skip topology setup test")
class TestCDCTopologySetup(TestCDCSyncBase):
    """Test CDC topology setup, switching, and configuration management."""

    def setup_method(self):
        """Setup for each test method."""
        self.resources_to_cleanup = []

    def teardown_method(self):
        """Cleanup after each test method."""
        # Cleanup will be handled by individual test methods as needed
        pass

    def test_normal_topology_setup(
        self,
        upstream_client,
        downstream_client,
        upstream_uri,
        downstream_uri,
        upstream_token,
        downstream_token,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """Test case 1: Normal upstream/downstream topology setup."""
        # Create a basic topology configuration
        config = {
            "clusters": [
                {
                    "cluster_id": source_cluster_id,
                    "connection_param": {"uri": upstream_uri, "token": upstream_token},
                    "pchannels": [
                        f"{source_cluster_id}-rootcoord-dml_{i}"
                        for i in range(pchannel_num)
                    ],
                },
                {
                    "cluster_id": target_cluster_id,
                    "connection_param": {
                        "uri": downstream_uri,
                        "token": downstream_token,
                    },
                    "pchannels": [
                        f"{target_cluster_id}-rootcoord-dml_{i}"
                        for i in range(pchannel_num)
                    ],
                },
            ],
            "cross_cluster_topology": [
                {
                    "source_cluster_id": source_cluster_id,
                    "target_cluster_id": target_cluster_id,
                }
            ],
        }

        # Test normal topology setup
        upstream_client.update_replicate_configuration(**config)
        downstream_client.update_replicate_configuration(**config)

        # Wait for configuration to take effect
        time.sleep(3)

        # Verify topology is working by creating a test collection
        test_collection_name = self.gen_unique_name("topology_test")
        self.resources_to_cleanup.append(("collection", test_collection_name))

        # Create collection on upstream
        upstream_client.create_collection(
            collection_name=test_collection_name,
            schema=self.create_default_schema(upstream_client),
        )

        # Verify it syncs to downstream
        def check_sync():
            return downstream_client.has_collection(test_collection_name)

        assert self.wait_for_sync(
            check_sync, 30, f"collection {test_collection_name} sync"
        )

        # Cleanup
        self.cleanup_collection(upstream_client, test_collection_name)

    def test_switch_upstream_downstream(
        self,
        upstream_client,
        downstream_client,
        upstream_uri,
        downstream_uri,
        upstream_token,
        downstream_token,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """Test case 2: Switch upstream/downstream after initial setup."""
        # First setup normal topology (source -> target)
        original_config = {
            "clusters": [
                {
                    "cluster_id": source_cluster_id,
                    "connection_param": {"uri": upstream_uri, "token": upstream_token},
                    "pchannels": [
                        f"{source_cluster_id}-rootcoord-dml_{i}"
                        for i in range(pchannel_num)
                    ],
                },
                {
                    "cluster_id": target_cluster_id,
                    "connection_param": {
                        "uri": downstream_uri,
                        "token": downstream_token,
                    },
                    "pchannels": [
                        f"{target_cluster_id}-rootcoord-dml_{i}"
                        for i in range(pchannel_num)
                    ],
                },
            ],
            "cross_cluster_topology": [
                {
                    "source_cluster_id": source_cluster_id,
                    "target_cluster_id": target_cluster_id,
                }
            ],
        }

        upstream_client.update_replicate_configuration(**original_config)
        downstream_client.update_replicate_configuration(**original_config)
        time.sleep(3)

        # Now switch the direction (target -> source)
        switched_config = {
            "clusters": [
                {
                    "cluster_id": source_cluster_id,
                    "connection_param": {"uri": upstream_uri, "token": upstream_token},
                    "pchannels": [
                        f"{source_cluster_id}-rootcoord-dml_{i}"
                        for i in range(pchannel_num)
                    ],
                },
                {
                    "cluster_id": target_cluster_id,
                    "connection_param": {
                        "uri": downstream_uri,
                        "token": downstream_token,
                    },
                    "pchannels": [
                        f"{target_cluster_id}-rootcoord-dml_{i}"
                        for i in range(pchannel_num)
                    ],
                },
            ],
            "cross_cluster_topology": [
                {
                    "source_cluster_id": target_cluster_id,  # Switched
                    "target_cluster_id": source_cluster_id,  # Switched
                }
            ],
        }

        # Apply switched configuration
        upstream_client.update_replicate_configuration(**switched_config)
        downstream_client.update_replicate_configuration(**switched_config)
        time.sleep(3)

        # Test the switched topology by creating collection on downstream (now source)
        test_collection_name = self.gen_unique_name("switch_test")
        self.resources_to_cleanup.append(("collection", test_collection_name))

        downstream_client.create_collection(
            collection_name=test_collection_name,
            schema=self.create_default_schema(downstream_client),
        )

        # Verify it syncs to upstream (now target)
        def check_switched_sync():
            return upstream_client.has_collection(test_collection_name)

        assert self.wait_for_sync(
            check_switched_sync, 30, f"switched collection {test_collection_name} sync"
        )

        # Cleanup
        self.cleanup_collection(downstream_client, test_collection_name)

    def test_invalid_topology_structures(
        self,
        upstream_client,
        downstream_client,
        upstream_uri,
        downstream_uri,
        upstream_token,
        downstream_token,
        source_cluster_id,
        target_cluster_id,
    ):
        """Test case 4: Invalid topology structure cases."""

        # Test case 4.1: Missing cluster in cross_cluster_topology
        invalid_config_1 = {
            "clusters": [
                {
                    "cluster_id": source_cluster_id,
                    "connection_param": {"uri": upstream_uri, "token": upstream_token},
                    "pchannels": [f"{source_cluster_id}-rootcoord-dml_0"],
                }
            ],
            "cross_cluster_topology": [
                {
                    "source_cluster_id": source_cluster_id,
                    "target_cluster_id": "nonexistent_cluster",  # This cluster is not defined
                }
            ],
        }

        with pytest.raises(Exception):
            upstream_client.update_replicate_configuration(**invalid_config_1)

        # Test case 4.2: Circular dependency (A -> B, B -> A)
        invalid_config_2 = {
            "clusters": [
                {
                    "cluster_id": source_cluster_id,
                    "connection_param": {"uri": upstream_uri, "token": upstream_token},
                    "pchannels": [f"{source_cluster_id}-rootcoord-dml_0"],
                },
                {
                    "cluster_id": target_cluster_id,
                    "connection_param": {
                        "uri": downstream_uri,
                        "token": downstream_token,
                    },
                    "pchannels": [f"{target_cluster_id}-rootcoord-dml_0"],
                },
            ],
            "cross_cluster_topology": [
                {
                    "source_cluster_id": source_cluster_id,
                    "target_cluster_id": target_cluster_id,
                },
                {
                    "source_cluster_id": target_cluster_id,
                    "target_cluster_id": source_cluster_id,  # Circular dependency
                },
            ],
        }

        # This may or may not fail depending on implementation, but test it
        with pytest.raises(Exception):
            upstream_client.update_replicate_configuration(**invalid_config_2)

        # Test case 4.3: Invalid connection parameters
        invalid_config_3 = {
            "clusters": [
                {
                    "cluster_id": source_cluster_id,
                    "connection_param": {
                        "uri": "http://invalid-host:19530",  # Invalid URI
                        "token": "invalid:token",
                    },
                    "pchannels": [f"{source_cluster_id}-rootcoord-dml_0"],
                },
                {
                    "cluster_id": target_cluster_id,
                    "connection_param": {
                        "uri": downstream_uri,
                        "token": downstream_token,
                    },
                    "pchannels": [f"{target_cluster_id}-rootcoord-dml_0"],
                },
            ],
            "cross_cluster_topology": [
                {
                    "source_cluster_id": source_cluster_id,
                    "target_cluster_id": target_cluster_id,
                }
            ],
        }
        with pytest.raises(Exception):
            upstream_client.update_replicate_configuration(**invalid_config_3)

        # Test case 4.4: Empty cluster list but non-empty topology
        invalid_config_4 = {
            "clusters": [],  # Empty clusters
            "cross_cluster_topology": [
                {
                    "source_cluster_id": source_cluster_id,
                    "target_cluster_id": target_cluster_id,
                }
            ],
        }
        with pytest.raises(Exception):
            upstream_client.update_replicate_configuration(**invalid_config_4)

        # Test case 4.5: Invalid pchannel format
        invalid_config_5 = {
            "clusters": [
                {
                    "cluster_id": source_cluster_id,
                    "connection_param": {"uri": upstream_uri, "token": upstream_token},
                    "pchannels": ["invalid-channel-format"],  # Invalid format
                }
            ],
            "cross_cluster_topology": [],
        }
        with pytest.raises(Exception):
            upstream_client.update_replicate_configuration(**invalid_config_5)

    @pytest.mark.order(-1)
    def test_clear_configuration_disconnect(
        self,
        upstream_client,
        downstream_client,
        upstream_uri,
        downstream_uri,
        upstream_token,
        downstream_token,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """Test case 3: Clear configuration and disconnect CDC."""
        # First setup normal topology
        config = {
            "clusters": [
                {
                    "cluster_id": source_cluster_id,
                    "connection_param": {"uri": upstream_uri, "token": upstream_token},
                    "pchannels": [
                        f"{source_cluster_id}-rootcoord-dml_{i}"
                        for i in range(pchannel_num)
                    ],
                },
                {
                    "cluster_id": target_cluster_id,
                    "connection_param": {
                        "uri": downstream_uri,
                        "token": downstream_token,
                    },
                    "pchannels": [
                        f"{target_cluster_id}-rootcoord-dml_{i}"
                        for i in range(pchannel_num)
                    ],
                },
            ],
            "cross_cluster_topology": [
                {
                    "source_cluster_id": source_cluster_id,
                    "target_cluster_id": target_cluster_id,
                }
            ],
        }

        upstream_client.update_replicate_configuration(**config)
        downstream_client.update_replicate_configuration(**config)
        time.sleep(3)

        # Verify topology is working
        test_collection_name = self.gen_unique_name("clear_config_test")
        upstream_client.create_collection(
            collection_name=test_collection_name,
            schema=self.create_default_schema(upstream_client),
        )

        def check_initial_sync():
            return downstream_client.has_collection(test_collection_name)

        assert self.wait_for_sync(
            check_initial_sync, 30, f"initial collection {test_collection_name} sync"
        )

        # Now clear the configuration (empty topology)
        empty_upstream_config = {
            "clusters": [
                {
                    "cluster_id": source_cluster_id,
                    "connection_param": {"uri": upstream_uri, "token": upstream_token},
                    "pchannels": [
                        f"{source_cluster_id}-rootcoord-dml_{i}"
                        for i in range(pchannel_num)
                    ],
                }
            ],
            "cross_cluster_topology": [],
        }
        empty_downstream_config = {
            "clusters": [
                {
                    "cluster_id": target_cluster_id,
                    "connection_param": {
                        "uri": downstream_uri,
                        "token": downstream_token,
                    },
                    "pchannels": [
                        f"{target_cluster_id}-rootcoord-dml_{i}"
                        for i in range(pchannel_num)
                    ],
                }
            ],
            "cross_cluster_topology": [],
        }
        # Apply empty configuration to disconnect CDC
        upstream_client.update_replicate_configuration(**empty_upstream_config)
        downstream_client.update_replicate_configuration(**empty_downstream_config)
        time.sleep(3)

        # Test that CDC is disconnected - create new collection and verify it doesn't sync
        disconnected_collection_name = self.gen_unique_name("disconnected_test")
        upstream_client.create_collection(
            collection_name=disconnected_collection_name,
            schema=self.create_default_schema(upstream_client),
        )

        # Wait a reasonable time and verify it doesn't sync
        time.sleep(10)
        assert not downstream_client.has_collection(disconnected_collection_name), (
            "Collection should not sync after CDC disconnection"
        )

        # Cleanup
        self.cleanup_collection(upstream_client, test_collection_name)
        self.cleanup_collection(upstream_client, disconnected_collection_name)
