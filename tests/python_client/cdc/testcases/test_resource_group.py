"""Resource groups are local; replica load configuration is replicated."""

import pytest
from common.common_type import CaseLabel
from pymilvus import MilvusException

pytestmark = pytest.mark.tags(CaseLabel.CDC, CaseLabel.L3)


class TestCDCSyncResourceGroup:
    @pytest.mark.parametrize("role", ["upstream", "downstream"])
    def test_resource_group_create_update_drop_are_local(self, cdc_contract, role):
        c = cdc_contract
        client = getattr(c, role)
        peer = c.downstream if role == "upstream" else c.upstream
        name = c.name("local_rg")
        config = {"requests": {"node_num": 0}, "limits": {"node_num": 0}}

        def cleanup(target):
            if name in target.list_resource_groups():
                target.update_resource_groups({name: config})
                target.drop_resource_group(name)

        for target in (client, peer):
            c.request.addfinalizer(lambda target=target: cleanup(target))
        client.create_resource_group(name, config=config)
        c.control_barrier()
        assert name in client.list_resource_groups()
        assert name not in peer.list_resource_groups()
        peer.create_resource_group(name, config=config)
        before = peer.describe_resource_group(name).config
        changed = {"requests": {"node_num": 0}, "limits": {"node_num": 1}}
        client.update_resource_groups({name: changed})
        assert client.describe_resource_group(name).config.limits.node_num == 1
        c.control_barrier()
        assert peer.describe_resource_group(name).config == before
        client.update_resource_groups({name: config})
        client.drop_resource_group(name)
        c.control_barrier()
        assert name not in client.list_resource_groups()
        assert name in peer.list_resource_groups()

    def test_transfer_replica_broadcast_and_secondary_rejection(self, cdc_contract):
        c = cdc_contract
        group = c.name("replica_target")
        for client in (c.upstream, c.downstream):
            c.request.addfinalizer(lambda client=client: client.drop_resource_group(group))
            client.create_resource_group(group)
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        default = "__default_resource_group"

        def groups(client):
            return sorted(info.resource_group for info in client.describe_replica(name, timeout=10))

        with c.unchanged(name):
            with pytest.raises(MilvusException, match="(?i)not primary|secondary"):
                c.downstream.transfer_replica(default, group, name, 1, timeout=10)
        assert groups(c.upstream) == groups(c.downstream) == [default]
        try:
            # Transfer the test collection's assignment, without moving shared query nodes.
            c.upstream.transfer_replica(default, group, name, 1, timeout=30)

            def transferred():
                assert groups(c.upstream) == [group]
                # Default streaming.replication.useLocalReplicaConfig=true.
                assert groups(c.downstream) == [default]

            c.wait(transferred, "replica resource-group assignment")
        finally:
            c.upstream.transfer_replica(group, default, name, 1, timeout=30)
        c.assert_rows(name, c.data())
