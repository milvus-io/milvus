"""CDC control-plane replication and explicitly local API contracts."""

import json
import time
from contextlib import closing

import grpc
import pytest
from common.common_type import CaseLabel
from pymilvus import MilvusClient, MilvusException
from pymilvus.client.utils import check_status
from pymilvus.grpc_gen import common_pb2, milvus_pb2, milvus_pb2_grpc

from .contract_helpers import rows_equal, rpc

pytestmark = pytest.mark.tags(CaseLabel.CDC, CaseLabel.L3)


class TestCDCControlAPI:
    def test_restore_rbac_and_alter_role_enforce_downstream_permissions(self, cdc_contract, downstream_uri):
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        role, user = c.name("restore_role"), c.name("restore_user", max_length=31)
        password = "CDC_test_password_482!"
        c.request.addfinalizer(lambda: c.upstream.drop_role(role, force_drop=True))
        c.request.addfinalizer(lambda: c.upstream.drop_user(user))
        c.upstream.create_role(role, description="before restore")
        c.upstream.create_user(user, password)
        c.upstream.grant_role(user, role)
        c.upstream.grant_privilege(role, "Collection", "Query", name, db_name="default")
        backup = rpc(c.upstream, "BackupRBAC", milvus_pb2.BackupRBACMetaRequest()).RBAC_meta
        # Restore only test-owned objects, never the cluster's complete backup.
        meta = milvus_pb2.RBACMeta(
            users=[u for u in backup.users if u.user == user],
            roles=[r for r in backup.roles if r.name == role],
            grants=[g for g in backup.grants if g.role.name == role],
        )
        assert len(meta.users) == len(meta.roles) == len(meta.grants) == 1
        c.upstream.drop_user(user)
        c.upstream.drop_role(role, force_drop=True)
        c.control_barrier()
        assert user not in c.downstream.list_users()
        assert role not in c.downstream.list_roles()
        rpc(c.upstream, "RestoreRBAC", milvus_pb2.RestoreRBACMetaRequest(RBAC_meta=meta))
        c.upstream.alter_role(role, "restored and altered")

        forbidden = c.name("forbidden")
        c.request.addfinalizer(lambda: c.upstream.drop_collection(forbidden))

        def check():
            assert c.downstream.describe_role(role)["description"] == "restored and altered"
            assert role in c.downstream.describe_user(user)["roles"]
            with closing(MilvusClient(uri=downstream_uri, token=f"{user}:{password}")) as client:
                rows_equal(client, name, c.data())
                with pytest.raises(grpc.RpcError) as denied:
                    client.create_collection(forbidden, dimension=4, timeout=10)
                assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
                assert not c.downstream.has_collection(forbidden)

        c.wait(check, "restored credentials, role and effective query privilege")
        before = c.downstream.describe_role(role)
        with pytest.raises(MilvusException, match="(?i)not primary|secondary"):
            c.downstream.alter_role(role, "forbidden", timeout=10)
        with pytest.raises(MilvusException, match="(?i)not primary|secondary"):
            rpc(c.downstream, "RestoreRBAC", milvus_pb2.RestoreRBACMetaRequest(RBAC_meta=meta))
        c.control_barrier()
        assert c.downstream.describe_role(role) == before
        c.upstream.revoke_privilege(role, "Collection", "Query", name, db_name="default")

        def revoked():
            with closing(MilvusClient(uri=downstream_uri, token=f"{user}:{password}")) as client:
                try:
                    client.query(name, filter="id >= 0", timeout=10)
                except grpc.RpcError as denied:
                    assert denied.code() == grpc.StatusCode.PERMISSION_DENIED
                else:
                    raise AssertionError("Revoked query privilege is still accepted")

        c.wait(revoked, "revoked query permission on a new downstream connection")
        c.assert_rows(name, c.data())

    @pytest.mark.parametrize("role", ["upstream", "downstream"])
    def test_phrase_match_slop_is_available_on_both_roles(self, cdc_contract, role):
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        with c.unchanged(name):
            result = rpc(
                getattr(c, role),
                "ComputePhraseMatchSlop",
                milvus_pb2.ComputePhraseMatchSlopRequest(
                    analyzer_params=json.dumps({"tokenizer": "standard"}),
                    query_text="red blue",
                    data_texts=["red blue", "red green blue", "yellow"],
                ),
            )
            assert list(result.is_match) == [True, True, False]
            assert list(result.slops)[:2] == [0, 1]

    @pytest.mark.parametrize("role", ["upstream", "downstream"])
    def test_telemetry_and_client_commands_stay_local(self, cdc_contract, role):
        c = cdc_contract
        client = getattr(c, role)
        peer = c.downstream if role == "upstream" else c.upstream
        client_id = c.name("telemetry")

        def call(target, method, request):
            stub = milvus_pb2_grpc.ClientTelemetryServiceStub(target._get_connection()._final_channel)
            response = getattr(stub, method)(request, timeout=15)
            check_status(response.status)
            return response

        heartbeat = milvus_pb2.ClientHeartbeatRequest(
            client_info=common_pb2.ClientInfo(
                sdk_type="CDC-test",
                sdk_version="1",
                host="cdc-test",
                reserved={"client_id": client_id, "db_name": "default"},
            ),
            report_timestamp=int(time.time() * 1000),
        )
        call(client, "ClientHeartbeat", heartbeat)
        query = milvus_pb2.GetClientTelemetryRequest(client_id=client_id, include_metrics=True)
        assert len(call(client, "GetClientTelemetry", query).clients) == 1
        c.control_barrier()
        assert not call(peer, "GetClientTelemetry", query).clients
        pushed = call(
            client,
            "PushClientCommand",
            milvus_pb2.PushClientCommandRequest(
                command_type="push_config",
                payload=b'{"sampling_rate":0.5}',
                target_client_id=client_id,
                ttl_seconds=120,
                persistent=False,
            ),
        )
        delete = milvus_pb2.DeleteClientCommandRequest(command_id=pushed.command_id)
        c.request.addfinalizer(lambda: call(client, "DeleteClientCommand", delete))
        c.control_barrier()
        assert pushed.command_id in [cmd.command_id for cmd in call(client, "ClientHeartbeat", heartbeat).commands]
        assert pushed.command_id not in [cmd.command_id for cmd in call(peer, "ClientHeartbeat", heartbeat).commands]
        call(client, "DeleteClientCommand", delete)
        assert pushed.command_id not in [cmd.command_id for cmd in call(client, "ClientHeartbeat", heartbeat).commands]
