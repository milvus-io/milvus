# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Struct array schema rbac regression cases."""

import secrets
import time

import grpc
import pytest
from common import common_func as cf
from common.common_type import CaseLabel
from common.struct_array_isolation_helpers import StructArrayIsolationHelpersBase
from common.struct_array_nullable_helpers import (
    _clips,
    _core_children,
    _record,
)
from pymilvus import DataType, MilvusClient
from pymilvus.exceptions import MilvusException


class TestMilvusClientStructArraySchemaRbac(StructArrayIsolationHelpersBase):
    @pytest.mark.tags(CaseLabel.L3, CaseLabel.RBAC)
    @pytest.mark.parametrize("operation", ["add", "drop", "alter"])
    def test_struct_schema_permissions_grant_and_revoke(self, operation):
        """
        target: enforce DDL privileges on nullable Struct children for an authenticated reader
        method: denied request and owner readback, grant/same-session success, revoke/fresh-session denial
        expected: code 7 at authorization, no denied mutation, exact rows after allowed DDL and reload
        requires: authorizationEnabled=true and an administrator connection (dedicated RBAC lane)
        """
        owner = self._client()
        name = self._create(owner, _core_children())
        model = self._seed(owner, name, [_record(n, _clips(n)) for n in range(12)])
        user, role = cf.gen_unique_str("struct_user"), cf.gen_unique_str("struct_role")
        password = secrets.token_urlsafe(24)
        uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        owner.create_user(user, password)
        owner.create_role(role)
        owner.grant_role(user, role)
        owner.grant_privilege_v2(role, "CollectionReadOnly", name, db_name="default")
        privilege = "CreateCollection" if operation == "alter" else "AlterCollectionSchema"
        scope = {
            "collection_name": "*" if operation == "alter" else name,
            "db_name": "*" if operation == "alter" else "default",
        }
        actors = []

        def invoke(actor, second=False):
            if operation == "add":
                actor.add_collection_field(
                    name, "clips[second]" if second else "clips[added]", DataType.INT64, nullable=True, timeout=20
                )
            elif operation == "drop":
                actor.drop_collection_field(name, field_name="clips[label]" if second else "clips[score]", timeout=20)
            else:
                actor.alter_collection_field(
                    name, "clips[score]", {"field.description": "second" if second else "allowed"}, timeout=20
                )

        try:
            actor = MilvusClient(uri, user=user, password=password)
            actors.append(actor)
            self._check(actor, name, model)
            before = owner.describe_collection(name)
            with pytest.raises((MilvusException, grpc.RpcError)) as denied:
                invoke(actor)
            assert self._permission_denied(denied.value), denied.value
            assert owner.describe_collection(name) == before
            self._check(owner, name, model)
            owner.grant_privilege_v2(role, privilege, **scope)
            deadline = time.monotonic() + 30
            while True:
                try:
                    invoke(actor)
                    break
                except (MilvusException, grpc.RpcError) as exc:
                    assert self._permission_denied(exc) and time.monotonic() < deadline, exc
                    time.sleep(0.5)
            for row in model.values():
                for element in row["clips"] or []:
                    if operation == "add":
                        element["added"] = None
                    elif operation == "drop":
                        del element["score"]
            self._reload(owner, name)
            self._check(owner, name, model)
            owner.revoke_privilege_v2(role, privilege, **scope)
            fresh = MilvusClient(uri, user=user, password=password)
            actors.append(fresh)
            before = owner.describe_collection(name)
            with pytest.raises((MilvusException, grpc.RpcError)) as denied:
                invoke(fresh, second=True)
            assert self._permission_denied(denied.value), denied.value
            assert owner.describe_collection(name) == before
            self._check(owner, name, model)
        finally:
            for actor in actors:
                actor.close()
            owner.revoke_role(user, role)
            owner.drop_user(user)
            owner.revoke_privilege_v2(role, privilege, **scope)
            owner.revoke_privilege_v2(role, "CollectionReadOnly", name, db_name="default")
            owner.drop_role(role)
