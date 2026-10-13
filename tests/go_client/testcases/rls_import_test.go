// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testcases

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/entity"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// CI enables proxy.rls.importEnforcementEnabled for the freshly deployed build.
// Import and GetImportState are public RPCs exposed by the SDK service client.
func TestRLSBulkImportChecks(t *testing.T) {
	for _, mode := range []string{"owner", "tags", "mixed", "missing_principal", "no_insert", "skip", "forced_skip"} {
		t.Run(mode, func(t *testing.T) {
			ctx := hp.CreateContext(t, 5*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t).Client
			name := prepareRLSCollection(ctx, t, mc, "", false)
			expr := "owner == $current_principal"
			if mode == "tags" {
				expr = "department == $current_principal_tags['department']"
				require.NoError(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"eng"}`)))
			}
			actions := []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query}
			check := ""
			if mode != "no_insert" {
				actions = append(actions, milvuspb.RowPolicyAction_Insert)
				check = expr
			}
			createRLSPolicy(ctx, t, mc, name, "importer", expr, check, actions...)
			rows := []map[string]any{
				{"id": int64(10), "owner": "alice", "department": "eng", "active": true, "number": int64(10), "vector": []float32{10, 1, 2, 3}},
				{"id": int64(11), "owner": "alice", "department": "eng", "active": true, "number": int64(11), "vector": []float32{11, 1, 2, 3}},
			}
			if mode == "mixed" || mode == "skip" || mode == "forced_skip" {
				rows[1]["owner"] = "bob"
			}
			cfg := getMinIOConfig()
			storage, err := newMinIOClient(cfg)
			require.NoError(t, err)
			payload, err := json.Marshal(rows)
			require.NoError(t, err)
			object := "bulkinsert_data/" + name + "/rls.json"
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				require.NoError(t, storage.RemoveObject(cleanup, cfg.bucket, object, miniogo.RemoveObjectOptions{}))
			})
			_, err = storage.PutObject(ctx, cfg.bucket, object, bytes.NewReader(payload), int64(len(payload)), miniogo.PutObjectOptions{ContentType: "application/json"})
			require.NoError(t, err)
			var options []*commonpb.KeyValuePair
			if mode != "missing_principal" {
				options = append(options, &commonpb.KeyValuePair{Key: "rls_principal", Value: "alice"})
			}
			if mode == "skip" || mode == "forced_skip" {
				options = append(options, &commonpb.KeyValuePair{Key: "skip_rls", Value: "true"})
			}
			if mode == "forced_skip" {
				require.NoError(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("rls.force", true)))
			}
			response, err := mc.GetService().Import(ctx, &milvuspb.ImportRequest{CollectionName: name, Files: []string{object}, Options: options})
			expected := []int64{1, 2, 3, 4}
			if mode == "missing_principal" || mode == "forced_skip" {
				message := "rls_principal"
				if mode == "forced_skip" {
					message = "rls.force"
				}
				require.ErrorContains(t, merr.CheckRPCCall(response, err), message)
			} else {
				require.NoError(t, merr.CheckRPCCall(response, err))
				require.Len(t, response.GetTasks(), 1)
				var progress *milvuspb.GetImportStateResponse
				var reason string
				for {
					progress, err = mc.GetService().GetImportState(ctx, &milvuspb.GetImportStateRequest{Task: response.GetTasks()[0]})
					require.NoError(t, merr.CheckRPCCall(progress, err))
					for _, info := range progress.GetInfos() {
						if info.GetKey() == "failed_reason" {
							reason = info.GetValue()
						}
					}
					// The public v1 projection maps intermediate v2 states to ImportFailed without a reason.
					if progress.GetState() == commonpb.ImportState_ImportCompleted || (progress.GetState() == commonpb.ImportState_ImportFailed && reason != "") {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatalf("Import did not terminate: %v", progress)
					case <-time.After(time.Second):
					}
				}
				if mode == "mixed" || mode == "no_insert" {
					require.Equal(t, commonpb.ImportState_ImportFailed, progress.GetState())
					require.Contains(t, reason, "RLS")
					if mode == "no_insert" {
						require.Contains(t, reason, "no applicable")
					}
				} else {
					require.Equal(t, commonpb.ImportState_ImportCompleted, progress.GetState(), reason)
					expected = append(expected, 10, 11)
				}
				load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name).WithRefresh(true))
				require.NoError(t, err)
				require.NoError(t, load.Await(ctx))
			}
			if mode == "forced_skip" {
				require.NoError(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("rls.force", false)))
			}
			queryRLS(ctx, t, mc, name, "", true, expected...)
			visible := []int64{1, 3}
			if mode == "tags" {
				visible = append(visible, 4)
			}
			if mode == "owner" || mode == "tags" {
				visible = append(visible, 10, 11)
			} else if mode == "skip" {
				visible = append(visible, 10)
			}
			queryRLS(ctx, t, mc, name, "alice", false, visible...)
			if len(expected) == 6 {
				result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id in [10,11]").WithOutputFields("id", "owner", "department", "active", "number", "vector").WithSkipRLS(true).WithConsistencyLevel(entity.ClStrong))
				require.NoError(t, err)
				require.Equal(t, 2, result.ResultCount)
				for i := range result.ResultCount {
					id, err := result.GetColumn("id").GetAsInt64(i)
					require.NoError(t, err)
					for field, want := range rows[id-10] {
						got, err := result.GetColumn(field).Get(i)
						require.NoError(t, err)
						require.Equal(t, want, got, "field=%s id=%d", field, id)
					}
				}
			}
		})
	}
}
