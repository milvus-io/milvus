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
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// The 2.6 suite has no shared object-storage helper; honor explicit settings and the Go SDK CI service names.
func rlsImportStorage() (*miniogo.Client, string, error) {
	address, bucket := "localhost:9000", "a-bucket"
	milvusAddress := strings.TrimSpace(hp.GetAddr())
	if !strings.Contains(milvusAddress, "://") {
		milvusAddress = "//" + milvusAddress
	}
	if endpoint, err := url.Parse(milvusAddress); err == nil {
		service, namespace, _ := strings.Cut(endpoint.Hostname(), ".")
		if strings.HasPrefix(service, "gosdk-") && strings.HasSuffix(service, "-milvus") {
			host := strings.TrimSuffix(service, "-milvus") + "-minio"
			if namespace != "" {
				host += "." + namespace
			}
			address, bucket = net.JoinHostPort(host, "9000"), "milvus-bucket"
		}
	}
	setting := func(key, fallback string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return fallback
	}
	storage, err := miniogo.New(setting("MINIO_ADDRESS", address), &miniogo.Options{
		Creds:  miniocreds.NewStaticV4(setting("MINIO_ACCESS_KEY", "minioadmin"), setting("MINIO_SECRET_KEY", "minioadmin"), ""),
		Secure: false,
	})
	return storage, setting("MINIO_BUCKET", bucket), err
}

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
				{"id": int64(10), "owner": "alice", "department": "eng", "active": true, "number": int64(10), "vector": entity.FloatVector{10, 1, 2, 3}},
				{"id": int64(11), "owner": "alice", "department": "eng", "active": true, "number": int64(11), "vector": entity.FloatVector{11, 1, 2, 3}},
			}
			if mode == "mixed" || mode == "skip" || mode == "forced_skip" {
				rows[1]["owner"] = "bob"
			}
			storage, bucket, err := rlsImportStorage()
			require.NoError(t, err)
			payload, err := json.Marshal(rows)
			require.NoError(t, err)
			object := "bulkinsert_data/" + name + "/rls.json"
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				require.NoError(t, storage.RemoveObject(cleanup, bucket, object, miniogo.RemoveObjectOptions{}))
			})
			_, err = storage.PutObject(ctx, bucket, object, bytes.NewReader(payload), int64(len(payload)), miniogo.PutObjectOptions{ContentType: "application/json"})
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
				load, err := mc.RefreshLoad(ctx, client.NewRefreshLoadOption(name))
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

func TestRLSBulkImportPolicyCompositionAndSnapshot(t *testing.T) {
	for _, mode := range []string{"combined", "denied_owner", "denied_active", "denied_department", "snapshot_policy", "snapshot_tags"} {
		t.Run(mode, func(t *testing.T) {
			ctx := hp.CreateContext(t, 5*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t).Client
			name := prepareRLSCollection(ctx, t, mc, "", false)
			createRLSPolicy(ctx, t, mc, name, "reader", "true", "", milvuspb.RowPolicyAction_Query)
			snapshot := strings.HasPrefix(mode, "snapshot_")
			if snapshot {
				expr := "owner == $current_principal"
				if mode == "snapshot_tags" {
					expr = "department == $current_principal_tags['department']"
					require.NoError(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"eng"}`)))
				}
				createRLSPolicy(ctx, t, mc, name, "writer", "", expr, milvuspb.RowPolicyAction_Insert)
			} else {
				for i, expr := range []string{"owner == 'alice'", "owner == 'bob'", "active == true", "department == 'eng'"} {
					kind := milvuspb.RowPolicyType_RowPolicyTypePermissive
					if i >= 2 {
						kind = milvuspb.RowPolicyType_RowPolicyTypeRestrictive
					}
					require.NoError(t, mc.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(name, fmt.Sprintf("rule_%d", i), kind, milvuspb.RowPolicyAction_Insert).WithCheckExpr(expr)))
				}
			}
			rows := make([]map[string]any, 4096)
			for i := range rows {
				id := int64(100 + i)
				owner := "alice"
				if !snapshot && i%2 == 1 {
					owner = "bob"
				}
				rows[i] = map[string]any{"id": id, "owner": owner, "department": "eng", "active": true, "number": id, "vector": entity.FloatVector{float32(id), 1, 2, 3}}
			}
			// Put the rejected row last so the valid prefix must never become visible.
			last := rows[len(rows)-1]
			switch mode {
			case "denied_owner":
				last["owner"] = "nobody"
			case "denied_active":
				last["active"] = false
			case "denied_department":
				last["department"] = "sales"
			}
			storage, bucket, err := rlsImportStorage()
			require.NoError(t, err)
			object := "bulkinsert_data/" + name + "/composition.json"
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				require.NoError(t, storage.RemoveObject(cleanup, bucket, object, miniogo.RemoveObjectOptions{}))
			})
			payload, err := json.Marshal(rows)
			require.NoError(t, err)
			_, err = storage.PutObject(ctx, bucket, object, bytes.NewReader(payload), int64(len(payload)), miniogo.PutObjectOptions{ContentType: "application/json"})
			require.NoError(t, err)
			submit := func() int64 {
				response, err := mc.GetService().Import(ctx, &milvuspb.ImportRequest{CollectionName: name, Files: []string{object}, Options: []*commonpb.KeyValuePair{{Key: "rls_principal", Value: "alice"}}})
				require.NoError(t, merr.CheckRPCCall(response, err))
				require.Len(t, response.GetTasks(), 1)
				return response.GetTasks()[0]
			}
			job := submit()
			if snapshot {
				// A visible job has persisted its CHECK snapshot. This does not require a worker pause.
				progress, err := mc.GetService().GetImportState(ctx, &milvuspb.GetImportStateRequest{Task: job})
				require.NoError(t, merr.CheckRPCCall(progress, err))
				if mode == "snapshot_policy" {
					require.NoError(t, mc.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "writer", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Insert).WithCheckExpr("false")))
				} else {
					require.NoError(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"sales"}`)))
				}
			}
			expectedState := commonpb.ImportState_ImportCompleted
			if strings.HasPrefix(mode, "denied_") {
				expectedState = commonpb.ImportState_ImportFailed
			}
			waitRLSImport(ctx, t, mc, job, expectedState)
			if snapshot {
				// A later submission must capture the new policy/tag values and reject the same file.
				waitRLSImport(ctx, t, mc, submit(), commonpb.ImportState_ImportFailed)
			}
			load, err := mc.RefreshLoad(ctx, client.NewRefreshLoadOption(name))
			require.NoError(t, err)
			require.NoError(t, load.Await(ctx))
			expected := []int64{1, 2, 3, 4}
			if expectedState == commonpb.ImportState_ImportCompleted {
				for _, row := range rows {
					expected = append(expected, row["id"].(int64))
				}
			}
			queryRLS(ctx, t, mc, name, "", true, expected...)
			queryRLS(ctx, t, mc, name, "alice", false, expected...)
			result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 100").WithOutputFields("id", "owner", "department", "active", "number", "vector").WithSkipRLS(true).WithConsistencyLevel(entity.ClStrong))
			require.NoError(t, err)
			require.Equal(t, len(expected)-4, result.ResultCount)
			for i := range result.ResultCount {
				id, err := result.GetColumn("id").GetAsInt64(i)
				require.NoError(t, err)
				require.GreaterOrEqual(t, id, int64(100))
				require.Less(t, id, int64(100+len(rows)))
				for field, want := range rows[id-100] {
					got, err := result.GetColumn(field).Get(i)
					require.NoError(t, err)
					require.Equal(t, want, got, "field=%s id=%d", field, id)
				}
			}
		})
	}
}

func waitRLSImport(ctx context.Context, t *testing.T, mc *client.Client, job int64, expected commonpb.ImportState) {
	t.Helper()
	for {
		progress, err := mc.GetService().GetImportState(ctx, &milvuspb.GetImportStateRequest{Task: job})
		require.NoError(t, merr.CheckRPCCall(progress, err))
		reason := ""
		for _, info := range progress.GetInfos() {
			if info.GetKey() == "failed_reason" {
				reason = info.GetValue()
			}
		}
		// The public v1 projection maps intermediate v2 states to Failed without a reason.
		if progress.GetState() == commonpb.ImportState_ImportCompleted || (progress.GetState() == commonpb.ImportState_ImportFailed && reason != "") {
			require.Equal(t, expected, progress.GetState(), reason)
			if expected == commonpb.ImportState_ImportFailed {
				require.Contains(t, reason, "RLS")
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Import did not terminate: %v", progress)
		case <-time.After(time.Second):
		}
	}
}
