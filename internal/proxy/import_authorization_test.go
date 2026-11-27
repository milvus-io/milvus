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

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type importAuthorizationCoordinator struct {
	*metadataPrivilegeCoordinator
	progress    *internalpb.GetImportProgressResponse
	list        *internalpb.ListImportsResponse
	progressErr error
	listErr     error
	describe    func(*milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	show        func(*milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error)
	listRequest *internalpb.ListImportsRequestInternal
}

func (c *importAuthorizationCoordinator) ShowCollections(_ context.Context, req *milvuspb.ShowCollectionsRequest, _ ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	if c.show != nil {
		return c.show(req)
	}
	ids := []int64{41}
	if req.GetDbName() == "tenant_a" {
		ids = []int64{43, 44}
	}
	return &milvuspb.ShowCollectionsResponse{Status: merr.Success(), CollectionIds: ids}, nil
}

func (c *importAuthorizationCoordinator) GetImportProgress(context.Context, *internalpb.GetImportProgressRequest, ...grpc.CallOption) (*internalpb.GetImportProgressResponse, error) {
	if c.progressErr != nil {
		return nil, c.progressErr
	}
	if c.progress != nil {
		return proto.Clone(c.progress).(*internalpb.GetImportProgressResponse), nil
	}
	return &internalpb.GetImportProgressResponse{
		Status: merr.Success(), State: internalpb.ImportJobState_Failed,
		CollectionName: "records", CollectionId: 41, Reason: "cannot read staging/tenant_b/private.json",
	}, nil
}

func (c *importAuthorizationCoordinator) ListImports(_ context.Context, req *internalpb.ListImportsRequestInternal, _ ...grpc.CallOption) (*internalpb.ListImportsResponse, error) {
	c.listRequest = req
	if c.listErr != nil {
		return nil, c.listErr
	}
	if c.list != nil {
		return proto.Clone(c.list).(*internalpb.ListImportsResponse), nil
	}
	return &internalpb.ListImportsResponse{
		Status: merr.Success(), JobIDs: []string{"100"},
		States:          []internalpb.ImportJobState{internalpb.ImportJobState_Failed},
		CollectionNames: []string{"records"}, CollectionIds: []int64{41}, Progresses: []int64{0},
		Reasons: []string{"cannot read staging/tenant_b/private.json"},
	}, nil
}

func (c *importAuthorizationCoordinator) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	if c.describe != nil {
		return c.describe(req)
	}
	resp, err := c.metadataPrivilegeCoordinator.DescribeCollection(ctx, req, opts...)
	resp.UpdateTimestamp = 100
	resp.UpdateTimestampStr = "100"
	return resp, err
}

// Exercise the public Proxy entry points and real RBAC policy evaluation.
// The authenticated user either has no grant or Import on an identically
// named collection in another database. Neither may receive job metadata.
func TestImportMetadataRequiresResourcePrivilege(t *testing.T) {
	for _, user := range []string{"no_roles", "reader"} {
		t.Run(user, func(t *testing.T) {
			coord, setPolicy := setupMetadataPrivileges(t)
			setPolicy("tenant_a", commonpb.ObjectPrivilege_PrivilegeImport)
			node := &Proxy{mixCoord: &importAuthorizationCoordinator{metadataPrivilegeCoordinator: coord}}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			ctx := NewContextWithMetadata(context.Background(), user, "tenant_a")
			t.Run("progress", func(t *testing.T) {
				resp, err := node.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{DbName: "tenant_a", JobID: "100"})
				require.NoError(t, err)
				require.Error(t, merr.Error(resp.GetStatus()))
				require.Empty(t, resp.GetReason())
				require.Empty(t, resp.GetCollectionName())
			})
			t.Run("legacy_progress", func(t *testing.T) {
				resp, err := node.GetImportState(ctx, &milvuspb.GetImportStateRequest{Task: 100})
				require.NoError(t, err)
				require.Error(t, merr.Error(resp.GetStatus()))
				require.Empty(t, resp.GetInfos())
			})
			t.Run("list", func(t *testing.T) {
				resp, err := node.ListImports(ctx, &internalpb.ListImportsRequest{DbName: "tenant_a"})
				require.NoError(t, err)
				require.Empty(t, resp.GetJobIDs())
				require.Empty(t, resp.GetReasons())
			})
			t.Run("legacy_list", func(t *testing.T) {
				resp, err := node.ListImportTasks(ctx, &milvuspb.ListImportTasksRequest{DbName: "tenant_a"})
				require.NoError(t, err)
				require.Empty(t, resp.GetTasks())
			})
		})
	}
}

func TestImportMetadataPositiveControls(t *testing.T) {
	for _, mode := range []string{"granted", "legacy_zero_revision", "root", "auth_disabled", "root_bound_without_grant"} {
		t.Run(mode, func(t *testing.T) {
			coord, setPolicy := setupMetadataPrivileges(t)
			user := "reader"
			switch mode {
			case "granted", "legacy_zero_revision":
				setPolicy("tenant_b", commonpb.ObjectPrivilege_PrivilegeImport)
			case "root":
				user = "root"
			case "auth_disabled":
				require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false"))
			case "root_bound_without_grant":
				user = "root"
				require.NoError(t, Params.Save(Params.CommonCfg.RootShouldBindRole.Key, "true"))
			}
			c := &importAuthorizationCoordinator{metadataPrivilegeCoordinator: coord}
			if mode == "legacy_zero_revision" {
				c.describe = func(req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
					resp, err := coord.DescribeCollection(context.Background(), req)
					resp.UpdateTimestampStr = "0"
					return resp, err
				}
			}
			node := &Proxy{mixCoord: c}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			ctx := NewContextWithMetadata(context.Background(), user, "tenant_a")
			resp, err := node.GetImportState(ctx, &milvuspb.GetImportStateRequest{Task: 100})
			require.NoError(t, err)
			if mode == "root_bound_without_grant" {
				require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrPrivilegeNotPermitted)
				require.Empty(t, resp.GetInfos())
				return
			}
			require.NoError(t, merr.Error(resp.GetStatus()))
			require.Contains(t, resp.GetInfos()[0].GetValue(), "staging/tenant_b/private.json", "authorized diagnostics remain intact")
			list, err := node.ListImports(ctx, &internalpb.ListImportsRequest{DbName: "tenant_b"})
			require.NoError(t, err)
			require.NoError(t, merr.Error(list.GetStatus()))
			require.Equal(t, []string{"100"}, list.GetJobIDs())
		})
	}
}

func TestImportListFiltersAlignedMetadataAndReusesCollectionDecisions(t *testing.T) {
	coord, setPolicy := setupMetadataPrivileges(t)
	setPolicy("tenant_a", commonpb.ObjectPrivilege_PrivilegeImport)
	calls := make(map[int64]int)
	c := &importAuthorizationCoordinator{metadataPrivilegeCoordinator: coord}
	c.list = &internalpb.ListImportsResponse{
		Status: merr.Success(), JobIDs: []string{"foreign", "own1", "other", "own2"},
		CollectionIds: []int64{41, 43, 44, 43}, CollectionNames: []string{"records", "old_name", "other", "old_name"},
		States:  []internalpb.ImportJobState{internalpb.ImportJobState_Failed, internalpb.ImportJobState_Completed, internalpb.ImportJobState_Pending, internalpb.ImportJobState_Failed},
		Reasons: []string{"foreign secret", "", "other secret", "own diagnostic"}, Progresses: []int64{0, 100, 0, 10},
	}
	// Retained jobs elsewhere in the cluster must not cause per-ID RPCs.
	for id := int64(1000); id < 2000; id++ {
		c.list.JobIDs = append(c.list.JobIDs, "foreign")
		c.list.CollectionIds = append(c.list.CollectionIds, id)
		c.list.CollectionNames = append(c.list.CollectionNames, "records")
		c.list.States = append(c.list.States, internalpb.ImportJobState_Completed)
		c.list.Reasons = append(c.list.Reasons, "")
		c.list.Progresses = append(c.list.Progresses, 100)
	}
	showCalls := 0
	c.show = func(req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
		showCalls++
		require.Equal(t, "tenant_a", req.GetDbName())
		return &milvuspb.ShowCollectionsResponse{Status: merr.Success(), CollectionIds: []int64{43, 44}}, nil
	}
	c.describe = func(req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		id := req.GetCollectionID()
		if id == 0 {
			id = 43 // Resolve the explicit collection selector.
		} else {
			require.Empty(t, req.GetDbName())
			require.Empty(t, req.GetCollectionName(), "never resolve a persisted job by a historical name")
			calls[id]++
		}
		db, name := "tenant_a", "records"
		if id == 41 {
			db = "tenant_b"
		}
		if id == 44 {
			name = "other"
		}
		return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionID: id, DbName: db, Schema: &schemapb.CollectionSchema{Name: name}, UpdateTimestamp: 100, UpdateTimestampStr: "100"}, nil
	}
	node := &Proxy{mixCoord: c}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	cache, err := metacache.NewMetaCache(c)
	require.NoError(t, err)
	t.Cleanup(cache.Close)
	node.setMetaCache(cache)
	ctx := NewContextWithMetadata(context.Background(), "reader", "tenant_a")
	resp, err := node.ListImports(ctx, &internalpb.ListImportsRequest{})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, []string{"own1", "own2"}, resp.GetJobIDs())
	require.Equal(t, []int64{43, 43}, resp.GetCollectionIds())
	require.Equal(t, []string{"records", "records"}, resp.GetCollectionNames())
	require.Equal(t, []string{"", "own diagnostic"}, resp.GetReasons())
	require.Equal(t, []int64{100, 10}, resp.GetProgresses())
	require.Equal(t, []internalpb.ImportJobState{internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed}, resp.GetStates())
	require.Equal(t, map[int64]int{43: 2, 44: 1}, calls, "only selected DB identities are resolved; authorized identities are revalidated once, not once per job")
	resp, err = node.ListImports(ctx, &internalpb.ListImportsRequest{CollectionName: "records"})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, int64(43), c.listRequest.GetCollectionID())
	require.Equal(t, []string{"own1", "own2"}, resp.GetJobIDs())
	require.Equal(t, 1, showCalls, "explicit collection selection does not enumerate the database again")
}

func TestImportMetadataRevalidatesIdentityAfterAuthorization(t *testing.T) {
	for _, mode := range []string{"drop_recreate", "move", "name_aba", "legacy_zero_aba"} {
		for _, list := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/progress", true: "/list"}[list], func(t *testing.T) {
				coord, setPolicy := setupMetadataPrivileges(t)
				c := &importAuthorizationCoordinator{metadataPrivilegeCoordinator: coord}
				calls := 0
				c.describe = func(req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
					calls++
					if calls == 1 {
						// Return an old snapshot after a grant to a new resource
						// occupying the old name has already reached this Proxy.
						setPolicy("tenant_b", commonpb.ObjectPrivilege_PrivilegeImport)
						resp, err := coord.DescribeCollection(context.Background(), req)
						resp.UpdateTimestamp = 100
						resp.UpdateTimestampStr = "100"
						if mode == "legacy_zero_aba" {
							resp.UpdateTimestamp = 0
							resp.UpdateTimestampStr = "0"
						}
						return resp, err
					}
					if mode == "drop_recreate" {
						return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionID()))}, nil
					}
					if mode == "name_aba" || mode == "legacy_zero_aba" {
						// The temporary same-name collection was dropped and
						// this ID moved back; its revision still advanced.
						setPolicy("")
						return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionID: 41, DbName: "tenant_b", Schema: &schemapb.CollectionSchema{Name: "records"}, UpdateTimestamp: 200, UpdateTimestampStr: "200"}, nil
					}
					return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionID: 41, DbName: "moved_db", Schema: &schemapb.CollectionSchema{Name: "records"}}, nil
				}
				node := &Proxy{mixCoord: c}
				node.UpdateStateCode(commonpb.StateCode_Healthy)
				ctx := NewContextWithMetadata(context.Background(), "reader", "tenant_b")
				if list {
					resp, err := node.ListImportTasks(ctx, &milvuspb.ListImportTasksRequest{DbName: "tenant_b"})
					require.NoError(t, err)
					if mode != "drop_recreate" {
						require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable)
					} else {
						require.NoError(t, merr.Error(resp.GetStatus()))
					}
					require.Empty(t, resp.GetTasks())
				} else {
					resp, err := node.GetImportState(ctx, &milvuspb.GetImportStateRequest{Task: 100})
					require.NoError(t, err)
					if mode != "drop_recreate" {
						require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable)
					} else {
						require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrPrivilegeNotPermitted)
					}
					require.Empty(t, resp.GetInfos())
				}
				require.Equal(t, 2, calls)
			})
		}
	}
}

func TestImportMetadataFailsClosed(t *testing.T) {
	for _, mode := range []string{"deleted", "recreated", "missing_database", "old_coordinator", "old_identity_response", "missing_revision", "inconsistent_revision", "noncanonical_revision", "missing_recheck_revision", "metadata_error", "progress_error", "missing_job", "list_error", "malformed_list", "database_selection_error"} {
		t.Run(mode, func(t *testing.T) {
			coord, setPolicy := setupMetadataPrivileges(t)
			setPolicy("tenant_b", commonpb.ObjectPrivilege_PrivilegeImport)
			c := &importAuthorizationCoordinator{metadataPrivilegeCoordinator: coord}
			want := merr.ErrPrivilegeNotPermitted
			describeCalls := 0
			c.describe = func(req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
				describeCalls++
				switch mode {
				case "inconsistent_revision", "noncanonical_revision", "missing_recheck_revision":
					resp, err := coord.DescribeCollection(context.Background(), req)
					resp.UpdateTimestamp = 100
					resp.UpdateTimestampStr = "100"
					if mode == "inconsistent_revision" {
						resp.UpdateTimestampStr = "200"
					} else if mode == "noncanonical_revision" {
						resp.UpdateTimestampStr = "0100"
					} else if describeCalls%2 == 0 {
						resp.UpdateTimestampStr = ""
					}
					return resp, err
				case "deleted":
					return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionID()))}, nil
				case "missing_database":
					return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.ErrDatabaseNotFound)}, nil
				case "recreated":
					return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionID: 99, DbName: "tenant_b", Schema: &schemapb.CollectionSchema{Name: "records"}}, nil
				case "old_identity_response":
					return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionID: 41, Schema: &schemapb.CollectionSchema{Name: "records"}}, nil
				case "metadata_error":
					return nil, merr.ErrServiceUnavailable
				default:
					return coord.DescribeCollection(context.Background(), req)
				}
			}
			switch mode {
			case "recreated", "old_identity_response", "missing_revision", "inconsistent_revision", "noncanonical_revision", "missing_recheck_revision", "metadata_error", "old_coordinator":
				want = merr.ErrServiceUnavailable
			case "progress_error":
				c.progressErr = merr.ErrServiceUnavailable
			case "list_error":
				c.listErr = merr.ErrServiceUnavailable
			case "missing_job":
				c.progress = &internalpb.GetImportProgressResponse{Status: merr.Status(merr.ErrImportSysFailed)}
			case "malformed_list":
				c.list = &internalpb.ListImportsResponse{Status: merr.Success(), JobIDs: []string{"100"}}
			case "database_selection_error":
				c.show = func(*milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
					return nil, merr.ErrServiceUnavailable
				}
			}
			if mode == "old_coordinator" {
				c.progress = &internalpb.GetImportProgressResponse{Status: merr.Success(), CollectionName: "records", Reason: "secret"}
				c.list, _ = c.ListImports(context.Background(), &internalpb.ListImportsRequestInternal{})
				c.list.CollectionIds = nil
			}
			node := &Proxy{mixCoord: c}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			ctx := NewContextWithMetadata(context.Background(), "reader", "tenant_b")
			if mode != "list_error" && mode != "malformed_list" && mode != "database_selection_error" {
				resp, err := node.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{JobID: "100"})
				if mode == "progress_error" {
					require.ErrorIs(t, err, merr.ErrServiceUnavailable)
					require.Nil(t, resp)
					return
				}
				require.NoError(t, err)
				require.ErrorIs(t, merr.Error(resp.GetStatus()), want)
				require.Empty(t, resp.GetReason())
				require.Empty(t, resp.GetTaskProgresses())
				require.Empty(t, resp.GetCollectionName())
			}
			if mode == "missing_job" {
				return
			}
			resp, err := node.ListImports(ctx, &internalpb.ListImportsRequest{DbName: "tenant_b"})
			require.NoError(t, err)
			if mode == "deleted" || mode == "missing_database" {
				require.NoError(t, merr.Error(resp.GetStatus()))
			} else {
				require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable)
			}
			require.Empty(t, resp.GetJobIDs())
			require.Empty(t, resp.GetReasons())
		})
	}
}
