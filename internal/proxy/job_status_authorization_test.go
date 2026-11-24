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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type statusAuthzCollection struct {
	db, name string
}

// statusAuthzCoordinator serves job, compaction and segment metadata
// for collections in several databases. The authenticated "reader" holds the
// role metadata_reader, whose grants each test sets; "no_roles" holds none;
// "operator" holds the built-in admin role.
type statusAuthzCoordinator struct {
	*metadataPrivilegeCoordinator
	collections map[int64]statusAuthzCollection

	restoreJobs     map[int64]*datapb.RestoreSnapshotInfo
	refreshJobs     []*datapb.ExternalCollectionRefreshJob
	compactions     map[int64]*milvuspb.GetCompactionPlansResponse
	segments        map[int64]*datapb.SegmentInfo
	segmentRequests [][]int64
}

func newStatusAuthzCoordinator(t *testing.T) *statusAuthzCoordinator {
	base, _ := setupMetadataPrivileges(t)
	c := &statusAuthzCoordinator{
		metadataPrivilegeCoordinator: base,
		collections: map[int64]statusAuthzCollection{
			41: {db: "tenant_b", name: "records"},
			50: {db: "tenant_b", name: "restored"},
			60: {db: "tenant_c", name: "other"},
		},
	}
	c.grant(t)
	return c
}

type statusAuthzGrant struct {
	db, collection string
	privilege      commonpb.ObjectPrivilege
}

// grant replaces the grants of metadata_reader. An empty db and collection
// grant the privilege globally.
func (c *statusAuthzCoordinator) grant(t *testing.T, grants ...statusAuthzGrant) {
	c.policies = nil
	for _, g := range grants {
		if g.db == "" && g.collection == "" {
			c.policies = append(c.policies, funcutil.PolicyForPrivilege("metadata_reader",
				commonpb.ObjectType_Global.String(), util.AnyWord, g.privilege.String(), util.AnyWord))
			continue
		}
		c.policies = append(c.policies, funcutil.PolicyForPrivilege("metadata_reader",
			commonpb.ObjectType_Collection.String(), g.collection, g.privilege.String(), g.db))
	}
	require.NoError(t, privilege.InitPrivilegeCache(context.Background(), c))
}

// builtinGroup grants, on one collection, every privilege of a built-in
// privilege group. RootCoord expands such groups before Proxy sees them.
func builtinGroup(db, collection string, group []string) []statusAuthzGrant {
	grants := make([]statusAuthzGrant, 0, len(group))
	for _, name := range group {
		value, ok := commonpb.ObjectPrivilege_value[util.PrivilegeNameForMetastore(name)]
		if !ok {
			continue // Not an object privilege (the deprecated Expr); nothing checks it.
		}
		grants = append(grants, statusAuthzGrant{db, collection, commonpb.ObjectPrivilege(value)})
	}
	return grants
}

func (c *statusAuthzCoordinator) ListPolicy(context.Context, *internalpb.ListPolicyRequest, ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	return &internalpb.ListPolicyResponse{
		Status: merr.Success(), PolicyInfos: c.policies,
		UserRoles: []string{
			funcutil.EncodeUserRoleCache("reader", "metadata_reader"),
			funcutil.EncodeUserRoleCache("operator", util.RoleAdmin),
		},
	}, nil
}

func (c *statusAuthzCoordinator) DescribeCollection(_ context.Context, req *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	c.describeCalls++
	respond := func(id int64, coll statusAuthzCollection) *milvuspb.DescribeCollectionResponse {
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Success(), CollectionID: id, DbName: coll.db,
			Schema: &schemapb.CollectionSchema{Name: coll.name, Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "pk"},
			}},
			UpdateTimestamp: 100, UpdateTimestampStr: "100",
		}
	}
	if id := req.GetCollectionID(); id != 0 {
		coll, ok := c.collections[id]
		if !ok {
			return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(id))}, nil
		}
		return respond(id, coll), nil
	}
	db := req.GetDbName()
	if db == "" {
		db = util.DefaultDBName
	}
	for id, coll := range c.collections {
		if coll.db == db && coll.name == req.GetCollectionName() {
			return respond(id, coll), nil
		}
	}
	return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFoundWithDB(db, req.GetCollectionName()))}, nil
}

// GetRestoreSnapshotState answers as DataCoord does, including its error for
// an absent job.
func (c *statusAuthzCoordinator) GetRestoreSnapshotState(_ context.Context, req *datapb.GetRestoreSnapshotStateRequest, _ ...grpc.CallOption) (*datapb.GetRestoreSnapshotStateResponse, error) {
	info, ok := c.restoreJobs[req.GetJobId()]
	if !ok {
		return &datapb.GetRestoreSnapshotStateResponse{
			Status: merr.Status(merr.WrapErrImportSysFailedMsg("restore job not found: jobID=%d", req.GetJobId())),
		}, nil
	}
	return &datapb.GetRestoreSnapshotStateResponse{Status: merr.Success(), Info: proto.Clone(info).(*datapb.RestoreSnapshotInfo)}, nil
}

func (c *statusAuthzCoordinator) GetRefreshExternalCollectionProgress(_ context.Context, req *datapb.GetRefreshExternalCollectionProgressRequest, _ ...grpc.CallOption) (*datapb.GetRefreshExternalCollectionProgressResponse, error) {
	for _, job := range c.refreshJobs {
		if job.GetJobId() == req.GetJobId() {
			return &datapb.GetRefreshExternalCollectionProgressResponse{Status: merr.Success(), JobInfo: proto.Clone(job).(*datapb.ExternalCollectionRefreshJob)}, nil
		}
	}
	return &datapb.GetRefreshExternalCollectionProgressResponse{
		Status: merr.Status(merr.WrapErrParameterInvalidMsg("refresh job %d not found", req.GetJobId())),
	}, nil
}

func (c *statusAuthzCoordinator) ListRefreshExternalCollectionJobs(_ context.Context, req *datapb.ListRefreshExternalCollectionJobsRequest, _ ...grpc.CallOption) (*datapb.ListRefreshExternalCollectionJobsResponse, error) {
	resp := &datapb.ListRefreshExternalCollectionJobsResponse{Status: merr.Success()}
	for _, job := range c.refreshJobs {
		if req.GetCollectionId() == 0 || job.GetCollectionId() == req.GetCollectionId() {
			resp.Jobs = append(resp.Jobs, proto.Clone(job).(*datapb.ExternalCollectionRefreshJob))
		}
	}
	return resp, nil
}

// compactionPlans answers as DataCoord does: an ID it has nothing for is an
// undefined state, not an error.
func (c *statusAuthzCoordinator) compactionPlans(compactionID int64) *milvuspb.GetCompactionPlansResponse {
	if plans, ok := c.compactions[compactionID]; ok {
		return proto.Clone(plans).(*milvuspb.GetCompactionPlansResponse)
	}
	return &milvuspb.GetCompactionPlansResponse{Status: merr.Success(), State: commonpb.CompactionState_UndefiedState}
}

func (c *statusAuthzCoordinator) GetCompactionState(_ context.Context, req *milvuspb.GetCompactionStateRequest, _ ...grpc.CallOption) (*milvuspb.GetCompactionStateResponse, error) {
	plans := c.compactionPlans(req.GetCompactionID())
	resp := &milvuspb.GetCompactionStateResponse{Status: merr.Success(), State: plans.GetState()}
	for _, info := range plans.GetMergeInfos() {
		if info.GetState() == commonpb.CompactionTaskState_CompactionTaskStateExecuting {
			resp.ExecutingPlanNo++
		} else {
			resp.CompletedPlanNo++
		}
	}
	return resp, nil
}

func (c *statusAuthzCoordinator) GetCompactionStateWithPlans(_ context.Context, req *milvuspb.GetCompactionPlansRequest, _ ...grpc.CallOption) (*milvuspb.GetCompactionPlansResponse, error) {
	if req.GetCollectionId() == 0 {
		return c.compactionPlans(req.GetCompactionID()), nil
	}
	resp := &milvuspb.GetCompactionPlansResponse{Status: merr.Success(), State: commonpb.CompactionState_UndefiedState}
	for _, plans := range c.compactions {
		for _, info := range plans.GetMergeInfos() {
			if info.GetCollectionId() == req.GetCollectionId() {
				resp.MergeInfos = append(resp.MergeInfos, info)
				resp.State = plans.GetState()
			}
		}
	}
	return resp, nil
}

// GetSegmentInfo stops at the first missing segment, as DataCoord does.
func (c *statusAuthzCoordinator) GetSegmentInfo(_ context.Context, req *datapb.GetSegmentInfoRequest, _ ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
	c.segmentRequests = append(c.segmentRequests, req.GetSegmentIDs())
	resp := &datapb.GetSegmentInfoResponse{Status: merr.Success()}
	for _, id := range req.GetSegmentIDs() {
		info, ok := c.segments[id]
		if !ok {
			return &datapb.GetSegmentInfoResponse{Status: merr.Status(merr.WrapErrSegmentNotFound(id))}, nil
		}
		resp.Infos = append(resp.Infos, proto.Clone(info).(*datapb.SegmentInfo))
	}
	return resp, nil
}

func newStatusAuthzProxy(t *testing.T, coord *statusAuthzCoordinator) *Proxy {
	cache, err := metacache.NewMetaCache(coord)
	require.NoError(t, err)
	t.Cleanup(cache.Close)
	node := &Proxy{
		mixCoord:  coord,
		metaCache: cache,
		sched:     &scheduler.TaskScheduler{DdQueue: &scheduler.DdTaskQueue{}},
	}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	return node
}

func disableAuthorizationForTest(t *testing.T) {
	require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false"))
	t.Cleanup(func() { Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true") })
}

func userContext(user string) context.Context {
	return NewContextWithMetadata(context.Background(), user, "tenant_a")
}

// runDdTasksInline makes the DDL queue execute each task on the caller's
// goroutine, so that Proxy methods backed by tasks can run without a scheduler.
func runDdTasksInline(t *testing.T) {
	var execErr error
	enqueue := mockey.Mock((*scheduler.DdTaskQueue).Enqueue).To(func(task taskmodel.Task) error {
		execErr = task.Execute(task.TraceCtx())
		return nil
	}).Build()
	wait := mockey.Mock((*TaskCondition).WaitToFinish).To(func() error { return execErr }).Build()
	t.Cleanup(func() {
		enqueue.UnPatch()
		wait.UnPatch()
	})
}

func TestRestoreSnapshotStateAuthorization(t *testing.T) {
	runDdTasksInline(t)
	coord := newStatusAuthzCoordinator(t)
	coord.restoreJobs = map[int64]*datapb.RestoreSnapshotInfo{
		// Restored from tenant_b.records into tenant_b.restored.
		7: {JobId: 7, SnapshotName: "nightly", CollectionId: 50, SourceCollectionId: 41, State: datapb.RestoreSnapshotState_RestoreSnapshotExecuting, Reason: "copying"},
		// Restored from an external snapshot into tenant_b.restored.
		8: {JobId: 8, SnapshotName: "imported", CollectionId: 50, External: true, State: datapb.RestoreSnapshotState_RestoreSnapshotCompleted},
		// Reported by a coordinator that predates the source identity.
		9: {JobId: 9, SnapshotName: "nightly", CollectionId: 50, State: datapb.RestoreSnapshotState_RestoreSnapshotCompleted},
	}
	node := newStatusAuthzProxy(t, coord)
	get := func(user string, jobID int64) *milvuspb.GetRestoreSnapshotStateResponse {
		resp, err := node.GetRestoreSnapshotState(userContext(user), &milvuspb.GetRestoreSnapshotStateRequest{JobId: jobID})
		require.NoError(t, err)
		return resp
	}
	restore := commonpb.ObjectPrivilege_PrivilegeRestoreSnapshot

	t.Run("denied_like_missing", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			user   string
			grants []statusAuthzGrant
		}{
			{"no_grants", "no_roles", nil},
			{"same_name_other_database", "reader", []statusAuthzGrant{{"tenant_a", "records", restore}, {"tenant_a", "restored", restore}}},
			{"other_privilege", "reader", []statusAuthzGrant{{"tenant_b", "records", commonpb.ObjectPrivilege_PrivilegeQuery}}},
			{"source_grant_on_external_job", "reader", []statusAuthzGrant{{"tenant_b", "records", restore}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				coord.grant(t, tc.grants...)
				missing := get(tc.user, 404)
				jobs := []int64{7, 8, 9}
				if tc.name == "source_grant_on_external_job" {
					jobs = []int64{8, 9}
				}
				for _, jobID := range jobs {
					denied := get(tc.user, jobID)
					assert.ErrorIs(t, merr.Error(denied.GetStatus()), merr.ErrPrivilegeNotPermitted)
					assert.Nil(t, denied.GetInfo())
					assert.True(t, proto.Equal(denied, missing), "job %d: existing and missing jobs must look the same: %v / %v", jobID, denied, missing)
				}
			})
		}
	})

	t.Run("initiator_and_target_owner", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			grants  []statusAuthzGrant
			allowed []int64
		}{
			// RestoreSnapshot itself is authorized on the source collection.
			{"source", []statusAuthzGrant{{"tenant_b", "records", restore}}, []int64{7}},
			{"target", []statusAuthzGrant{{"tenant_b", "restored", restore}}, []int64{7, 8, 9}},
			{"collection_admin_group", builtinGroup("tenant_b", "records", util.CollectionAdminPrivileges), []int64{7}},
			{"external_restore", []statusAuthzGrant{{"", "", commonpb.ObjectPrivilege_PrivilegeRestoreExternalSnapshot}}, []int64{8}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				coord.grant(t, tc.grants...)
				for _, jobID := range []int64{7, 8, 9} {
					resp := get("reader", jobID)
					allowed := false
					for _, id := range tc.allowed {
						allowed = allowed || id == jobID
					}
					if allowed {
						require.NoError(t, merr.Error(resp.GetStatus()), "job %d", jobID)
						assert.Equal(t, jobID, resp.GetInfo().GetJobId())
						assert.Equal(t, "restored", resp.GetInfo().GetCollectionName())
					} else {
						assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrPrivilegeNotPermitted, "job %d", jobID)
					}
				}
			})
		}
	})

	t.Run("unrestricted_callers", func(t *testing.T) {
		coord.grant(t)
		for _, user := range []string{util.UserRoot, "operator"} {
			resp := get(user, 8)
			require.NoError(t, merr.Error(resp.GetStatus()), user)
			assert.Equal(t, int64(8), resp.GetInfo().GetJobId())
		}
		disableAuthorizationForTest(t)
		resp := get("no_roles", 7)
		require.NoError(t, merr.Error(resp.GetStatus()))
		missing := get("no_roles", 404)
		assert.ErrorIs(t, merr.Error(missing.GetStatus()), merr.ErrImportSysFailed, "without authorization an absent job keeps its error")
	})
}

func TestRefreshExternalCollectionJobAuthorization(t *testing.T) {
	coord := newStatusAuthzCoordinator(t)
	coord.refreshJobs = []*datapb.ExternalCollectionRefreshJob{
		{JobId: 11, CollectionId: 41, CollectionName: "records", ExternalSource: "s3://tenant-b/records/", State: indexpb.JobState_JobStateFinished},
		{JobId: 12, CollectionId: 60, CollectionName: "other", ExternalSource: "s3://tenant-c/other/", State: indexpb.JobState_JobStateInProgress},
		// The collection was dropped after the job ran.
		{JobId: 13, CollectionId: 70, CollectionName: "dropped", ExternalSource: "s3://tenant-d/dropped/", State: indexpb.JobState_JobStateFailed},
	}
	node := newStatusAuthzProxy(t, coord)
	refresh := commonpb.ObjectPrivilege_PrivilegeRefreshExternalCollection
	progress := func(user string, jobID int64) *milvuspb.GetRefreshExternalCollectionProgressResponse {
		resp, err := node.GetRefreshExternalCollectionProgress(userContext(user), &milvuspb.GetRefreshExternalCollectionProgressRequest{JobId: jobID})
		require.NoError(t, err)
		return resp
	}
	list := func(user, db, name string) *milvuspb.ListRefreshExternalCollectionJobsResponse {
		resp, err := node.ListRefreshExternalCollectionJobs(userContext(user), &milvuspb.ListRefreshExternalCollectionJobsRequest{DbName: db, CollectionName: name})
		require.NoError(t, err)
		return resp
	}
	jobIDs := func(resp *milvuspb.ListRefreshExternalCollectionJobsResponse) []int64 {
		var ids []int64
		for _, job := range resp.GetJobs() {
			ids = append(ids, job.GetJobId())
		}
		return ids
	}

	t.Run("zero_grants", func(t *testing.T) {
		coord.grant(t)
		missing := progress("no_roles", 404)
		assert.ErrorIs(t, merr.Error(missing.GetStatus()), merr.ErrPrivilegeNotPermitted)
		for _, jobID := range []int64{11, 12, 13} {
			denied := progress("no_roles", jobID)
			assert.Nil(t, denied.GetJobInfo())
			assert.True(t, proto.Equal(denied, missing), "job %d: existing and missing jobs must look the same", jobID)
		}
		all := list("no_roles", "tenant_a", "")
		require.NoError(t, merr.Error(all.GetStatus()))
		assert.Empty(t, all.GetJobs())
		existing := list("no_roles", "tenant_b", "records")
		absent := list("no_roles", "tenant_b", "absent")
		assert.ErrorIs(t, merr.Error(existing.GetStatus()), merr.ErrPrivilegeNotPermitted)
		assert.Empty(t, existing.GetJobs())
		assert.True(t, proto.Equal(existing, absent), "existing and missing collections must look the same: %v / %v", existing, absent)
	})

	t.Run("owner", func(t *testing.T) {
		coord.grant(t, statusAuthzGrant{"tenant_b", "records", refresh})
		resp := progress("reader", 11)
		require.NoError(t, merr.Error(resp.GetStatus()))
		assert.Equal(t, "s3://tenant-b/records/", resp.GetJobInfo().GetExternalSource())
		missing := progress("reader", 404)
		for _, jobID := range []int64{12, 13} {
			assert.True(t, proto.Equal(progress("reader", jobID), missing), "job %d", jobID)
		}
		assert.Equal(t, []int64{11}, jobIDs(list("reader", "tenant_a", "")))
		named := list("reader", "tenant_b", "records")
		require.NoError(t, merr.Error(named.GetStatus()))
		assert.Equal(t, []int64{11}, jobIDs(named))
		// The same name in another database does not authorize this one.
		coord.grant(t, statusAuthzGrant{"tenant_a", "records", refresh})
		assert.Empty(t, list("reader", "tenant_a", "").GetJobs())
		assert.ErrorIs(t, merr.Error(progress("reader", 11).GetStatus()), merr.ErrPrivilegeNotPermitted)
		assert.ErrorIs(t, merr.Error(list("reader", "tenant_b", "records").GetStatus()), merr.ErrPrivilegeNotPermitted)
	})

	t.Run("unrestricted_callers", func(t *testing.T) {
		coord.grant(t)
		assert.Equal(t, []int64{11, 12, 13}, jobIDs(list(util.UserRoot, "tenant_a", "")))
		// The admin role holds every privilege on every existing collection; a
		// job whose collection was dropped has none left to authorize it.
		assert.Equal(t, []int64{11, 12}, jobIDs(list("operator", "tenant_a", "")))
		for _, user := range []string{util.UserRoot, "operator"} {
			require.NoError(t, merr.Error(progress(user, 12).GetStatus()), user)
		}
		disableAuthorizationForTest(t)
		assert.Equal(t, []int64{11, 12, 13}, jobIDs(list("no_roles", "tenant_a", "")))
		assert.ErrorIs(t, merr.Error(progress("no_roles", 404).GetStatus()), merr.ErrParameterInvalid,
			"without authorization an absent job keeps its error")
	})
}

func TestCompactionStateAuthorization(t *testing.T) {
	coord := newStatusAuthzCoordinator(t)
	coord.compactions = map[int64]*milvuspb.GetCompactionPlansResponse{
		// A manual compaction of tenant_b.records.
		5: {Status: merr.Success(), State: commonpb.CompactionState_Executing, MergeInfos: []*milvuspb.CompactionMergeInfo{
			{PlanId: 501, TriggerId: 5, CollectionId: 41, Sources: []int64{1, 2}, Channel: "dml_0_41v0", State: commonpb.CompactionTaskState_CompactionTaskStateExecuting},
			{PlanId: 502, TriggerId: 5, CollectionId: 41, Sources: []int64{3}, Channel: "dml_1_41v1", State: commonpb.CompactionTaskState_CompactionTaskStateCompleted},
		}},
		// A manual compaction of tenant_c.other.
		6: {Status: merr.Success(), State: commonpb.CompactionState_Completed, MergeInfos: []*milvuspb.CompactionMergeInfo{
			{PlanId: 601, TriggerId: 6, CollectionId: 60, Sources: []int64{9}, FailureReason: "tenant_c detail", State: commonpb.CompactionTaskState_CompactionTaskStateFailed},
		}},
		// A manual rewrite whose tasks are not generated yet, and -1, which a
		// manual compaction that planned nothing returns.
		8:  {Status: merr.Success(), State: commonpb.CompactionState_Executing},
		-1: {Status: merr.Success(), State: commonpb.CompactionState_Completed},
	}
	node := newStatusAuthzProxy(t, coord)
	compact := commonpb.ObjectPrivilege_PrivilegeCompaction
	state := func(user string, id int64) *milvuspb.GetCompactionStateResponse {
		resp, err := node.GetCompactionState(userContext(user), &milvuspb.GetCompactionStateRequest{CompactionID: id})
		require.NoError(t, err)
		return resp
	}
	plans := func(user string, req *milvuspb.GetCompactionPlansRequest) *milvuspb.GetCompactionPlansResponse {
		resp, err := node.GetCompactionStateWithPlans(userContext(user), req)
		require.NoError(t, err)
		return resp
	}

	t.Run("by_id_hidden_like_unknown", func(t *testing.T) {
		for _, tc := range []struct {
			user   string
			grants []statusAuthzGrant
			hidden []int64
		}{
			{"no_roles", nil, []int64{5, 6}},
			{"reader", []statusAuthzGrant{{"tenant_a", "records", compact}}, []int64{5, 6}},
			{"reader", []statusAuthzGrant{{"tenant_b", "records", commonpb.ObjectPrivilege_PrivilegeQuery}}, []int64{5, 6}},
			{"reader", []statusAuthzGrant{{"tenant_b", "records", compact}}, []int64{6}},
		} {
			coord.grant(t, tc.grants...)
			unknownState := state(tc.user, 404)
			unknownPlans := plans(tc.user, &milvuspb.GetCompactionPlansRequest{CompactionID: 404})
			require.NoError(t, merr.Error(unknownState.GetStatus()))
			for _, id := range tc.hidden {
				assert.True(t, proto.Equal(state(tc.user, id), unknownState), "%s: compaction %d", tc.user, id)
				assert.True(t, proto.Equal(plans(tc.user, &milvuspb.GetCompactionPlansRequest{CompactionID: id}), unknownPlans), "%s: compaction %d", tc.user, id)
			}
		}
	})

	t.Run("by_id_owner", func(t *testing.T) {
		coord.grant(t, builtinGroup("tenant_b", "records", util.CollectionReadWritePrivileges)...)
		resp := state("reader", 5)
		assert.Equal(t, commonpb.CompactionState_Executing, resp.GetState())
		assert.Equal(t, int64(1), resp.GetExecutingPlanNo())
		withPlans := plans("reader", &milvuspb.GetCompactionPlansRequest{CompactionID: 5})
		assert.Len(t, withPlans.GetMergeInfos(), 2)
	})

	t.Run("by_id_without_tasks", func(t *testing.T) {
		// No task names an owner, so nothing is withheld; these IDs must keep
		// working for the caller that started the compaction.
		coord.grant(t)
		assert.Equal(t, commonpb.CompactionState_Completed, state("no_roles", -1).GetState())
		assert.Equal(t, commonpb.CompactionState_Executing, state("no_roles", 8).GetState())
		assert.Equal(t, commonpb.CompactionState_Executing, plans("no_roles", &milvuspb.GetCompactionPlansRequest{CompactionID: 8}).GetState())
	})

	t.Run("by_name", func(t *testing.T) {
		coord.grant(t, statusAuthzGrant{"tenant_a", "records", compact})
		existing := plans("reader", &milvuspb.GetCompactionPlansRequest{DbName: "tenant_b", CollectionName: "records"})
		absent := plans("reader", &milvuspb.GetCompactionPlansRequest{DbName: "tenant_b", CollectionName: "absent"})
		assert.ErrorIs(t, merr.Error(existing.GetStatus()), merr.ErrPrivilegeNotPermitted)
		assert.Empty(t, existing.GetMergeInfos())
		assert.True(t, proto.Equal(existing, absent), "existing and missing collections must look the same: %v / %v", existing, absent)

		coord.grant(t, statusAuthzGrant{"tenant_b", "records", compact})
		allowed := plans("reader", &milvuspb.GetCompactionPlansRequest{DbName: "tenant_b", CollectionName: "records"})
		require.NoError(t, merr.Error(allowed.GetStatus()))
		assert.Len(t, allowed.GetMergeInfos(), 2)
		// The database comes from the connection when the request names none.
		ctx := NewContextWithMetadata(context.Background(), "reader", "tenant_b")
		resp, err := node.GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{CollectionName: "records"})
		require.NoError(t, err)
		assert.Len(t, resp.GetMergeInfos(), 2)
	})

	t.Run("unrestricted_callers", func(t *testing.T) {
		coord.grant(t)
		for _, user := range []string{util.UserRoot, "operator"} {
			assert.Len(t, plans(user, &milvuspb.GetCompactionPlansRequest{CompactionID: 6}).GetMergeInfos(), 1, user)
		}
		disableAuthorizationForTest(t)
		assert.Equal(t, int64(1), state("no_roles", 6).GetCompletedPlanNo())
		assert.Len(t, plans("no_roles", &milvuspb.GetCompactionPlansRequest{CompactionID: 6}).GetMergeInfos(), 1)
	})

	t.Run("unknown_owner_fails_closed", func(t *testing.T) {
		coord.grant(t)
		coord.compactions[9] = &milvuspb.GetCompactionPlansResponse{
			Status: merr.Success(), State: commonpb.CompactionState_Completed,
			MergeInfos: []*milvuspb.CompactionMergeInfo{{PlanId: 901, TriggerId: 9}},
		}
		resp := state("no_roles", 9)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable)
	})
}

func TestSegmentsInfoAuthorization(t *testing.T) {
	coord := newStatusAuthzCoordinator(t)
	segments := map[int64]*datapb.SegmentInfo{
		1001: {ID: 1001, CollectionID: 41, InsertChannel: "dml_0_41v0", NumOfRows: 10},
		1002: {ID: 1002, CollectionID: 60, InsertChannel: "dml_0_60v0", NumOfRows: 20},
		// The collection of this segment was dropped.
		1003: {ID: 1003, CollectionID: 70, InsertChannel: "dml_0_70v0", NumOfRows: 30},
	}
	node := newStatusAuthzProxy(t, coord)
	get := func(user string, ids ...int64) *internalpb.GetSegmentsInfoResponse {
		resp, err := node.GetSegmentsInfo(userContext(user), &internalpb.GetSegmentsInfoRequest{DbName: "tenant_b", CollectionID: 41, SegmentIDs: ids})
		require.NoError(t, err)
		return resp
	}
	// asMissing is what the caller would get if the segments it may not read
	// did not exist at all.
	asMissing := func(user string, hidden []int64, ids ...int64) *internalpb.GetSegmentsInfoResponse {
		coord.segments = map[int64]*datapb.SegmentInfo{}
		for id, info := range segments {
			coord.segments[id] = info
		}
		for _, id := range hidden {
			delete(coord.segments, id)
		}
		disableAuthorizationForTest(t)
		defer Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		return get(user, ids...)
	}

	coord.grant(t, statusAuthzGrant{"tenant_b", "records", commonpb.ObjectPrivilege_PrivilegeGetStatistics})
	for _, ids := range [][]int64{
		{1002},
		{1003},
		{1002, 1001},
		{1001, 1002},
		{1002, 9999},       // DataCoord reports 9999; it must report 1002 as if absent.
		{1001, 9999, 1002}, // DataCoord reports 9999, which is still the first absent one.
		{9999, 1002},
	} {
		want := asMissing("reader", []int64{1002, 1003}, ids...)
		coord.segments = segments
		got := get("reader", ids...)
		assert.ErrorIs(t, merr.Error(got.GetStatus()), merr.ErrSegmentNotFound, "%v", ids)
		assert.True(t, proto.Equal(got, want), "%v: forbidden segments must look absent: %v / %v", ids, got, want)
	}

	coord.segments = segments
	resp := get("reader", 1001)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Len(t, resp.GetSegmentInfos(), 1)
	assert.Equal(t, int64(41), resp.GetSegmentInfos()[0].GetCollectionID())

	coord.grant(t)
	want := asMissing("no_roles", []int64{1001, 1002, 1003}, 1001)
	coord.segments = segments
	assert.True(t, proto.Equal(get("no_roles", 1001), want))

	resp = get(util.UserRoot, 1001, 1002, 1003)
	require.NoError(t, merr.Error(resp.GetStatus()))
	assert.Len(t, resp.GetSegmentInfos(), 3)
	resp = get("operator", 1001, 1002)
	require.NoError(t, merr.Error(resp.GetStatus()))
	assert.Len(t, resp.GetSegmentInfos(), 2)
	assert.ErrorIs(t, merr.Error(get("operator", 1003).GetStatus()), merr.ErrSegmentNotFound,
		"a segment whose collection was dropped has no collection left to authorize it")
}
