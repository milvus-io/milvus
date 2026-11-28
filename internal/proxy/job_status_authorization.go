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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Job status requests name a job by ID only, so they carry no privilege
// annotation and PrivilegeInterceptor lets them through. Each handler below
// resolves the collection that owns the job and authorizes the caller on it.
// A job that does not exist and a job the caller may not see give the same
// answer.

func restoreJobDenied() error {
	// Do not reveal whether a job exists or the database/collection that owns it.
	return merr.WrapErrPrivilegeNotPermitted("not allowed to access restore snapshot job")
}

// authorizeRestoreSnapshotJob lets the caller see a restore job when they may
// do any of what starts or completes one: RestoreSnapshot on the target
// collection, RestoreSnapshot on the source collection (what RestoreSnapshot
// itself is authorized on; the target is created by the job), or, for a job
// restored from an external snapshot, the global RestoreExternalSnapshot.
func (node *Proxy) authorizeRestoreSnapshotJob(ctx context.Context, info *datapb.RestoreSnapshotInfo) error {
	subject, err := resourceSubject(ctx)
	if err != nil {
		return err
	}
	if subject.bypass {
		return nil
	}
	if info == nil {
		return restoreJobDenied()
	}
	if info.GetExternal() {
		allowed, err := subject.permitsCluster(commonpb.ObjectPrivilege_PrivilegeRestoreExternalSnapshot.String())
		if err != nil || allowed {
			return err
		}
	}
	// Coordinators that predate source_collection_id report zero; the job can
	// then be authorized only through its target.
	for _, collectionID := range []int64{info.GetCollectionId(), info.GetSourceCollectionId()} {
		if collectionID <= 0 {
			continue
		}
		_, allowed, err := node.authorizeCollectionID(ctx, collectionID, subject,
			commonpb.ObjectPrivilege_PrivilegeRestoreSnapshot.String(), "restore snapshot job")
		if err != nil || allowed {
			return err
		}
	}
	return restoreJobDenied()
}

func refreshJobDenied() error {
	// Do not reveal whether a job exists or the database/collection that owns it.
	return merr.WrapErrPrivilegeNotPermitted("not allowed to access refresh external collection job")
}

// refreshJobAuthorizer decides refresh jobs against the privilege that starts
// them, RefreshExternalCollection on the owning collection. Decisions are kept
// for one request only, so a revocation or DDL is seen by the next request.
type refreshJobAuthorizer struct {
	node      *Proxy
	subject   rbacSubject
	decisions map[int64]bool
}

func (node *Proxy) newRefreshJobAuthorizer(ctx context.Context) (*refreshJobAuthorizer, error) {
	subject, err := resourceSubject(ctx)
	if err != nil {
		return nil, err
	}
	return &refreshJobAuthorizer{node: node, subject: subject, decisions: make(map[int64]bool)}, nil
}

func (a *refreshJobAuthorizer) permits(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) (bool, error) {
	if a.subject.bypass {
		return true, nil
	}
	collectionID := job.GetCollectionId()
	if allowed, ok := a.decisions[collectionID]; ok {
		return allowed, nil
	}
	_, allowed, err := a.node.authorizeCollectionID(ctx, collectionID, a.subject,
		commonpb.ObjectPrivilege_PrivilegeRefreshExternalCollection.String(), "refresh external collection job")
	if err != nil {
		return false, err
	}
	a.decisions[collectionID] = allowed
	return allowed, nil
}

// compactionNotVisible is what DataCoord reports for a compaction ID it has no
// tasks or target for.
func compactionNotVisible() *milvuspb.GetCompactionPlansResponse {
	return &milvuspb.GetCompactionPlansResponse{
		Status: merr.Success(),
		State:  commonpb.CompactionState_UndefiedState,
	}
}

// compactionPlansVisible reports whether the caller may see the tasks a
// compaction ID selected: the caller needs Compaction, the privilege that
// starts a manual compaction, on every collection the tasks belong to. A
// compaction the caller may not see is presented as an unknown compaction ID,
// the answer DataCoord gives for an ID it has nothing for, so that a caller
// cannot tell the two apart. This is a filter rather than an error because
// DataCoord does not reject unknown IDs: -1 (a manual compaction that planned
// nothing) and a manual rewrite whose tasks are not generated yet are polled
// through IDs with no tasks. Such an answer carries a state and no task, and
// is returned as is.
func (node *Proxy) compactionPlansVisible(ctx context.Context, plans *milvuspb.GetCompactionPlansResponse) (bool, error) {
	subject, err := resourceSubject(ctx)
	if err != nil {
		return false, err
	}
	if subject.bypass {
		return true, nil
	}
	checked := make(map[int64]struct{})
	for _, info := range plans.GetMergeInfos() {
		collectionID := info.GetCollectionId()
		if _, ok := checked[collectionID]; ok {
			continue
		}
		checked[collectionID] = struct{}{}
		_, allowed, err := node.authorizeCollectionID(ctx, collectionID, subject,
			commonpb.ObjectPrivilege_PrivilegeCompaction.String(), "compaction")
		if err != nil || !allowed {
			return false, err
		}
	}
	return true, nil
}
