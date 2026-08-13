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

package datacoord

import (
	"context"
	"sort"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var errSnapshotExportJobPersistence = errors.New("snapshot export job metadata persistence failed")

type snapshotExportJobPersistenceError struct{ error }

func (e *snapshotExportJobPersistenceError) Unwrap() error { return e.error }

func (e *snapshotExportJobPersistenceError) Is(target error) bool {
	return target == errSnapshotExportJobPersistence
}

type snapshotExportMeta struct {
	catalog metastore.DataCoordCatalog
	jobs    *typeutil.ConcurrentMap[int64, *datapb.ExportSnapshotJob]
	locks   *lock.KeyLock[int64]
}

func newSnapshotExportMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*snapshotExportMeta, error) {
	jobs, err := catalog.ListExportSnapshotJobs(ctx)
	if err != nil {
		return nil, merr.Wrap(err, "failed to load snapshot export jobs")
	}

	meta := &snapshotExportMeta{
		catalog: catalog,
		jobs:    typeutil.NewConcurrentMap[int64, *datapb.ExportSnapshotJob](),
		locks:   lock.NewKeyLock[int64](),
	}
	for _, job := range jobs {
		if job == nil || job.GetJobId() == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("invalid snapshot export job record")
		}
		if _, loaded := meta.jobs.GetOrInsert(job.GetJobId(), proto.Clone(job).(*datapb.ExportSnapshotJob)); loaded {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate snapshot export job %d", job.GetJobId())
		}
	}
	mlog.Info(ctx, "snapshot export jobs loaded", mlog.Int("jobCount", len(jobs)))
	return meta, nil
}

func (m *snapshotExportMeta) CreateJob(ctx context.Context, job *datapb.ExportSnapshotJob) error {
	if job == nil || job.GetJobId() == 0 {
		return merr.WrapErrServiceInternalMsg("snapshot export job is invalid")
	}

	m.locks.Lock(job.GetJobId())
	defer m.locks.Unlock(job.GetJobId())
	if _, ok := m.jobs.Get(job.GetJobId()); ok {
		return merr.WrapErrServiceInternalMsg("snapshot export job %d already exists", job.GetJobId())
	}

	clone := proto.Clone(job).(*datapb.ExportSnapshotJob)
	if err := m.catalog.SaveExportSnapshotJob(ctx, clone); err != nil {
		return merr.Wrap(err, "failed to persist snapshot export job")
	}
	m.jobs.Insert(clone.GetJobId(), clone)
	return nil
}

func (m *snapshotExportMeta) GetJob(jobID int64) (*datapb.ExportSnapshotJob, bool) {
	job, ok := m.jobs.Get(jobID)
	if !ok {
		return nil, false
	}
	return proto.Clone(job).(*datapb.ExportSnapshotJob), true
}

func (m *snapshotExportMeta) GetJobs() []*datapb.ExportSnapshotJob {
	jobs := make([]*datapb.ExportSnapshotJob, 0, m.jobs.Len())
	m.jobs.Range(func(_ int64, job *datapb.ExportSnapshotJob) bool {
		jobs = append(jobs, proto.Clone(job).(*datapb.ExportSnapshotJob))
		return true
	})
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].GetStartTime() == jobs[j].GetStartTime() {
			return jobs[i].GetJobId() < jobs[j].GetJobId()
		}
		return jobs[i].GetStartTime() < jobs[j].GetStartTime()
	})
	return jobs
}

func (m *snapshotExportMeta) UpdateJob(
	ctx context.Context,
	jobID int64,
	mutate func(*datapb.ExportSnapshotJob) (skip bool, err error),
) (*datapb.ExportSnapshotJob, bool, error) {
	return m.updateJob(ctx, jobID, mutate)
}

func (m *snapshotExportMeta) TryUpdateJob(
	ctx context.Context,
	jobID int64,
	mutate func(*datapb.ExportSnapshotJob) (skip bool, err error),
) (*datapb.ExportSnapshotJob, bool, bool, error) {
	if !m.locks.TryLock(jobID) {
		return nil, false, false, nil
	}
	defer m.locks.Unlock(jobID)
	job, applied, err := m.updateJobLocked(ctx, jobID, mutate)
	return job, true, applied, err
}

func (m *snapshotExportMeta) updateJob(
	ctx context.Context,
	jobID int64,
	mutate func(*datapb.ExportSnapshotJob) (skip bool, err error),
) (*datapb.ExportSnapshotJob, bool, error) {
	m.locks.Lock(jobID)
	defer m.locks.Unlock(jobID)
	return m.updateJobLocked(ctx, jobID, mutate)
}

func (m *snapshotExportMeta) updateJobLocked(
	ctx context.Context,
	jobID int64,
	mutate func(*datapb.ExportSnapshotJob) (skip bool, err error),
) (*datapb.ExportSnapshotJob, bool, error) {
	job, ok := m.jobs.Get(jobID)
	if !ok {
		return nil, false, merr.WrapErrServiceInternalMsg("snapshot export job %d not found", jobID)
	}
	clone := proto.Clone(job).(*datapb.ExportSnapshotJob)
	skip, err := mutate(clone)
	if err != nil {
		return nil, false, err
	}
	if skip {
		return proto.Clone(job).(*datapb.ExportSnapshotJob), false, nil
	}
	if err := m.catalog.SaveExportSnapshotJob(ctx, clone); err != nil {
		return nil, false, &snapshotExportJobPersistenceError{
			error: merr.Wrap(err, "failed to update snapshot export job"),
		}
	}
	// The mutator owns clone for the duration of this call. Cache a separate
	// copy so retaining and modifying that pointer cannot bypass persistence.
	m.jobs.Insert(jobID, proto.Clone(clone).(*datapb.ExportSnapshotJob))
	return proto.Clone(clone).(*datapb.ExportSnapshotJob), true, nil
}

func (m *snapshotExportMeta) DropJob(ctx context.Context, jobID int64) error {
	m.locks.Lock(jobID)
	defer m.locks.Unlock(jobID)
	if err := m.catalog.DropExportSnapshotJob(ctx, jobID); err != nil {
		return merr.Wrap(err, "failed to drop snapshot export job")
	}
	m.jobs.Remove(jobID)
	return nil
}
