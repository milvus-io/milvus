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
	"time"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type ImportJobFilter func(job ImportJob) bool

func WithCollectionID(collectionID int64) ImportJobFilter {
	return func(job ImportJob) bool {
		return job.GetCollectionID() == collectionID
	}
}

func WithJobStates(states ...internalpb.ImportJobState) ImportJobFilter {
	return func(job ImportJob) bool {
		for _, state := range states {
			if job.GetState() == state {
				return true
			}
		}
		return false
	}
}

func WithJobVersions(versions ...internalpb.ImportVersion) ImportJobFilter {
	return func(job ImportJob) bool {
		return lo.Contains(versions, job.GetVersion())
	}
}

func WithoutJobStates(states ...internalpb.ImportJobState) ImportJobFilter {
	return func(job ImportJob) bool {
		for _, state := range states {
			if job.GetState() == state {
				return false
			}
		}
		return true
	}
}

type UpdateJobAction func(job ImportJob)

const importJobReasonAbortedByUser = "aborted by user"

// UnfailableJobStates are the committed states: the commit fence is out and
// the commit callback may already have made segments visible, so failing the
// job would drop committed data.
var UnfailableJobStates = typeutil.NewSet(
	internalpb.ImportJobState_Committing,
	internalpb.ImportJobState_Completed,
)

// isIllegalJobTransition reports whether moving job to state must be refused.
func isIllegalJobTransition(job ImportJob, state internalpb.ImportJobState) bool {
	return state == internalpb.ImportJobState_Failed && UnfailableJobStates.Contain(job.GetState())
}

func UpdateJobState(state internalpb.ImportJobState) UpdateJobAction {
	return func(job ImportJob) {
		if isIllegalJobTransition(job, state) {
			mlog.Warn(context.TODO(), "refused illegal import job state transition",
				mlog.FieldJobID(job.GetJobID()),
				mlog.String("from", job.GetState().String()), mlog.String("to", state.String()))
			return
		}
		job.(*importJob).State = state
		if state == internalpb.ImportJobState_Completed || state == internalpb.ImportJobState_Failed {
			// Credentials are needed through both phases and retries, but not
			// after a terminal transition. This clears the retained job record;
			// historical WAL entries remain subject to normal WAL retention.
			options := make([]*commonpb.KeyValuePair, 0, len(job.GetOptions()))
			for _, option := range job.GetOptions() {
				if option.GetKey() != importutilv2.ExternalSpec && option.GetKey() != importutilv2.SnapshotSourceURI {
					options = append(options, option)
				}
			}
			job.(*importJob).Options = options
			// releases requested disk resource
			job.(*importJob).RequestedDiskSize = 0
			// set cleanup ts
			dur := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
			cleanupTime := time.Now().Add(dur)
			cleanupTs := tsoutil.ComposeTSByTime(cleanupTime)
			job.(*importJob).CleanupTs = cleanupTs
			mlog.Info(context.TODO(), "set import job cleanup ts", mlog.FieldJobID(job.GetJobID()),
				mlog.Time("cleanupTime", cleanupTime), mlog.Uint64("cleanupTs", cleanupTs))
		}
	}
}

func UpdateJobReason(reason string) UpdateJobAction {
	return func(job ImportJob) {
		job.(*importJob).Reason = reason
	}
}

func UpdateRequestedDiskSize(requestSize int64) UpdateJobAction {
	return func(job ImportJob) {
		job.(*importJob).RequestedDiskSize = requestSize
	}
}

func UpdateJobCompleteTime(completeTime string) UpdateJobAction {
	return func(job ImportJob) {
		job.(*importJob).CompleteTime = completeTime
	}
}

// UpdateJobIDRanges applies the per-file ID ranges allocated and broadcast after
// preimport. The slice is keyed by position: the caller guarantees
// len(ranges) == len(job.GetFiles()), so entry i belongs to Files[i]. Each range is
// sized to that file's exact post-preimport row count, so its size is also the
// cross-cluster divergence authority the gate compares each cluster's local count
// against. The range configures the datanode's PK/RowID cursor.
func UpdateJobIDRanges(ranges []*commonpb.IDRange) UpdateJobAction {
	return func(job ImportJob) {
		j := job.(*importJob)
		for i, r := range ranges {
			j.Files[i].IdRange = r
		}
	}
}

type ImportJob interface {
	GetJobID() int64
	GetCollectionID() int64
	GetCollectionName() string
	GetPartitionIDs() []int64
	GetVchannels() []string
	GetReadyVchannels() []string
	GetCommittedVchannels() []string
	GetSchema() *schemapb.CollectionSchema
	GetTimeoutTs() uint64
	GetCleanupTs() uint64
	GetState() internalpb.ImportJobState
	GetReason() string
	GetRequestedDiskSize() int64
	GetCreateTime() string
	GetCompleteTime() string
	GetFiles() []*internalpb.ImportFile
	GetSnapshotL0Sources() []*internalpb.SnapshotImportL0Source
	GetOptions() []*commonpb.KeyValuePair
	GetAutoCommit() bool
	GetCommitByCoordinator() bool
	GetTR() *timerecord.TimeRecorder
	GetObjCleaned() bool
	GetDataTs() uint64
	GetVersion() internalpb.ImportVersion
	Clone() ImportJob
}

type importJob struct {
	*datapb.ImportJob

	tr *timerecord.TimeRecorder

	// objCleaned records that this checker already removed the job's temporary
	// import_v3 object prefix.
	//
	// It is not persisted: the delete is idempotent, so a restart simply repeats
	// it once. It is also deliberately not copied by Clone: clones are taken
	// under importMeta's lock, while the GC goroutine writes this flag without
	// that lock, so copying it would make every clone read a lock-free
	// GC-owned field. Losing the flag in a clone only costs one repeated,
	// idempotent delete. The GC goroutine is its only reader and writer.
	objCleaned bool
}

func (j *importJob) GetTR() *timerecord.TimeRecorder {
	return j.tr
}

// GetObjCleaned reports whether this process already removed the job's
// temporary import_v3 object prefix. Only the GC goroutine may call it: the
// flag is written without importMeta's lock.
func (j *importJob) GetObjCleaned() bool {
	return j.objCleaned
}

// markObjCleaned records that the temporary object prefix is gone. Only the GC
// goroutine calls it. The write needs no lock: no other field of a shared job
// is mutated in place (updates clone and replace under importMeta's lock) and
// no other goroutine reads this field.
func (j *importJob) markObjCleaned() {
	j.objCleaned = true
}

func (j *importJob) Clone() ImportJob {
	return &importJob{
		ImportJob: proto.Clone(j.ImportJob).(*datapb.ImportJob),
		tr:        j.tr,
	}
}
