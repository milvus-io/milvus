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

package copysegment

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type CopySegmentJobFilter func(job CopySegmentJob) bool

func WithCopyJobCollectionID(collectionID int64) CopySegmentJobFilter {
	return func(job CopySegmentJob) bool {
		return job.GetCollectionId() == collectionID
	}
}

func WithCopyJobStates(states ...datapb.CopySegmentJobState) CopySegmentJobFilter {
	return func(job CopySegmentJob) bool {
		for _, state := range states {
			if job.GetState() == state {
				return true
			}
		}
		return false
	}
}

func WithoutCopyJobStates(states ...datapb.CopySegmentJobState) CopySegmentJobFilter {
	return func(job CopySegmentJob) bool {
		for _, state := range states {
			if job.GetState() == state {
				return false
			}
		}
		return true
	}
}

// CopyJobTimeoutTs returns the job timeout deadline as a hybrid TSO timestamp
// (physical milliseconds << 18), matching how tryTimeoutJob reads it back via
// tsoutil.PhysicalTime and how CleanupTs is composed. Storing UnixNano here
// instead would be interpreted as a TSO landing centuries in the future, so
// the job timeout would never fire.
func CopyJobTimeoutTs(timeout time.Duration) uint64 {
	return tsoutil.AddPhysicalDurationOnTs(tsoutil.ComposeTSByTime(time.Now()), timeout)
}

type UpdateCopySegmentJobAction func(job CopySegmentJob)

func UpdateCopyJobState(state datapb.CopySegmentJobState) UpdateCopySegmentJobAction {
	return func(job CopySegmentJob) {
		job.(*Job).State = state
		if state == datapb.CopySegmentJobState_CopySegmentJobCompleted ||
			state == datapb.CopySegmentJobState_CopySegmentJobFailed {
			// Set cleanup ts based on copy segment task retention
			dur := Params.DataCoordCfg.CopySegmentTaskRetention.GetAsDuration(time.Second)
			cleanupTime := time.Now().Add(dur)
			cleanupTs := tsoutil.ComposeTSByTime(cleanupTime)
			job.(*Job).CleanupTs = cleanupTs
			mlog.Info(context.TODO(), "set copy segment job cleanup ts",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Time("cleanupTime", cleanupTime),
				mlog.Uint64("cleanupTs", cleanupTs))
		}
	}
}

func UpdateCopyJobReason(reason string) UpdateCopySegmentJobAction {
	return func(job CopySegmentJob) {
		job.(*Job).Reason = reason
	}
}

func UpdateCopyJobProgress(copied, total int64) UpdateCopySegmentJobAction {
	return func(job CopySegmentJob) {
		job.(*Job).CopiedSegments = copied
		job.(*Job).TotalSegments = total
	}
}

// UpdateCopyJobTotalSegments sets only the segment total, leaving CopiedSegments
// untouched. Used when (re-)entering Executing: on a resume after a restart some
// tasks may already be completed, and zeroing the copied count here would make
// the reported progress transiently drop until the next checkCopyingJob tick.
func UpdateCopyJobTotalSegments(total int64) UpdateCopySegmentJobAction {
	return func(job CopySegmentJob) {
		job.(*Job).TotalSegments = total
	}
}

func UpdateCopyJobCompleteTs(completeTs uint64) UpdateCopySegmentJobAction {
	return func(job CopySegmentJob) {
		job.(*Job).CompleteTs = completeTs
	}
}

func UpdateCopyJobTotalRows(totalRows int64) UpdateCopySegmentJobAction {
	return func(job CopySegmentJob) {
		job.(*Job).TotalRows = totalRows
	}
}

type CopySegmentJob interface {
	GetJobId() int64
	GetDbId() int64
	GetCollectionId() int64
	GetCollectionName() string
	GetState() datapb.CopySegmentJobState
	GetReason() string
	GetIdMappings() []*datapb.CopySegmentIDMapping // Lightweight ID mappings
	GetOptions() []*commonpb.KeyValuePair
	GetTimeoutTs() uint64
	GetCleanupTs() uint64
	GetStartTs() uint64
	GetCompleteTs() uint64
	GetTotalSegments() int64
	GetCopiedSegments() int64
	GetTotalRows() int64
	GetSnapshotName() string
	GetSourceCollectionId() int64
	GetPinId() int64
	GetExternal() bool
	GetSnapshotS3Location() string
	GetExternalSpec() string
	GetSnapshotFingerprint() string
	GetTR() *timerecord.TimeRecorder
	Clone() CopySegmentJob
}

type Job struct {
	*datapb.CopySegmentJob
	Tr            *timerecord.TimeRecorder
	SnapshotCache *CopySegmentSnapshotCache
}

func (j *Job) GetTR() *timerecord.TimeRecorder {
	return j.Tr
}

func (j *Job) Clone() CopySegmentJob {
	return &Job{
		CopySegmentJob: proto.Clone(j.CopySegmentJob).(*datapb.CopySegmentJob),
		Tr:             j.Tr,
		SnapshotCache:  j.SnapshotCache,
	}
}
