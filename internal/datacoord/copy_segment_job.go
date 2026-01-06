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
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
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

type UpdateCopySegmentJobAction func(job CopySegmentJob)

func UpdateCopyJobState(state datapb.CopySegmentJobState) UpdateCopySegmentJobAction {
	return func(job CopySegmentJob) {
		job.(*copySegmentJob).CopySegmentJob.State = state
		if state == datapb.CopySegmentJobState_CopySegmentJobCompleted ||
			state == datapb.CopySegmentJobState_CopySegmentJobFailed {
			// Set cleanup ts based on copy segment task retention
			dur := Params.DataCoordCfg.CopySegmentTaskRetention.GetAsDuration(time.Second)
			cleanupTime := time.Now().Add(dur)
			cleanupTs := tsoutil.ComposeTSByTime(cleanupTime, 0)
			job.(*copySegmentJob).CopySegmentJob.CleanupTs = cleanupTs
			log.Info("set copy segment job cleanup ts",
				zap.Int64("jobID", job.GetJobId()),
				zap.Time("cleanupTime", cleanupTime),
				zap.Uint64("cleanupTs", cleanupTs))
		}
	}
}

func UpdateCopyJobReason(reason string) UpdateCopySegmentJobAction {
	return func(job CopySegmentJob) {
		job.(*copySegmentJob).CopySegmentJob.Reason = reason
	}
}

func UpdateCopyJobProgress(copied, total int64) UpdateCopySegmentJobAction {
	return func(job CopySegmentJob) {
		job.(*copySegmentJob).CopySegmentJob.CopiedSegments = copied
		job.(*copySegmentJob).CopySegmentJob.TotalSegments = total
	}
}

func UpdateCopyJobCompleteTs(completeTs uint64) UpdateCopySegmentJobAction {
	return func(job CopySegmentJob) {
		job.(*copySegmentJob).CopySegmentJob.CompleteTs = completeTs
	}
}

func UpdateCopyJobTotalRows(totalRows int64) UpdateCopySegmentJobAction {
	return func(job CopySegmentJob) {
		job.(*copySegmentJob).CopySegmentJob.TotalRows = totalRows
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
	GetTR() *timerecord.TimeRecorder
	Clone() CopySegmentJob
}

type copySegmentJob struct {
	*datapb.CopySegmentJob
	tr *timerecord.TimeRecorder
}

func (j *copySegmentJob) GetTR() *timerecord.TimeRecorder {
	return j.tr
}

func (j *copySegmentJob) Clone() CopySegmentJob {
	return &copySegmentJob{
		CopySegmentJob: proto.Clone(j.CopySegmentJob).(*datapb.CopySegmentJob),
		tr:             j.tr,
	}
}
