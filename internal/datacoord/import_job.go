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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
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

func UpdateJobState(state internalpb.ImportJobState) UpdateJobAction {
	return func(job ImportJob) {
		job.(*importJob).ImportJob.State = state
		if state == internalpb.ImportJobState_Completed || state == internalpb.ImportJobState_Failed {
			// releases requested disk resource
			job.(*importJob).ImportJob.RequestedDiskSize = 0
			// set cleanup ts
			dur := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
			cleanupTime := time.Now().Add(dur)
			cleanupTs := tsoutil.ComposeTSByTime(cleanupTime, 0)
			job.(*importJob).ImportJob.CleanupTs = cleanupTs
			log.Info("set import job cleanup ts", zap.Int64("jobID", job.GetJobID()),
				zap.Time("cleanupTime", cleanupTime), zap.Uint64("cleanupTs", cleanupTs))
		}
	}
}

func UpdateJobReason(reason string) UpdateJobAction {
	return func(job ImportJob) {
		job.(*importJob).ImportJob.Reason = reason
	}
}

func UpdateRequestedDiskSize(requestSize int64) UpdateJobAction {
	return func(job ImportJob) {
		job.(*importJob).ImportJob.RequestedDiskSize = requestSize
	}
}

func UpdateJobCompleteTime(completeTime string) UpdateJobAction {
	return func(job ImportJob) {
		job.(*importJob).ImportJob.CompleteTime = completeTime
	}
}

type ImportJob interface {
	GetJobID() int64
	GetCollectionID() int64
	GetCollectionName() string
	GetPartitionIDs() []int64
	GetVchannels() []string
	GetReadyVchannels() []string
	GetSchema() *schemapb.CollectionSchema
	GetTimeoutTs() uint64
	GetCleanupTs() uint64
	GetState() internalpb.ImportJobState
	GetReason() string
	GetRequestedDiskSize() int64
	GetStartTime() string
	GetCompleteTime() string
	GetFiles() []*internalpb.ImportFile
	GetOptions() []*commonpb.KeyValuePair
	GetTR() *timerecord.TimeRecorder
	GetDataTs() uint64
	Clone() ImportJob
}

type importJob struct {
	*datapb.ImportJob

	tr *timerecord.TimeRecorder
}

func (j *importJob) GetTR() *timerecord.TimeRecorder {
	return j.tr
}

func (j *importJob) Clone() ImportJob {
	return &importJob{
		ImportJob: proto.Clone(j.ImportJob).(*datapb.ImportJob),
		tr:        j.tr,
	}
}
