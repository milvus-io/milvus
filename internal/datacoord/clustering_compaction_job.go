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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type ClusteringCompactionJob struct {
	triggerID         UniqueID
	collectionID      UniqueID
	clusteringKeyID   UniqueID
	clusteringKeyName string
	clusteringKeyType schemapb.DataType
	// ClusteringCompactionJob life cycle:
	//   trigger -> pipelining:
	//              executing:
	//              completed or failed or timeout
	state          compactionTaskState
	startTime      uint64
	lastUpdateTime uint64
	// todo: only store partial info in meta
	pipeliningPlans []*datapb.CompactionPlan
	executingPlans  []*datapb.CompactionPlan
	completedPlans  []*datapb.CompactionPlan
	failedPlans     []*datapb.CompactionPlan
	timeoutPlans    []*datapb.CompactionPlan
	analyzeTaskID   UniqueID
}

func convertClusteringCompactionJob(info *datapb.ClusteringCompactionInfo) *ClusteringCompactionJob {
	job := &ClusteringCompactionJob{
		triggerID:         info.GetTriggerID(),
		collectionID:      info.GetCollectionID(),
		clusteringKeyID:   info.GetClusteringKeyID(),
		clusteringKeyName: info.GetClusteringKeyName(),
		clusteringKeyType: info.GetClusteringKeyType(),
		state:             compactionTaskState(info.GetState()),
		startTime:         info.GetStartTime(),
		lastUpdateTime:    info.GetLastUpdateTime(),
		pipeliningPlans:   info.PipeliningPlans,
		executingPlans:    info.ExecutingPlans,
		completedPlans:    info.CompletedPlans,
		failedPlans:       info.FailedPlans,
		timeoutPlans:      info.TimeoutPlans,
		analyzeTaskID:     info.GetAnalyzeTaskID(),
	}
	return job
}

func convertFromClusteringCompactionJob(job *ClusteringCompactionJob) *datapb.ClusteringCompactionInfo {
	info := &datapb.ClusteringCompactionInfo{
		TriggerID:         job.triggerID,
		CollectionID:      job.collectionID,
		ClusteringKeyID:   job.clusteringKeyID,
		ClusteringKeyName: job.clusteringKeyName,
		ClusteringKeyType: job.clusteringKeyType,
		State:             datapb.CompactionTaskState(job.state),
		StartTime:         job.startTime,
		LastUpdateTime:    job.lastUpdateTime,
		PipeliningPlans:   job.pipeliningPlans,
		ExecutingPlans:    job.executingPlans,
		CompletedPlans:    job.completedPlans,
		FailedPlans:       job.failedPlans,
		TimeoutPlans:      job.timeoutPlans,
		AnalyzeTaskID:     job.analyzeTaskID,
	}
	return info
}
