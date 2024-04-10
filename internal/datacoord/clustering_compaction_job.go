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
	// should only store partial info in meta
	compactionPlans      []*datapb.CompactionPlan
	compactionPlanStates []compactionTaskState
	analysisTaskID       UniqueID
}

func convertClusteringCompactionJob(info *datapb.ClusteringCompactionInfo) *ClusteringCompactionJob {
	compactionPlanStates := make([]compactionTaskState, 0)
	for _, compactionPlanState := range info.GetCompactionPlanStates() {
		compactionPlanStates = append(compactionPlanStates, compactionTaskState(compactionPlanState))
	}
	job := &ClusteringCompactionJob{
		triggerID:            info.GetTriggerID(),
		collectionID:         info.GetCollectionID(),
		clusteringKeyID:      info.GetClusteringKeyID(),
		clusteringKeyName:    info.GetClusteringKeyName(),
		clusteringKeyType:    info.GetClusteringKeyType(),
		state:                compactionTaskState(info.GetState()),
		startTime:            info.GetStartTime(),
		lastUpdateTime:       info.GetLastUpdateTime(),
		compactionPlans:      info.GetCompactionPlans(),
		compactionPlanStates: compactionPlanStates,
		analysisTaskID:       info.GetAnalysisTaskID(),
	}
	return job
}

func convertFromClusteringCompactionJob(job *ClusteringCompactionJob) *datapb.ClusteringCompactionInfo {
	compactionPlanStates := make([]int32, 0)
	for _, compactionPlanState := range job.compactionPlanStates {
		compactionPlanStates = append(compactionPlanStates, int32(compactionPlanState))
	}
	compactionPlans := make([]*datapb.CompactionPlan, 0)
	for _, compactionPlan := range job.compactionPlans {
		segments := make([]*datapb.CompactionSegmentBinlogs, 0)
		for _, segment := range compactionPlan.SegmentBinlogs {
			segments = append(segments, &datapb.CompactionSegmentBinlogs{
				SegmentID:    segment.GetSegmentID(),
				CollectionID: segment.GetCollectionID(),
				PartitionID:  segment.GetPartitionID(),
			})
		}
		compactionPlans = append(compactionPlans, &datapb.CompactionPlan{
			PlanID:            compactionPlan.GetPlanID(),
			SegmentBinlogs:    segments,
			StartTime:         compactionPlan.GetStartTime(),
			TimeoutInSeconds:  compactionPlan.GetTimeoutInSeconds(),
			Type:              compactionPlan.GetType(),
			Timetravel:        compactionPlan.GetTimetravel(),
			Channel:           compactionPlan.GetChannel(),
			CollectionTtl:     compactionPlan.GetCollectionTtl(),
			TotalRows:         compactionPlan.GetTotalRows(),
			ClusteringKeyId:   compactionPlan.GetClusteringKeyId(),
			MaxSegmentRows:    compactionPlan.GetMaxSegmentRows(),
			PreferSegmentRows: compactionPlan.GetPreferSegmentRows(),
			AnalyzeResultPath: compactionPlan.GetAnalyzeResultPath(),
			AnalyzeSegmentIds: compactionPlan.GetAnalyzeSegmentIds(),
		})
	}

	info := &datapb.ClusteringCompactionInfo{
		TriggerID:            job.triggerID,
		CollectionID:         job.collectionID,
		ClusteringKeyID:      job.clusteringKeyID,
		ClusteringKeyName:    job.clusteringKeyName,
		ClusteringKeyType:    job.clusteringKeyType,
		State:                int32(job.state),
		StartTime:            job.startTime,
		LastUpdateTime:       job.lastUpdateTime,
		CompactionPlans:      job.compactionPlans,
		CompactionPlanStates: compactionPlanStates,
		AnalysisTaskID:       job.analysisTaskID,
	}
	return info
}

func (job *ClusteringCompactionJob) addCompactionPlan(plan *datapb.CompactionPlan, state compactionTaskState) {
	job.compactionPlans = append(job.compactionPlans, plan)
	job.compactionPlanStates = append(job.compactionPlanStates, state)
}
