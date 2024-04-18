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
	state     compactionTaskState
	startTime uint64
	endTime   uint64
	// should only store partial info in meta
	compactionPlans []*datapb.CompactionPlan
	analyzeTaskID   UniqueID
}

func convertToClusteringCompactionJob(info *datapb.ClusteringCompactionInfo) *ClusteringCompactionJob {
	job := &ClusteringCompactionJob{
		triggerID:         info.GetTriggerID(),
		collectionID:      info.GetCollectionID(),
		clusteringKeyID:   info.GetClusteringKeyID(),
		clusteringKeyName: info.GetClusteringKeyName(),
		clusteringKeyType: info.GetClusteringKeyType(),
		state:             compactionTaskState(info.GetState()),
		startTime:         info.GetStartTime(),
		endTime:           info.GetEndTime(),
		compactionPlans:   info.GetCompactionPlans(),
		analyzeTaskID:     info.GetAnalyzeTaskID(),
	}
	return job
}

func convertFromClusteringCompactionJob(job *ClusteringCompactionJob) *datapb.ClusteringCompactionInfo {
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
		TriggerID:         job.triggerID,
		CollectionID:      job.collectionID,
		ClusteringKeyID:   job.clusteringKeyID,
		ClusteringKeyName: job.clusteringKeyName,
		ClusteringKeyType: job.clusteringKeyType,
		State:             int32(job.state),
		StartTime:         job.startTime,
		EndTime:           job.endTime,
		CompactionPlans:   job.compactionPlans,
		AnalyzeTaskID:     job.analyzeTaskID,
	}
	return info
}

func (job *ClusteringCompactionJob) addCompactionPlan(plan *datapb.CompactionPlan, state compactionTaskState) {
	plan.State = int32(state)
	job.compactionPlans = append(job.compactionPlans, plan)
}
