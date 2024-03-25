package datacoord

import "github.com/milvus-io/milvus/internal/proto/datapb"

type MajorCompactionJob struct {
	triggerID       UniqueID
	collectionID    UniqueID
	clusteringKeyID UniqueID
	// MajorCompactionJob life cycle:
	//   trigger -> pipelining:
	//              analyzing:
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
	analysisTaskID  UniqueID
	analysisResult  string
}

func convertMajorCompactionJob(info *datapb.MajorCompactionInfo) *MajorCompactionJob {
	job := &MajorCompactionJob{
		triggerID:       info.GetTriggerID(),
		collectionID:    info.GetCollectionID(),
		clusteringKeyID: info.GetClusteringKeyID(),
		state:           compactionTaskState(info.GetState()),
		startTime:       info.GetStartTime(),
		lastUpdateTime:  info.GetLastUpdateTime(),
		pipeliningPlans: info.PipeliningPlans,
		executingPlans:  info.ExecutingPlans,
		completedPlans:  info.CompletedPlans,
		failedPlans:     info.FailedPlans,
		timeoutPlans:    info.TimeoutPlans,
		analysisTaskID:  info.GetAnalysisTaskID(),
	}
	return job
}

func convertFromMajorCompactionJob(job *MajorCompactionJob) *datapb.MajorCompactionInfo {
	info := &datapb.MajorCompactionInfo{
		TriggerID:       job.triggerID,
		CollectionID:    job.collectionID,
		ClusteringKeyID: job.clusteringKeyID,
		State:           datapb.CompactionTaskState(job.state),
		StartTime:       job.startTime,
		LastUpdateTime:  job.lastUpdateTime,
		PipeliningPlans: job.pipeliningPlans,
		ExecutingPlans:  job.executingPlans,
		CompletedPlans:  job.completedPlans,
		FailedPlans:     job.failedPlans,
		TimeoutPlans:    job.timeoutPlans,
		AnalysisTaskID:  job.analysisTaskID,
		AnalysisResult:  job.analysisResult,
	}
	return info
}
