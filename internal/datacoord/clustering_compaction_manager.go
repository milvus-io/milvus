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
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/proto/indexpb"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ClusteringCompactionManager struct {
	ctx               context.Context
	meta              *meta
	allocator         allocator
	compactionHandler compactionPlanContext
	scheduler         Scheduler
	analyzeScheduler  *taskScheduler

	forceMu sync.Mutex
	quit    chan struct{}
	wg      sync.WaitGroup
	signals chan *compactionSignal
	ticker  *time.Ticker

	jobs map[UniqueID]*ClusteringCompactionJob
}

func newClusteringCompactionManager(
	ctx context.Context,
	meta *meta,
	allocator allocator,
	compactionHandler compactionPlanContext,
	analyzeScheduler *taskScheduler,
) *ClusteringCompactionManager {
	return &ClusteringCompactionManager{
		ctx:               ctx,
		meta:              meta,
		allocator:         allocator,
		compactionHandler: compactionHandler,
		analyzeScheduler:  analyzeScheduler,
	}
}

func (t *ClusteringCompactionManager) start() {
	t.quit = make(chan struct{})
	t.ticker = time.NewTicker(Params.DataCoordCfg.ClusteringCompactionStateCheckInterval.GetAsDuration(time.Second))
	t.wg.Add(1)
	go t.startJobCheckLoop()
}

func (t *ClusteringCompactionManager) stop() {
	close(t.quit)
	t.wg.Wait()
}

func (t *ClusteringCompactionManager) submit(job *ClusteringCompactionJob) error {
	log.Info("Insert clustering compaction job", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID))
	t.saveJob(job)
	err := t.runCompactionJob(job)
	if err != nil {
		job.state = failed
		log.Warn("mark clustering compaction job failed", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID))
		t.saveJob(job)
	}
	return nil
}

func (t *ClusteringCompactionManager) startJobCheckLoop() {
	defer logutil.LogPanic()
	defer t.wg.Done()
	for {
		select {
		case <-t.quit:
			t.ticker.Stop()
			log.Info("clustering compaction loop exit")
			return
		case <-t.ticker.C:
			err := t.checkAllJobState()
			if err != nil {
				log.Warn("unable to triggerClusteringCompaction", zap.Error(err))
			}
		}
	}
}

func (t *ClusteringCompactionManager) checkAllJobState() error {
	jobs := t.GetAllJobs()
	for _, job := range jobs {
		err := t.checkJobState(job)
		if err != nil {
			log.Error("fail to check job state", zap.Error(err))
			job.state = failed
			log.Warn("mark clustering compaction job failed", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID))
			t.saveJob(job)
		}
	}
	return nil
}

func (t *ClusteringCompactionManager) checkJobState(job *ClusteringCompactionJob) error {
	if job.state == completed || job.state == failed || job.state == timeout {
		if time.Since(tsoutil.PhysicalTime(job.startTime)) > Params.DataCoordCfg.ClusteringCompactionDropTolerance.GetAsDuration(time.Second) {
			// skip handle this error, try best to delete meta
			t.dropJob(job)
		}
		return nil
	}
	pipeliningPlans := make([]*datapb.CompactionPlan, 0)
	executingPlans := make([]*datapb.CompactionPlan, 0)
	completedPlans := make([]*datapb.CompactionPlan, 0)
	failedPlans := make([]*datapb.CompactionPlan, 0)
	timeoutPlans := make([]*datapb.CompactionPlan, 0)
	checkFunc := func(plans []*datapb.CompactionPlan, lastState compactionTaskState) error {
		for _, plan := range plans {
			compactionTask := t.compactionHandler.getCompaction(plan.GetPlanID())
			// todo: if datacoord crash during clustering compaction, compactTask will lost, we can resubmit these plan
			if compactionTask == nil {
				// if one compaction task is lost, mark it as failed, and the clustering compaction will be marked failed as well
				log.Warn("compaction task lost", zap.Int64("planID", plan.GetPlanID()))
				failedPlans = append(failedPlans, plan)
				continue
			}
			switch compactionTask.state {
			case pipelining:
				pipeliningPlans = append(pipeliningPlans, plan)
			case executing:
				executingPlans = append(executingPlans, plan)
			case failed:
				failedPlans = append(failedPlans, plan)
			case timeout:
				timeoutPlans = append(timeoutPlans, plan)
			case completed:
				completedPlans = append(completedPlans, plan)
			}

			if lastState == executing && compactionTask.state == completed {
				// new finish task, commit the partitionStats and do cleaning
				collectionID := job.collectionID
				partitionID := compactionTask.plan.SegmentBinlogs[0].PartitionID
				vChannelName := compactionTask.plan.GetChannel()

				// read the temp file and write it to formal path
				tempPartitionStatsPath := path.Join(t.meta.chunkManager.RootPath(), common.PartitionStatsTempPath, metautil.JoinIDPath(collectionID, partitionID), compactionTask.plan.GetChannel(), strconv.FormatInt(compactionTask.plan.PlanID, 10))
				partitionStatsPath := path.Join(t.meta.chunkManager.RootPath(), common.PartitionStatsPath, metautil.JoinIDPath(collectionID, partitionID), compactionTask.plan.GetChannel(), strconv.FormatInt(compactionTask.plan.PlanID, 10))
				tempStats, err := t.meta.chunkManager.Read(t.ctx, tempPartitionStatsPath)
				if err != nil {
					return err
				}
				err = t.meta.chunkManager.Write(t.ctx, partitionStatsPath, tempStats)
				if err != nil {
					return err
				}

				// list the partition stats, normally the files should not be more than two
				statsPathPrefix := path.Join(t.meta.chunkManager.RootPath(), common.PartitionStatsPath, metautil.JoinIDPath(collectionID, partitionID), vChannelName)
				filePaths, _, err := t.meta.chunkManager.ListWithPrefix(t.ctx, statsPathPrefix, true)
				if err != nil {
					return err
				}
				_, maxPartitionStatsPath := storage.FindPartitionStatsMaxVersion(filePaths)
				toRemovePaths := make([]string, 0)
				for _, filePath := range filePaths {
					// keep the newest one, still need it for search before querynode handoff
					if filePath != maxPartitionStatsPath {
						toRemovePaths = append(toRemovePaths, filePath)
					}
				}
				// remove old partition stats
				if len(toRemovePaths) > 0 {
					err = t.meta.chunkManager.MultiRemove(t.ctx, toRemovePaths)
					if err != nil {
						return err
					}
				}

				err = t.meta.chunkManager.Remove(t.ctx, tempPartitionStatsPath)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
	checkFunc(job.pipeliningPlans, pipelining)
	checkFunc(job.executingPlans, executing)
	checkFunc(job.completedPlans, completed)
	checkFunc(job.failedPlans, failed)
	checkFunc(job.timeoutPlans, timeout)

	pipeliningPlans = append(pipeliningPlans, t.generateNewPlans(job)...)
	job.pipeliningPlans = pipeliningPlans
	job.executingPlans = executingPlans
	job.completedPlans = completedPlans
	job.failedPlans = failedPlans
	job.timeoutPlans = timeoutPlans

	if len(job.pipeliningPlans) > 0 {
		err := t.runCompactionJob(job)
		if err != nil {
			return err
		}
	}

	if len(job.pipeliningPlans)+len(job.executingPlans) == 0 {
		if len(job.failedPlans) == 0 && len(job.timeoutPlans) == 0 {
			job.state = completed
		} else if len(job.failedPlans) > 0 {
			job.state = failed
		} else {
			job.state = timeout
		}
	}

	log.Info("Update clustering compaction job", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID), zap.String("state", datapb.CompactionTaskState(job.state).String()))
	return t.saveJob(job)
}

func (t *ClusteringCompactionManager) generateNewPlans(job *ClusteringCompactionJob) []*datapb.CompactionPlan {
	return nil
}

func (t *ClusteringCompactionManager) runCompactionJob(job *ClusteringCompactionJob) error {
	t.forceMu.Lock()
	defer t.forceMu.Unlock()

	plans := job.pipeliningPlans
	for _, plan := range plans {
		segIDs := fetchSegIDs(plan.GetSegmentBinlogs())
		start := time.Now()
		planId, analyzeTaskID, err := t.allocator.allocN(2)
		if err != nil {
			return err
		}
		plan.PlanID = planId
		plan.TimeoutInSeconds = Params.DataCoordCfg.ClusteringCompactionTimeoutInSeconds.GetAsInt32()

		// clustering compaction firstly analyze the plan, then decide whether to execute compaction
		if typeutil.IsVectorType(job.clusteringKeyType) {
			newAnalyzeTask := &model.AnalyzeTask{
				CollectionID: job.collectionID,
				PartitionID:  plan.SegmentBinlogs[0].PartitionID,
				FieldID:      job.clusteringKeyID,
				FieldName:    job.clusteringKeyName,
				FieldType:    job.clusteringKeyType,
				SegmentIDs:   segIDs,
				TaskID:       analyzeTaskID,
			}
			err = t.meta.analyzeMeta.AddAnalyzeTask(newAnalyzeTask)
			if err != nil {
				log.Warn("failed to create analyze task", zap.Int64("planID", plan.PlanID), zap.Error(err))
				return err
			}
			t.analyzeScheduler.enqueue(&analyzeTask{
				taskID: analyzeTaskID,
				taskInfo: &indexpb.AnalyzeResult{
					TaskID: analyzeTaskID,
					State:  indexpb.JobState_JobStateInit,
				},
			})
			log.Info("submit analyze task", zap.Int64("id", analyzeTaskID))

			var analyzeTask *model.AnalyzeTask
			analyzeFinished := func() bool {
				analyzeTask = t.meta.analyzeMeta.GetTask(analyzeTaskID)
				log.Debug("check analyze task state", zap.Int64("id", analyzeTaskID), zap.String("state", analyzeTask.State.String()))
				if analyzeTask.State == indexpb.JobState_JobStateFinished ||
					analyzeTask.State == indexpb.JobState_JobStateFailed {
					return true
				}
				return false
			}
			for !analyzeFinished() {
				// respect context deadline/cancel
				select {
				case <-t.ctx.Done():
					return nil
				default:
				}
				time.Sleep(1 * time.Second)
			}
			log.Info("get analyzeTask", zap.Any("analyzeTask", analyzeTask))
			if analyzeTask.State == indexpb.JobState_JobStateFinished {
				//version := int64(0) // analyzeTask.Version
				plan.AnalyzeResultPath = path.Join(metautil.JoinIDPath(analyzeTask.TaskID, analyzeTask.Version))
				offSetSegmentIDs := make([]int64, 0)
				for _, segID := range analyzeTask.SegmentIDs {
					offSetSegmentIDs = append(offSetSegmentIDs, segID)
				}
				plan.AnalyzeSegmentIds = offSetSegmentIDs
			}
		}

		//shouldDo, err := t.shouldDoClusteringCompaction(analyzeResult)
		//if err != nil {
		//	log.Warn("failed to decide whether to execute this compaction plan", zap.Int64("planID", plan.PlanID), zap.Int64s("segmentIDs", segIDs), zap.Error(err))
		//	continue
		//}
		//if !shouldDo {
		//	log.Info("skip execute compaction plan", zap.Int64("planID", plan.PlanID))
		//	continue
		//}

		trigger := &compactionSignal{
			id:           job.triggerID,
			collectionID: job.collectionID,
			partitionID:  plan.SegmentBinlogs[0].PartitionID,
		}
		err = t.compactionHandler.execCompactionPlan(trigger, plan)
		if err != nil {
			log.Warn("failed to execute compaction plan", zap.Int64("planID", plan.GetPlanID()), zap.Int64s("segmentIDs", segIDs), zap.Error(err))
			continue
		}
		log.Info("execute clustering compaction plan", zap.Int64("planID", plan.GetPlanID()), zap.Int64s("segmentIDs", segIDs))

		segIDMap := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
		for _, seg := range plan.SegmentBinlogs {
			segIDMap[seg.SegmentID] = seg.Deltalogs
		}
		log.Info("time cost of generating L2 compaction",
			zap.Any("segID2DeltaLogs", segIDMap),
			zap.Int64("planID", plan.PlanID),
			zap.Int64("time cost", time.Since(start).Milliseconds()),
			zap.Int64s("segmentIDs", segIDs))
	}
	if len(plans) > 0 {
		job.state = executing
	}
	log.Info("Update clustering compaction job", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID))
	err := t.saveJob(job)
	return err
}

//func (t *MajorCompactionManager) shouldDoMajorCompaction(analyzeResult *indexpb.AnalyzeResult) (bool, error) {
//	return true, nil
//}

// IsClusteringCompacting get clustering compaction info by collection id
func (t *ClusteringCompactionManager) IsClusteringCompacting(collectionID UniqueID) bool {
	infos := t.meta.GetClusteringCompactionInfos(collectionID)
	executingInfos := lo.Filter(infos, func(info *datapb.ClusteringCompactionInfo, _ int) bool {
		return info.State == datapb.CompactionTaskState_analyzing ||
			info.State == datapb.CompactionTaskState_executing ||
			info.State == datapb.CompactionTaskState_pipelining
	})
	return len(executingInfos) > 0
}

func (t *ClusteringCompactionManager) setSegmentsCompacting(plan *datapb.CompactionPlan, compacting bool) {
	for _, segmentBinlogs := range plan.GetSegmentBinlogs() {
		t.meta.SetSegmentCompacting(segmentBinlogs.GetSegmentID(), compacting)
	}
}

func (t *ClusteringCompactionManager) fillClusteringCompactionPlans(segments []*SegmentInfo, clusteringKeyId int64, compactTime *compactTime) []*datapb.CompactionPlan {
	plan := segmentsToPlan(segments, datapb.CompactionType_ClusteringCompaction, compactTime)
	plan.ClusteringKeyId = clusteringKeyId
	clusteringMaxSegmentSize := paramtable.Get().DataCoordCfg.ClusteringCompactionMaxSegmentSize.GetAsSize()
	clusteringPreferSegmentSize := paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSize.GetAsSize()
	segmentMaxSize := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	plan.MaxSegmentRows = segments[0].MaxRowNum * clusteringMaxSegmentSize / segmentMaxSize
	plan.PreferSegmentRows = segments[0].MaxRowNum * clusteringPreferSegmentSize / segmentMaxSize
	return []*datapb.CompactionPlan{
		plan,
	}
}

// GetAllJobs returns cloned ClusteringCompactionJob from local meta cache
func (t *ClusteringCompactionManager) GetAllJobs() []*ClusteringCompactionJob {
	jobs := make([]*ClusteringCompactionJob, 0)
	infos := t.meta.GetClonedClusteringCompactionInfos()
	for _, info := range infos {
		job := convertClusteringCompactionJob(info)
		jobs = append(jobs, job)
	}
	return jobs
}

// dropJob drop clustering compaction job in meta
func (t *ClusteringCompactionManager) dropJob(job *ClusteringCompactionJob) error {
	info := convertFromClusteringCompactionJob(job)
	return t.meta.DropClusteringCompactionInfo(info)
}

func (t *ClusteringCompactionManager) saveJob(job *ClusteringCompactionJob) error {
	info := convertFromClusteringCompactionJob(job)
	return t.meta.SaveClusteringCompactionInfo(info)
}

func triggerCompactionPolicy(ctx context.Context, meta *meta, collectionID int64, partitionID int64, channel string, segments []*SegmentInfo) (bool, error) {
	log := log.With(zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID))
	partitionStatsPrefix := path.Join(meta.chunkManager.RootPath(), common.PartitionStatsPath, strconv.FormatInt(collectionID, 10), strconv.FormatInt(partitionID, 10), channel)
	files, _, err := meta.chunkManager.ListWithPrefix(ctx, partitionStatsPrefix, true)
	if err != nil {
		log.Error("Fail to list partition stats", zap.String("prefix", partitionStatsPrefix), zap.Error(err))
		return false, err
	}
	version, partitionStatsPath := storage.FindPartitionStatsMaxVersion(files)
	if version <= 0 {
		var newDataSize int64 = 0
		for _, seg := range segments {
			newDataSize += seg.getSegmentSize()
		}
		if newDataSize > Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.GetAsSize() {
			log.Info("New data is larger than threshold, do compaction", zap.Int64("newDataSize", newDataSize))
			return true, nil
		}
		log.Info("No partition stats and no enough new data, skip compaction")
		return false, nil
	}

	pTime, _ := tsoutil.ParseTS(uint64(version))
	if time.Since(pTime) < Params.DataCoordCfg.ClusteringCompactionMinInterval.GetAsDuration(time.Second) {
		log.Debug("Too short time before last clustering compaction, skip compaction")
		return false, nil
	}
	if time.Since(pTime) > Params.DataCoordCfg.ClusteringCompactionMaxInterval.GetAsDuration(time.Second) {
		log.Debug("It is a long time after last clustering compaction, do compaction")
		return true, nil
	}
	partitionStatsBytes, err := meta.chunkManager.Read(ctx, partitionStatsPath)
	if err != nil {
		log.Error("Fail to read partition stats", zap.String("path", partitionStatsPath), zap.Error(err))
		return false, err
	}
	partitionStats, err := storage.DeserializePartitionsStatsSnapshot(partitionStatsBytes)
	if err != nil {
		log.Error("Fail to deserialize partition stats", zap.String("path", partitionStatsPath), zap.Error(err))
		return false, err
	}
	log.Info("Read partition stats", zap.Int64("version", version))

	var compactedSegmentSize int64 = 0
	var uncompactedSegmentSize int64 = 0
	for _, seg := range segments {
		if _, ok := partitionStats.SegmentStats[seg.ID]; ok {
			compactedSegmentSize += seg.getSegmentSize()
		} else {
			uncompactedSegmentSize += seg.getSegmentSize()
		}
	}

	// ratio based
	//ratio := float64(uncompactedSegmentSize) / float64(compactedSegmentSize)
	//if ratio > Params.DataCoordCfg.ClusteringCompactionNewDataRatioThreshold.GetAsFloat() {
	//	log.Info("New data is larger than threshold, do compaction", zap.Float64("ratio", ratio))
	//	return true, nil
	//}
	//log.Info("New data is smaller than threshold, skip compaction", zap.Float64("ratio", ratio))
	//return false, nil

	// size based
	if uncompactedSegmentSize > Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.GetAsSize() {
		log.Info("New data is larger than threshold, do compaction", zap.Int64("newDataSize", uncompactedSegmentSize))
		return true, nil
	}
	log.Info("New data is smaller than threshold, skip compaction", zap.Int64("newDataSize", uncompactedSegmentSize))
	return false, nil
}
