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
	"fmt"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
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

	quit    chan struct{}
	wg      sync.WaitGroup
	signals chan *compactionSignal
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
	t.wg.Add(2)
	go t.startJobCheckLoop()
	go t.startGCLoop()
}

func (t *ClusteringCompactionManager) stop() {
	close(t.quit)
	t.wg.Wait()
}

func (t *ClusteringCompactionManager) submit(job *ClusteringCompactionJob) error {
	log.Info("Insert clustering compaction job", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID))
	return t.saveJob(job)
}

func (t *ClusteringCompactionManager) getByTriggerId(triggerID int64) *datapb.ClusteringCompactionInfo {
	clusteringInfos := t.meta.GetClusteringCompactionInfosByTriggerID(triggerID)
	// should be one
	if len(clusteringInfos) == 0 {
		return nil
	}
	return clusteringInfos[0]
}

func (t *ClusteringCompactionManager) getCompactionJobState(triggerID int64) (state commonpb.CompactionState, executingCnt, completedCnt, failedCnt, timeoutCnt int) {
	tasks := make([]*compactionTask, 0)
	compactionJob := t.getByTriggerId(triggerID)
	plans := compactionJob.GetCompactionPlans()
	for _, plan := range plans {
		task := t.compactionHandler.getCompaction(plan.GetPlanID())
		if task == nil {
			continue
		}
		tasks = append(tasks, task)
		switch task.state {
		case pipelining:
			executingCnt++
		case executing:
			executingCnt++
		case completed:
			completedCnt++
		case failed:
			failedCnt++
		case timeout:
			timeoutCnt++
		}
	}

	compactionState := compactionTaskState(compactionJob.State)
	if compactionState == pipelining || compactionState == executing {
		state = commonpb.CompactionState_Executing
	} else {
		state = commonpb.CompactionState_Completed
	}
	return
}

func (t *ClusteringCompactionManager) startJobCheckLoop() {
	defer logutil.LogPanic()
	defer t.wg.Done()
	ticker := time.NewTicker(paramtable.Get().DataCoordCfg.ClusteringCompactionStateCheckInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-t.quit:
			log.Info("clustering compaction loop exit")
			return
		case <-ticker.C:
			err := t.checkAllJobState()
			if err != nil {
				log.Warn("unable to triggerClusteringCompaction", zap.Error(err))
			}
			ticker.Reset(paramtable.Get().DataCoordCfg.ClusteringCompactionStateCheckInterval.GetAsDuration(time.Second))
		}
	}
}

func (t *ClusteringCompactionManager) startGCLoop() {
	defer logutil.LogPanic()
	defer t.wg.Done()
	ticker := time.NewTicker(paramtable.Get().DataCoordCfg.ClusteringCompactionGCInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-t.quit:
			log.Info("clustering compaction gc loop exit")
			return
		case <-ticker.C:
			err := t.gc()
			if err != nil {
				log.Warn("fail to gc", zap.Error(err))
			}
			ticker.Reset(paramtable.Get().DataCoordCfg.ClusteringCompactionGCInterval.GetAsDuration(time.Second))
		}
	}
}

func (t *ClusteringCompactionManager) gc() error {
	log.Debug("start gc clustering compaction related meta and files")
	// gc clustering compaction jobs
	jobs := t.GetAllJobs()
	log.Debug("clustering compaction job meta", zap.Int("len", len(jobs)))
	for _, job := range jobs {
		if job.state == completed || job.state == failed || job.state == timeout {
			if time.Since(tsoutil.PhysicalTime(job.startTime)) > Params.DataCoordCfg.ClusteringCompactionDropTolerance.GetAsDuration(time.Second) {
				// skip handle this error, try best to delete meta
				err := t.dropJob(job)
				if err != nil {
					return err
				}
			}
		}
	}
	// gc partition stats
	channelPartitionStatsInfos := make(map[string][]*datapb.PartitionStatsInfo, 0)
	for _, partitionStatsInfo := range t.meta.partitionStatsInfos {
		channel := fmt.Sprintf("%d/%d/%s", partitionStatsInfo.CollectionID, partitionStatsInfo.PartitionID, partitionStatsInfo.VChannel)
		infos, exist := channelPartitionStatsInfos[channel]
		if exist {
			infos = append(infos, partitionStatsInfo)
			channelPartitionStatsInfos[channel] = infos
		} else {
			channelPartitionStatsInfos[channel] = []*datapb.PartitionStatsInfo{partitionStatsInfo}
		}
	}
	log.Debug("channels with PartitionStats meta", zap.Int("len", len(channelPartitionStatsInfos)))

	for channel, infos := range channelPartitionStatsInfos {
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Version > infos[j].Version
		})
		log.Debug("PartitionStats in channel", zap.String("channel", channel), zap.Int("len", len(infos)))
		if len(infos) > 2 {
			for i := 2; i < len(infos); i++ {
				info := infos[i]
				partitionStatsPath := path.Join(t.meta.chunkManager.RootPath(), common.PartitionStatsPath, metautil.JoinIDPath(info.CollectionID, info.PartitionID), info.GetVChannel(), strconv.FormatInt(info.GetVersion(), 10))
				err := t.meta.chunkManager.Remove(t.ctx, partitionStatsPath)
				log.Debug("remove partition stats file", zap.String("path", partitionStatsPath))
				if err != nil {
					return err
				}
				err = t.meta.DropPartitionStatsInfo(info)
				log.Debug("drop partition stats meta", zap.Any("info", info))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
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
		return nil
	}

	if job.state == pipelining {
		err := t.runCompactionJob(job)
		if err != nil {
			job.state = failed
			log.Warn("mark clustering compaction job failed", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID))
			t.saveJob(job)
		}
		return nil
	}

	for index, plan := range job.compactionPlans {
		compactionTask := t.compactionHandler.getCompaction(plan.GetPlanID())
		if compactionTask == nil {
			// if one compaction task is lost, mark it as failed, and the clustering compaction will be marked failed as well
			log.Warn("compaction task lost", zap.Int64("planID", plan.GetPlanID()))
			job.state = failed
			break
		}

		log.Debug("compaction task", zap.Int64("planID", plan.GetPlanID()), zap.Any("state", compactionTask.state))
		switch compactionTask.state {
		case completed:
			if job.compactionPlans[index].State == int32(executing) || job.compactionPlans[index].State == int32(pipelining) {
				segmentIDs := make([]int64, 0)
				for _, seg := range compactionTask.result.Segments {
					segmentIDs = append(segmentIDs, seg.GetSegmentID())
				}

				// wait for segment indexed
				collectionIndexes := t.meta.indexMeta.GetIndexesForCollection(job.collectionID, "")
				indexed := func() bool {
					for _, collectionIndex := range collectionIndexes {
						for _, segmentID := range segmentIDs {
							segmentIndexState := t.meta.indexMeta.GetSegmentIndexState(job.collectionID, segmentID, collectionIndex.IndexID)
							if segmentIndexState.GetState() != commonpb.IndexState_Finished {
								return false
							}
						}
					}
					return true
				}()
				log.Debug("check compaction result segments index states", zap.Bool("indexed", indexed), zap.Int64("planID", plan.GetPlanID()), zap.Int64s("segments", segmentIDs))
				if indexed {
					err := t.meta.SavePartitionStatsInfo(&datapb.PartitionStatsInfo{
						CollectionID: job.collectionID,
						PartitionID:  compactionTask.plan.SegmentBinlogs[0].PartitionID,
						VChannel:     compactionTask.plan.GetChannel(),
						Version:      compactionTask.plan.PlanID,
						SegmentIDs:   segmentIDs,
					})
					if err != nil {
						return err
					}

					job.compactionPlans[index].State = int32(completed)
					job.state = completed
					ts, err := t.allocator.allocTimestamp(t.ctx)
					if err != nil {
						return err
					}
					job.endTime = ts
					elapse := tsoutil.PhysicalTime(ts).UnixMilli() - tsoutil.PhysicalTime(job.startTime).UnixMilli()
					log.Debug("clustering compaction job elapse", zap.Int64("triggerID", job.triggerID), zap.Int64("collectionID", job.collectionID), zap.Int64("elapse", elapse))
					metrics.DataCoordCompactionLatency.
						WithLabelValues(fmt.Sprint(typeutil.IsVectorType(job.clusteringKeyType)), datapb.CompactionType_ClusteringCompaction.String()).
						Observe(float64(elapse))
				}
				// not mark job.compactionPlanStates[index]==completed before index complete
			}
			// todo: for now, a clustering compaction job has only one compactionPlan
		case failed:
			// todo: retry sub tasks
			job.state = failed
			job.compactionPlans[index].State = int32(compactionTask.state)
		case timeout:
			// todo: retry sub tasks
			job.state = timeout
			job.compactionPlans[index].State = int32(compactionTask.state)
		case pipelining:
			job.compactionPlans[index].State = int32(compactionTask.state)
		case executing:
			job.compactionPlans[index].State = int32(compactionTask.state)
		}
	}

	log.Info("Update clustering compaction job", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID), zap.String("state", fmt.Sprint(job.state)))
	return t.saveJob(job)
}

func (t *ClusteringCompactionManager) generateNewPlans(job *ClusteringCompactionJob) []*datapb.CompactionPlan {
	return nil
}

func (t *ClusteringCompactionManager) runCompactionJob(job *ClusteringCompactionJob) error {
	plans := job.compactionPlans
	currentID, _, err := t.allocator.allocN(int64(2 * len(plans)))
	if err != nil {
		return err
	}
	for index, plan := range plans {
		if job.compactionPlans[index].State != int32(pipelining) {
			continue
		}
		segIDs := fetchSegIDs(plan.GetSegmentBinlogs())
		start := time.Now()
		planId := currentID
		currentID++
		analyzeTaskID := currentID
		currentID++
		plan.PlanID = planId
		plan.TimeoutInSeconds = Params.DataCoordCfg.ClusteringCompactionTimeoutInSeconds.GetAsInt32()

		// clustering compaction firstly analyze the plan, then decide whether to execute compaction
		if typeutil.IsVectorType(job.clusteringKeyType) {
			newAnalyzeTask := &indexpb.AnalyzeTask{
				CollectionID: job.collectionID,
				PartitionID:  plan.SegmentBinlogs[0].PartitionID,
				FieldID:      job.clusteringKeyID,
				FieldName:    job.clusteringKeyName,
				FieldType:    job.clusteringKeyType,
				SegmentIDs:   segIDs,
				TaskID:       analyzeTaskID,
				State:        indexpb.JobState_JobStateInit,
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

			var analyzeTask *indexpb.AnalyzeTask
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
	err = t.saveJob(job)
	return err
}

//func (t *MajorCompactionManager) shouldDoMajorCompaction(analyzeResult *indexpb.AnalyzeResult) (bool, error) {
//	return true, nil
//}

// IsClusteringCompacting get clustering compaction info by collection id
func (t *ClusteringCompactionManager) IsClusteringCompacting(collectionID UniqueID) bool {
	infos := t.meta.GetClusteringCompactionInfosByID(collectionID)
	executingInfos := lo.Filter(infos, func(info *datapb.ClusteringCompactionInfo, _ int) bool {
		state := compactionTaskState(info.State)
		return state == pipelining || state == executing
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
	infos := t.meta.GetClusteringCompactionInfos()
	for _, info := range infos {
		job := convertToClusteringCompactionJob(info)
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
	partitionStatsInfos := meta.ListPartitionStatsInfos(collectionID, partitionID, channel)
	sort.Slice(partitionStatsInfos, func(i, j int) bool {
		return partitionStatsInfos[i].Version > partitionStatsInfos[j].Version
	})

	if len(partitionStatsInfos) == 0 {
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

	partitionStats := partitionStatsInfos[0]
	version := partitionStats.Version
	pTime, _ := tsoutil.ParseTS(uint64(version))
	if time.Since(pTime) < Params.DataCoordCfg.ClusteringCompactionMinInterval.GetAsDuration(time.Second) {
		log.Debug("Too short time before last clustering compaction, skip compaction")
		return false, nil
	}
	if time.Since(pTime) > Params.DataCoordCfg.ClusteringCompactionMaxInterval.GetAsDuration(time.Second) {
		log.Debug("It is a long time after last clustering compaction, do compaction")
		return true, nil
	}

	var compactedSegmentSize int64 = 0
	var uncompactedSegmentSize int64 = 0
	for _, seg := range segments {
		if lo.Contains(partitionStats.SegmentIDs, seg.ID) {
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
