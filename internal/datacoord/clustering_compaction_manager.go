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
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
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
	analysisScheduler *analysisTaskScheduler

	forceMu  sync.Mutex
	quit     chan struct{}
	wg       sync.WaitGroup
	signals  chan *compactionSignal
	ticker   *time.Ticker
	gcTicker *time.Ticker

	jobs map[UniqueID]*ClusteringCompactionJob
}

func newClusteringCompactionManager(
	ctx context.Context,
	meta *meta,
	allocator allocator,
	compactionHandler compactionPlanContext,
	analysisScheduler *analysisTaskScheduler,
) *ClusteringCompactionManager {
	return &ClusteringCompactionManager{
		ctx:               ctx,
		meta:              meta,
		allocator:         allocator,
		compactionHandler: compactionHandler,
		analysisScheduler: analysisScheduler,
	}
}

func (t *ClusteringCompactionManager) start() {
	t.quit = make(chan struct{})
	t.ticker = time.NewTicker(Params.DataCoordCfg.ClusteringCompactionStateCheckInterval.GetAsDuration(time.Second))
	t.gcTicker = time.NewTicker(Params.DataCoordCfg.ClusteringCompactionStateCheckInterval.GetAsDuration(time.Second))
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

func (t *ClusteringCompactionManager) startGCLoop() {
	defer logutil.LogPanic()
	defer t.wg.Done()
	for {
		select {
		case <-t.quit:
			t.gcTicker.Stop()
			log.Info("clustering compaction gc loop exit")
			return
		case <-t.gcTicker.C:
			err := t.gc()
			if err != nil {
				log.Warn("fail to gc", zap.Error(err))
			}
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
				segmentIDs := make([]int64, 0)
				for _, seg := range compactionTask.result.Segments {
					segmentIDs = append(segmentIDs, seg.GetSegmentID())
				}
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
		planId, analysisTaskID, err := t.allocator.allocN(2)
		if err != nil {
			return err
		}
		plan.PlanID = planId
		plan.TimeoutInSeconds = Params.DataCoordCfg.ClusteringCompactionTimeoutInSeconds.GetAsInt32()

		// clustering compaction firstly analyze the plan, then decide whether to execute compaction
		if typeutil.IsVectorType(job.clusteringKeyType) {
			newAnalysisTask := &model.AnalysisTask{
				CollectionID: job.collectionID,
				PartitionID:  plan.SegmentBinlogs[0].PartitionID,
				FieldID:      job.clusteringKeyID,
				FieldName:    job.clusteringKeyName,
				FieldType:    job.clusteringKeyType,
				SegmentIDs:   segIDs,
				TaskID:       analysisTaskID,
			}
			err = t.analysisScheduler.analysisMeta.AddAnalysisTask(newAnalysisTask)
			if err != nil {
				log.Warn("failed to create analysis task", zap.Int64("planID", plan.PlanID), zap.Error(err))
				return err
			}
			t.analysisScheduler.enqueue(analysisTaskID)
			log.Info("submit analysis task", zap.Int64("id", analysisTaskID))

			var analysisTask *model.AnalysisTask
			analysisFinished := func() bool {
				analysisTask = t.analysisScheduler.analysisMeta.GetTask(analysisTaskID)
				log.Debug("check analysis task state", zap.Int64("id", analysisTaskID), zap.String("state", analysisTask.State.String()))
				if analysisTask.State == commonpb.IndexState_Finished ||
					analysisTask.State == commonpb.IndexState_Failed {
					return true
				}
				return false
			}
			for !analysisFinished() {
				// respect context deadline/cancel
				select {
				case <-t.ctx.Done():
					return nil
				default:
				}
				time.Sleep(1 * time.Second)
			}
			log.Info("get analysisTask", zap.Any("analysisTask", analysisTask))
			if analysisTask.State == commonpb.IndexState_Finished {
				//version := int64(0) // analysisTask.Version
				plan.AnalyzeResultPath = path.Join(metautil.JoinIDPath(analysisTask.TaskID, analysisTask.Version))
				offSetSegmentIDs := make([]int64, 0)
				for segID, _ := range analysisTask.SegmentOffsetMappingFiles {
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

//func (t *ClusteringCompactionManager) shouldDoClusteringCompaction(analyzeResult *indexpb.AnalysisResult) (bool, error) {
//	return true, nil
//}

// IsClusteringCompacting get clustering compaction info by collection id
func (t *ClusteringCompactionManager) IsClusteringCompacting(collectionID UniqueID) bool {
	infos := t.meta.GetClusteringCompactionInfosByID(collectionID)
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
	infos := t.meta.GetClusteringCompactionInfos()
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
