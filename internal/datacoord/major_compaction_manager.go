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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
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

//type MajorCompactionManager interface {
//	start()
//	stop()
//	submit(job *MajorCompactionJob) error
//}

type MajorCompactionManager struct {
	ctx               context.Context
	meta              *meta
	allocator         allocator
	compactionHandler compactionPlanContext
	scheduler         Scheduler
	analysisScheduler *analysisTaskScheduler

	forceMu sync.Mutex
	quit    chan struct{}
	wg      sync.WaitGroup
	signals chan *compactionSignal
	ticker  *time.Ticker

	majorCompactions map[UniqueID]*MajorCompactionJob
}

func newMajorCompactionManager(
	ctx context.Context,
	meta *meta,
	allocator allocator,
	compactionHandler compactionPlanContext,
	analysisScheduler *analysisTaskScheduler,
) *MajorCompactionManager {
	return &MajorCompactionManager{
		ctx:               ctx,
		meta:              meta,
		allocator:         allocator,
		compactionHandler: compactionHandler,
		analysisScheduler: analysisScheduler,
	}
}

func (t *MajorCompactionManager) start() {
	t.quit = make(chan struct{})
	t.ticker = time.NewTicker(Params.DataCoordCfg.MajorCompactionCheckInterval.GetAsDuration(time.Second))
	t.wg.Add(1)
	go t.startMajorCompactionLoop()
}

func (t *MajorCompactionManager) stop() {
	close(t.quit)
	t.wg.Wait()
}

func (t *MajorCompactionManager) submit(job *MajorCompactionJob) error {
	log.Info("Insert major compaction job", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID))
	t.saveMajorCompactionJob(job)
	err := t.runCompactionJob(job)
	if err != nil {
		job.state = failed
		log.Warn("mark major compaction job failed", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID))
		t.saveMajorCompactionJob(job)
	}
	return nil
}

func (t *MajorCompactionManager) startMajorCompactionLoop() {
	defer logutil.LogPanic()
	defer t.wg.Done()
	for {
		select {
		case <-t.quit:
			t.ticker.Stop()
			log.Info("major compaction loop exit")
			return
		case <-t.ticker.C:
			err := t.checkAllJobState()
			if err != nil {
				log.Warn("unable to triggerMajorCompaction", zap.Error(err))
			}
		}
	}
}

func (t *MajorCompactionManager) checkAllJobState() error {
	majorCompactionJobs := t.GetAllMajorCompactionJobs()
	for _, job := range majorCompactionJobs {
		err := t.checkJobState(job)
		if err != nil {
			log.Error("fail to check job state", zap.Error(err))
			job.state = failed
			log.Warn("mark major compaction job failed", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID))
			t.saveMajorCompactionJob(job)
		}
	}
	return nil
}

func (t *MajorCompactionManager) checkJobState(job *MajorCompactionJob) error {
	if job.state == completed || job.state == failed || job.state == timeout {
		if time.Since(tsoutil.PhysicalTime(job.startTime)) > Params.DataCoordCfg.MajorCompactionDropTolerance.GetAsDuration(time.Second) {
			// skip handle this error, try best to delete meta
			t.dropMajorCompactionJob(job)
		}
		return nil
	}
	pipeliningPlans := make([]*datapb.CompactionPlan, 0)
	executingPlans := make([]*datapb.CompactionPlan, 0)
	completedPlans := make([]*datapb.CompactionPlan, 0)
	failedPlans := make([]*datapb.CompactionPlan, 0)
	timeoutPlans := make([]*datapb.CompactionPlan, 0)
	checkFunc := func(plans []*datapb.CompactionPlan) {
		for _, plan := range plans {
			compactionTask := t.compactionHandler.getCompaction(plan.GetPlanID())
			// todo: if datacoord crash during major compaction, compactTask will lost, we can resubmit these plan
			if compactionTask == nil {
				// if one compaction task is lost, mark it as failed, and the major compaction will be marked failed as well
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
		}
	}
	checkFunc(job.pipeliningPlans)
	checkFunc(job.executingPlans)
	checkFunc(job.completedPlans)
	checkFunc(job.failedPlans)
	checkFunc(job.timeoutPlans)

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

	log.Info("Update major compaction job", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID), zap.String("state", datapb.CompactionTaskState(job.state).String()))
	return t.saveMajorCompactionJob(job)
}

func (t *MajorCompactionManager) generateNewPlans(job *MajorCompactionJob) []*datapb.CompactionPlan {
	return nil
}

func (t *MajorCompactionManager) runCompactionJob(job *MajorCompactionJob) error {
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
		plan.TimeoutInSeconds = Params.DataCoordCfg.MajorCompactionTimeoutInSeconds.GetAsInt32()

		// major compaction firstly analyze the plan, then decide whether to execute compaction
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

		//shouldDo, err := t.shouldDoMajorCompaction(analyzeResult)
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
		log.Info("execute major compaction plan", zap.Int64("planID", plan.GetPlanID()), zap.Int64s("segmentIDs", segIDs))

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
	log.Info("Update major compaction job", zap.Int64("tiggerID", job.triggerID), zap.Int64("collectionID", job.collectionID))
	err := t.saveMajorCompactionJob(job)
	return err
}

//func (t *MajorCompactionManager) shouldDoMajorCompaction(analyzeResult *indexpb.AnalysisResult) (bool, error) {
//	return true, nil
//}

// IsMajorCompacting get major compaction info by collection id
func (t *MajorCompactionManager) IsMajorCompacting(collectionID UniqueID) bool {
	infos := t.meta.GetMajorCompactionInfos(collectionID)
	executingInfos := lo.Filter(infos, func(info *datapb.MajorCompactionInfo, _ int) bool {
		return info.State == datapb.CompactionTaskState_analyzing ||
			info.State == datapb.CompactionTaskState_executing ||
			info.State == datapb.CompactionTaskState_pipelining
	})
	return len(executingInfos) > 0
}

func (t *MajorCompactionManager) setSegmentsCompacting(plan *datapb.CompactionPlan, compacting bool) {
	for _, segmentBinlogs := range plan.GetSegmentBinlogs() {
		t.meta.SetSegmentCompacting(segmentBinlogs.GetSegmentID(), compacting)
	}
}

func (t *MajorCompactionManager) fillMajorCompactionPlans(segments []*SegmentInfo, clusteringKeyId int64, compactTime *compactTime) []*datapb.CompactionPlan {
	plan := segmentsToPlan(segments, datapb.CompactionType_MajorCompaction, compactTime)
	plan.ClusteringKeyId = clusteringKeyId
	majorMaxSegmentSize := paramtable.Get().DataCoordCfg.MajorCompactionMaxSegmentSize.GetAsSize()
	majorPreferSegmentSize := paramtable.Get().DataCoordCfg.MajorCompactionPreferSegmentSize.GetAsSize()
	segmentMaxSize := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	plan.MaxSegmentRows = segments[0].MaxRowNum * majorMaxSegmentSize / segmentMaxSize
	plan.PreferSegmentRows = segments[0].MaxRowNum * majorPreferSegmentSize / segmentMaxSize
	return []*datapb.CompactionPlan{
		plan,
	}
}

// GetAllMajorCompactionJobs returns cloned MajorCompactionJob from local meta cache
func (t *MajorCompactionManager) GetAllMajorCompactionJobs() []*MajorCompactionJob {
	jobs := make([]*MajorCompactionJob, 0)
	infos := t.meta.GetClonedMajorCompactionInfos()
	for _, info := range infos {
		job := convertMajorCompactionJob(info)
		jobs = append(jobs, job)
	}
	return jobs
}

// dropMajorCompactionJob drop major compaction job in meta
func (t *MajorCompactionManager) dropMajorCompactionJob(job *MajorCompactionJob) error {
	info := convertFromMajorCompactionJob(job)
	return t.meta.DropMajorCompactionInfo(info)
}

func (t *MajorCompactionManager) saveMajorCompactionJob(job *MajorCompactionJob) error {
	info := convertFromMajorCompactionJob(job)
	return t.meta.SaveMajorCompactionInfo(info)
}

func triggerMajorCompactionPolicy(ctx context.Context, meta *meta, collectionID int64, partitionID int64, channel string, segments []*SegmentInfo) (bool, error) {
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
		if newDataSize > Params.DataCoordCfg.MajorCompactionNewDataSizeThreshold.GetAsSize() {
			log.Info("New data is larger than threshold, do compaction", zap.Int64("newDataSize", newDataSize))
			return true, nil
		}
		log.Info("No partition stats and no enough new data, skip compaction")
		return false, nil
	}

	pTime, _ := tsoutil.ParseTS(uint64(version))
	if time.Since(pTime) < Params.DataCoordCfg.MajorCompactionMinInterval.GetAsDuration(time.Second) {
		log.Info("Too short time before last major compaction, skip compaction")
		return false, nil
	}
	if time.Since(pTime) > Params.DataCoordCfg.MajorCompactionMaxInterval.GetAsDuration(time.Second) {
		log.Info("It is a long time after last major compaction, do compaction")
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
	//if ratio > Params.DataCoordCfg.MajorCompactionNewDataRatioThreshold.GetAsFloat() {
	//	log.Info("New data is larger than threshold, do compaction", zap.Float64("ratio", ratio))
	//	return true, nil
	//}
	//log.Info("New data is smaller than threshold, skip compaction", zap.Float64("ratio", ratio))
	//return false, nil

	// size based
	if uncompactedSegmentSize > Params.DataCoordCfg.MajorCompactionNewDataSizeThreshold.GetAsSize() {
		log.Info("New data is larger than threshold, do compaction", zap.Int64("newDataSize", uncompactedSegmentSize))
		return true, nil
	}
	log.Info("New data is smaller than threshold, skip compaction", zap.Int64("newDataSize", uncompactedSegmentSize))
	return false, nil
}
