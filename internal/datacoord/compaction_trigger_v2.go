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
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/logutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type CompactionTriggerType int8

const (
	TriggerTypeLevelZeroViewChange CompactionTriggerType = iota + 1
	TriggerTypeLevelZeroViewIDLE
	TriggerTypeLevelZeroViewManual
	TriggerTypeSegmentSizeViewChange
	TriggerTypeClustering
	TriggerTypeSingle
	TriggerTypeSort
)

func (t CompactionTriggerType) String() string {
	switch t {
	case TriggerTypeLevelZeroViewChange:
		return "LevelZeroViewChange"
	case TriggerTypeLevelZeroViewIDLE:
		return "LevelZeroViewIDLE"
	case TriggerTypeLevelZeroViewManual:
		return "LevelZeroViewManual"
	case TriggerTypeSegmentSizeViewChange:
		return "SegmentSizeViewChange"
	case TriggerTypeClustering:
		return "Clustering"
	case TriggerTypeSingle:
		return "Single"
	case TriggerTypeSort:
		return "Sort"
	default:
		return ""
	}
}

type TriggerManager interface {
	Start()
	Stop()
	OnCollectionUpdate(collectionID int64)
	ManualTrigger(ctx context.Context, collectionID int64, clusteringCompaction bool, l0Compaction bool) (UniqueID, error)
	GetPauseCompactionChan(jobID, collectionID int64) <-chan struct{}
	GetResumeCompactionChan(jobID, collectionID int64) <-chan struct{}
}

var _ TriggerManager = (*CompactionTriggerManager)(nil)

// CompactionTriggerManager registers Triggers to TriggerType
// so that when the certain TriggerType happens, the corresponding triggers can
// trigger the correct compaction plans.
// Trigger types:
// 1. Change of Views
//   - LevelZeroViewTrigger
//   - SegmentSizeViewTrigger
//
// 2. SystemIDLE & schedulerIDLE
// 3. Manual Compaction
type CompactionTriggerManager struct {
	inspector CompactionInspector
	handler   Handler
	allocator allocator.Allocator

	meta             *meta
	importMeta       ImportMeta
	l0Policy         *l0CompactionPolicy
	clusteringPolicy *clusteringCompactionPolicy
	singlePolicy     *singleCompactionPolicy

	cancel  context.CancelFunc
	closeWg sync.WaitGroup

	l0Triggering bool
	l0SigLock    *sync.Mutex
	l0TickSig    *sync.Cond

	pauseCompactionChanMap  map[int64]chan struct{}
	resumeCompactionChanMap map[int64]chan struct{}
	compactionChanLock      sync.Mutex
}

func NewCompactionTriggerManager(alloc allocator.Allocator, handler Handler, inspector CompactionInspector, meta *meta, importMeta ImportMeta) *CompactionTriggerManager {
	m := &CompactionTriggerManager{
		allocator:               alloc,
		handler:                 handler,
		inspector:               inspector,
		meta:                    meta,
		importMeta:              importMeta,
		pauseCompactionChanMap:  make(map[int64]chan struct{}),
		resumeCompactionChanMap: make(map[int64]chan struct{}),
	}
	m.l0SigLock = &sync.Mutex{}
	m.l0TickSig = sync.NewCond(m.l0SigLock)
	m.l0Policy = newL0CompactionPolicy(meta, alloc)
	m.clusteringPolicy = newClusteringCompactionPolicy(meta, m.allocator, m.handler)
	m.singlePolicy = newSingleCompactionPolicy(meta, m.allocator, m.handler)
	return m
}

// OnCollectionUpdate notifies L0Policy about latest collection's L0 segment changes
// This tells the l0 triggers about which collections are active
func (m *CompactionTriggerManager) OnCollectionUpdate(collectionID int64) {
	m.l0Policy.OnCollectionUpdate(collectionID)
}

func (m *CompactionTriggerManager) Start() {
	m.closeWg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go func() {
		defer m.closeWg.Done()
		m.loop(ctx)
	}()
}

func (m *CompactionTriggerManager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.closeWg.Wait()
}

func (m *CompactionTriggerManager) pauseL0SegmentCompacting(jobID, collectionID int64) {
	m.l0Policy.AddSkipCollection(collectionID)
	m.l0SigLock.Lock()
	for m.l0Triggering {
		m.l0TickSig.Wait()
	}
	m.l0SigLock.Unlock()
	m.compactionChanLock.Lock()
	if ch, ok := m.pauseCompactionChanMap[jobID]; ok {
		close(ch)
	}
	m.compactionChanLock.Unlock()
}

func (m *CompactionTriggerManager) resumeL0SegmentCompacting(jobID, collectionID int64) {
	m.compactionChanLock.Lock()
	m.l0Policy.RemoveSkipCollection(collectionID)
	if ch, ok := m.resumeCompactionChanMap[jobID]; ok {
		close(ch)
		delete(m.pauseCompactionChanMap, jobID)
		delete(m.resumeCompactionChanMap, jobID)
	}
	m.compactionChanLock.Unlock()
}

func (m *CompactionTriggerManager) GetPauseCompactionChan(jobID, collectionID int64) <-chan struct{} {
	m.compactionChanLock.Lock()
	defer m.compactionChanLock.Unlock()
	if ch, ok := m.pauseCompactionChanMap[jobID]; ok {
		return ch
	}
	ch := make(chan struct{})
	m.pauseCompactionChanMap[jobID] = ch
	go m.pauseL0SegmentCompacting(jobID, collectionID)
	return ch
}

func (m *CompactionTriggerManager) GetResumeCompactionChan(jobID, collectionID int64) <-chan struct{} {
	m.compactionChanLock.Lock()
	defer m.compactionChanLock.Unlock()
	if ch, ok := m.resumeCompactionChanMap[jobID]; ok {
		return ch
	}
	ch := make(chan struct{})
	m.resumeCompactionChanMap[jobID] = ch
	go m.resumeL0SegmentCompacting(jobID, collectionID)
	return ch
}

func (m *CompactionTriggerManager) setL0Triggering(b bool) {
	m.l0SigLock.Lock()
	defer m.l0SigLock.Unlock()
	m.l0Triggering = b
	if !b {
		m.l0TickSig.Broadcast()
	}
}

func (m *CompactionTriggerManager) loop(ctx context.Context) {
	defer logutil.LogPanic()

	log := log.Ctx(ctx)
	l0Ticker := time.NewTicker(Params.DataCoordCfg.L0CompactionTriggerInterval.GetAsDuration(time.Second))
	defer l0Ticker.Stop()
	clusteringTicker := time.NewTicker(Params.DataCoordCfg.ClusteringCompactionTriggerInterval.GetAsDuration(time.Second))
	defer clusteringTicker.Stop()
	singleTicker := time.NewTicker(Params.DataCoordCfg.MixCompactionTriggerInterval.GetAsDuration(time.Second))
	defer singleTicker.Stop()
	log.Info("Compaction trigger manager start")
	for {
		select {
		case <-ctx.Done():
			log.Info("Compaction trigger manager checkLoop quit")
			return
		case <-l0Ticker.C:
			if !m.l0Policy.Enable() {
				continue
			}
			if m.inspector.isFull() {
				log.RatedInfo(10, "Skip trigger l0 compaction since inspector is full")
				continue
			}
			m.setL0Triggering(true)
			events, err := m.l0Policy.Trigger(ctx)
			if err != nil {
				log.Warn("Fail to trigger L0 policy", zap.Error(err))
				m.setL0Triggering(false)
				continue
			}
			if len(events) > 0 {
				for triggerType, views := range events {
					m.notify(ctx, triggerType, views)
				}
			}
			m.setL0Triggering(false)
		case <-clusteringTicker.C:
			if !m.clusteringPolicy.Enable() {
				continue
			}
			if m.inspector.isFull() {
				log.RatedInfo(10, "Skip trigger clustering compaction since inspector is full")
				continue
			}
			events, err := m.clusteringPolicy.Trigger(ctx)
			if err != nil {
				log.Warn("Fail to trigger clustering policy", zap.Error(err))
				continue
			}
			if len(events) > 0 {
				for triggerType, views := range events {
					m.notify(ctx, triggerType, views)
				}
			}
		case <-singleTicker.C:
			if !m.singlePolicy.Enable() {
				continue
			}
			if m.inspector.isFull() {
				log.RatedInfo(10, "Skip trigger single compaction since inspector is full")
				continue
			}
			events, err := m.singlePolicy.Trigger(ctx)
			if err != nil {
				log.Warn("Fail to trigger single policy", zap.Error(err))
				continue
			}
			if len(events) > 0 {
				for triggerType, views := range events {
					m.notify(ctx, triggerType, views)
				}
			}
		case segID := <-getStatsTaskChSingleton():
			log.Info("receive new segment to trigger sort compaction", zap.Int64("segmentID", segID))
			view := m.singlePolicy.triggerSegmentSortCompaction(ctx, segID)
			if view == nil {
				log.Warn("segment no need to do sort compaction", zap.Int64("segmentID", segID))
				continue
			}
			m.notify(ctx, TriggerTypeSort, []CompactionView{view})
		}
	}
}

func (m *CompactionTriggerManager) ManualTrigger(ctx context.Context, collectionID int64, clusteringCompaction bool, l0Compaction bool) (UniqueID, error) {
	log.Ctx(ctx).Info("receive manual trigger", zap.Int64("collectionID", collectionID),
		zap.Bool("clusteringCompaction", clusteringCompaction), zap.Bool("l0Compaction", l0Compaction))

	var triggerID UniqueID
	var err error
	var views []CompactionView

	events := make(map[CompactionTriggerType][]CompactionView, 0)
	if l0Compaction {
		m.setL0Triggering(true)
		defer m.setL0Triggering(false)
		views, triggerID, err = m.l0Policy.triggerOneCollection(ctx, collectionID)
		events[TriggerTypeLevelZeroViewManual] = views
	} else if clusteringCompaction {
		views, triggerID, err = m.clusteringPolicy.triggerOneCollection(ctx, collectionID, true)
		events[TriggerTypeClustering] = views
	}
	if err != nil {
		return 0, err
	}
	if len(events) > 0 {
		for triggerType, views := range events {
			m.notify(ctx, triggerType, views)
		}
	}
	return triggerID, nil
}

func (m *CompactionTriggerManager) triggerViewForCompaction(ctx context.Context, eventType CompactionTriggerType,
	view CompactionView,
) ([]CompactionView, string) {
	if eventType == TriggerTypeLevelZeroViewIDLE {
		view, reason := view.ForceTrigger()
		return []CompactionView{view}, reason
	} else if eventType == TriggerTypeLevelZeroViewManual {
		return view.ForceTriggerAll()
	}
	outView, reason := view.Trigger()
	return []CompactionView{outView}, reason
}

func (m *CompactionTriggerManager) notify(ctx context.Context, eventType CompactionTriggerType, views []CompactionView) {
	log := log.Ctx(ctx)
	log.Debug("Start to trigger compactions", zap.String("eventType", eventType.String()))
	for _, view := range views {
		outViews, reason := m.triggerViewForCompaction(ctx, eventType, view)
		for _, outView := range outViews {
			if outView != nil {
				log.Info("Success to trigger a compaction, try to submit",
					zap.String("eventType", eventType.String()),
					zap.String("reason", reason),
					zap.String("output view", outView.String()),
					zap.Int64("triggerID", outView.GetTriggerID()))

				switch eventType {
				case TriggerTypeLevelZeroViewChange, TriggerTypeLevelZeroViewIDLE, TriggerTypeLevelZeroViewManual:
					m.SubmitL0ViewToScheduler(ctx, outView)
				case TriggerTypeClustering:
					m.SubmitClusteringViewToScheduler(ctx, outView)
				case TriggerTypeSingle:
					m.SubmitSingleViewToScheduler(ctx, outView, datapb.CompactionType_MixCompaction)
				case TriggerTypeSort:
					m.SubmitSingleViewToScheduler(ctx, outView, datapb.CompactionType_SortCompaction)
				}
			}
		}
	}
}

func (m *CompactionTriggerManager) SubmitL0ViewToScheduler(ctx context.Context, view CompactionView) {
	log := log.Ctx(ctx).With(zap.String("view", view.String()))
	taskID, err := m.allocator.AllocID(ctx)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return
	}

	levelZeroSegs := lo.Map(view.GetSegmentsView(), func(segView *SegmentView, _ int) int64 {
		return segView.ID
	})

	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.Error(err))
		return
	}

	err = m.addL0ImportTaskForImport(ctx, collection, view)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because add l0 import task fail", zap.Error(err))
		return
	}

	totalRows := lo.SumBy(view.GetSegmentsView(), func(segView *SegmentView) int64 {
		return segView.NumOfRows
	})

	task := &datapb.CompactionTask{
		TriggerID:     view.GetTriggerID(),
		PlanID:        taskID,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		StartTime:     time.Now().Unix(),
		TotalRows:     totalRows,
		InputSegments: levelZeroSegs,
		State:         datapb.CompactionTaskState_pipelining,
		Channel:       view.GetGroupLabel().Channel,
		CollectionID:  view.GetGroupLabel().CollectionID,
		PartitionID:   view.GetGroupLabel().PartitionID,
		Pos:           view.(*LevelZeroSegmentsView).earliestGrowingSegmentPos,
		Schema:        collection.Schema,
	}

	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("triggerID", task.GetTriggerID()),
			zap.Int64("planID", task.GetPlanID()),
			zap.Int64s("segmentIDs", task.GetInputSegments()),
			zap.Error(err))
		return
	}
	log.Info("Finish to submit a LevelZeroCompaction plan",
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
		zap.Int64s("L0 segments", levelZeroSegs),
	)
}

func (m *CompactionTriggerManager) addL0ImportTaskForImport(ctx context.Context, collection *collectionInfo, view CompactionView) error {
	// add l0 import task for the collection if the collection is importing
	importJobs := m.importMeta.GetJobBy(ctx,
		WithCollectionID(collection.ID),
		WithoutJobStates(internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed),
		WithoutL0Job(),
	)
	if len(importJobs) > 0 {
		partitionID := view.GetGroupLabel().PartitionID
		var (
			fileSize        int64 = 0
			totalRows       int64 = 0
			totalMemorySize int64 = 0
			importPaths     []string
		)
		idStart := time.Now().UnixMilli()
		for _, segmentView := range view.GetSegmentsView() {
			segInfo := m.meta.GetSegment(ctx, segmentView.ID)
			if segInfo == nil {
				continue
			}
			totalRows += int64(segmentView.DeltaRowCount)
			totalMemorySize += int64(segmentView.DeltaSize)
			for _, deltaLogs := range segInfo.GetDeltalogs() {
				for _, binlog := range deltaLogs.GetBinlogs() {
					fileSize += binlog.GetLogSize()
					importPaths = append(importPaths, binlog.GetLogPath())
				}
			}
		}
		if len(importPaths) == 0 {
			return nil
		}

		for i, job := range importJobs {
			newTasks, err := NewImportTasks([][]*datapb.ImportFileStats{
				{
					{
						ImportFile: &internalpb.ImportFile{
							Id:    idStart + int64(i),
							Paths: importPaths,
						},
						FileSize:        fileSize,
						TotalRows:       totalRows,
						TotalMemorySize: totalMemorySize,
						HashedStats: map[string]*datapb.PartitionImportStats{
							// which is vchannel
							view.GetGroupLabel().Channel: {
								PartitionRows: map[int64]int64{
									partitionID: totalRows,
								},
								PartitionDataSize: map[int64]int64{
									partitionID: totalMemorySize,
								},
							},
						},
					},
				},
			}, job, m.allocator, m.meta, m.importMeta, paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.GetAsInt())
			if err != nil {
				log.Warn("new import tasks failed", zap.Error(err))
				return err
			}
			for _, t := range newTasks {
				err = m.importMeta.AddTask(ctx, t)
				if err != nil {
					log.Warn("add new l0 import task from l0 compaction failed", WrapTaskLog(t, zap.Error(err))...)
					return err
				}
				log.Info("add new l0 import task from l0 compaction", WrapTaskLog(t)...)
			}
		}
	}
	return nil
}

func (m *CompactionTriggerManager) SubmitClusteringViewToScheduler(ctx context.Context, view CompactionView) {
	log := log.Ctx(ctx).With(zap.String("view", view.String()))
	taskID, _, err := m.allocator.AllocN(2)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return
	}
	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.Error(err))
		return
	}

	expectedSegmentSize := getExpectedSegmentSize(m.meta, collection.ID, collection.Schema)
	totalRows, maxSegmentRows, preferSegmentRows, err := calculateClusteringCompactionConfig(collection, view, expectedSegmentSize)
	if err != nil {
		log.Warn("Failed to calculate cluster compaction config fail", zap.Error(err))
		return
	}

	resultSegmentNum := (totalRows/preferSegmentRows + 1) * 2
	n := resultSegmentNum * paramtable.Get().DataCoordCfg.CompactionPreAllocateIDExpansionFactor.GetAsInt64()
	start, end, err := m.allocator.AllocN(n)
	if err != nil {
		log.Warn("pre-allocate result segments failed", zap.String("view", view.String()), zap.Error(err))
		return
	}
	task := &datapb.CompactionTask{
		PlanID:             taskID,
		TriggerID:          view.(*ClusteringSegmentsView).triggerID,
		State:              datapb.CompactionTaskState_pipelining,
		StartTime:          time.Now().Unix(),
		CollectionTtl:      view.(*ClusteringSegmentsView).collectionTTL.Nanoseconds(),
		Type:               datapb.CompactionType_ClusteringCompaction,
		CollectionID:       view.GetGroupLabel().CollectionID,
		PartitionID:        view.GetGroupLabel().PartitionID,
		Channel:            view.GetGroupLabel().Channel,
		Schema:             collection.Schema,
		ClusteringKeyField: view.(*ClusteringSegmentsView).clusteringKeyField,
		InputSegments:      lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:     []int64{},
		MaxSegmentRows:     maxSegmentRows,
		PreferSegmentRows:  preferSegmentRows,
		TotalRows:          totalRows,
		AnalyzeTaskID:      taskID + 1,
		LastStateStartTime: time.Now().Unix(),
		PreAllocatedSegmentIDs: &datapb.IDRange{
			Begin: start,
			End:   end,
		},
		MaxSize: expectedSegmentSize,
	}
	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("planID", task.GetPlanID()),
			zap.Error(err))
		return
	}
	log.Info("Finish to submit a clustering compaction task",
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.Int64("MaxSegmentRows", task.MaxSegmentRows),
		zap.Int64("PreferSegmentRows", task.PreferSegmentRows),
	)
}

func (m *CompactionTriggerManager) SubmitSingleViewToScheduler(ctx context.Context, view CompactionView, compactionType datapb.CompactionType) {
	log := log.Ctx(ctx).With(zap.String("view", view.String()))
	// TODO[GOOSE], 11 = 1 planID + 10 segmentID, this is a hack need to be removed.
	// Any plan that output segment number greater than 10 will be marked as invalid plan for now.
	n := 11 * paramtable.Get().DataCoordCfg.CompactionPreAllocateIDExpansionFactor.GetAsInt64()
	startID, endID, err := m.allocator.AllocN(n)
	if err != nil {
		log.Warn("fFailed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return
	}

	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.Error(err))
		return
	}
	var totalRows int64 = 0
	for _, s := range view.GetSegmentsView() {
		totalRows += s.NumOfRows
	}

	expectedSize := getExpectedSegmentSize(m.meta, collection.ID, collection.Schema)
	task := &datapb.CompactionTask{
		PlanID:             startID,
		TriggerID:          view.(*MixSegmentView).triggerID,
		State:              datapb.CompactionTaskState_pipelining,
		StartTime:          time.Now().Unix(),
		CollectionTtl:      view.(*MixSegmentView).collectionTTL.Nanoseconds(),
		Type:               compactionType, // todo: use SingleCompaction
		CollectionID:       view.GetGroupLabel().CollectionID,
		PartitionID:        view.GetGroupLabel().PartitionID,
		Channel:            view.GetGroupLabel().Channel,
		Schema:             collection.Schema,
		InputSegments:      lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:     []int64{},
		TotalRows:          totalRows,
		LastStateStartTime: time.Now().Unix(),
		MaxSize:            getExpandedSize(expectedSize),
		PreAllocatedSegmentIDs: &datapb.IDRange{
			Begin: startID + 1,
			End:   endID,
		},
	}
	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("triggerID", task.GetTriggerID()),
			zap.Int64("planID", task.GetPlanID()),
			zap.Int64s("segmentIDs", task.GetInputSegments()),
			zap.Error(err))
	}
	log.Info("Finish to submit a single compaction task",
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
	)
}

func getExpectedSegmentSize(meta *meta, collectionID int64, schema *schemapb.CollectionSchema) int64 {
	allDiskIndex := meta.indexMeta.AllDenseWithDiskIndex(collectionID, schema)
	if allDiskIndex {
		// Only if all dense vector fields index type are DiskANN, recalc segment max size here.
		return Params.DataCoordCfg.DiskSegmentMaxSize.GetAsInt64() * 1024 * 1024
	}
	// If some dense vector fields index type are not DiskANN, recalc segment max size using default policy.
	return Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
}

// chanPartSegments is an internal result struct, which is aggregates of SegmentInfos with same collectionID, partitionID and channelName
type chanPartSegments struct {
	collectionID UniqueID
	partitionID  UniqueID
	channelName  string
	segments     []*SegmentInfo
}
