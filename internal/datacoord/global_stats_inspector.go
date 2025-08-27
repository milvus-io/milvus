package datacoord

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type GlobalStatsInspector interface {
	Start()
	Stop()
}

var _ GlobalStatsInspector = (*globalStatsInspector)(nil)

type globalStatsInspector struct {
	ctx       context.Context
	cancel    context.CancelFunc
	loopWg    sync.WaitGroup
	mt        *meta
	scheduler task.GlobalScheduler
	allocator allocator.Allocator
	handler   Handler
}

func newGlobalStatsInspector(ctx context.Context,
	mt *meta,
	scheduler task.GlobalScheduler,
	allocator allocator.Allocator,
	handler Handler,
) *globalStatsInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &globalStatsInspector{
		ctx:       ctx,
		cancel:    cancel,
		loopWg:    sync.WaitGroup{},
		mt:        mt,
		scheduler: scheduler,
		allocator: allocator,
		handler:   handler,
	}
}

func (gsi *globalStatsInspector) Start() {
	gsi.reloadFromMeta()
	gsi.loopWg.Add(2)
	go gsi.triggerGlobalStatsTaskLoop()
	go gsi.cleanupGlobalStatsTasksLoop()
}

func (gsi *globalStatsInspector) reloadFromMeta() {
	tasks := gsi.mt.globalStatsMeta.GetAllTasks()
	for _, st := range tasks {
		if st.GetState() != indexpb.JobState_JobStateInit &&
			st.GetState() != indexpb.JobState_JobStateRetry &&
			st.GetState() != indexpb.JobState_JobStateInProgress {
			continue
		}
		if gsi.mt.GetCollection(st.GetCollectionID()) == nil {
			continue
		}

		gsi.scheduler.Enqueue(newGlobalStatsTask(
			proto.Clone(st).(*datapb.GlobalStatsTask),
			gsi.calculateGlobalStatsTaskSlot(st.GetCollectionID(), st.GetVChannel()),
			gsi.mt,
			gsi.handler,
			gsi.allocator,
		))
	}
}

func (gsi *globalStatsInspector) Stop() {
	gsi.cancel()
	gsi.loopWg.Wait()
}

func (gsi *globalStatsInspector) triggerGlobalStatsTaskLoop() {
	log.Info("start triggerGlobalStatsTaskLoop...")
	defer gsi.loopWg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.GlobalStatsTriggerInterval.GetAsDuration(time.Second))
	defer ticker.Stop()

	for {
		select {
		case <-gsi.ctx.Done():
			log.Warn("DataCoord context done, exit triggerGlobalStatsTaskLoop...")
			return
		case <-ticker.C:
			gsi.triggerGlobalStatsTask()
		}
	}
}

func (gsi *globalStatsInspector) triggerGlobalStatsTask() {
	collections := gsi.mt.GetCollections()
	for _, collection := range collections {
		for _, channel := range collection.VChannelNames {
			gsi.findFlushedSegmentsForChannel(collection.ID, channel)
		}
	}
}

func (gsi *globalStatsInspector) findFlushedSegmentsForChannel(collectionID int64, vChannel string) {
	flushedSegments := gsi.getFlushedSegmentsByChannel(vChannel)

	log.Info("found flushed segments for channel",
		zap.Int64("collectionID", collectionID),
		zap.String("vChannel", vChannel),
		zap.Int("segmentCount", len(flushedSegments)))
	if len(flushedSegments) == 0 {
		return
	}

	taskID, err := gsi.allocator.AllocID(gsi.ctx)
	if err != nil {
		log.Warn("failed to alloc task id for global stats task", zap.Error(err))
		return
	}

	segmentIDs := make([]int64, 0, len(flushedSegments))
	segmentInfos := make([]*datapb.SegmentInfo, 0, len(flushedSegments))
	for _, segment := range flushedSegments {
		segmentIDs = append(segmentIDs, segment.GetID())

		clonedSegment := segment.Clone()
		err := binlog.DecompressBinLogs(clonedSegment.SegmentInfo)
		if err != nil {
			log.Warn("failed to decompress binlogs for segment",
				zap.Int64("segmentID", segment.GetID()),
				zap.Error(err))
			continue
		}

		segmentInfos = append(segmentInfos, clonedSegment.SegmentInfo)
	}

	globalStatsTask := &datapb.GlobalStatsTask{
		ClusterID:     Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskID:        taskID,
		CollectionID:  collectionID,
		VChannel:      vChannel,
		Version:       0,
		SegmentInfos:  segmentInfos,
		StorageConfig: createStorageConfig(),
		Schema:        gsi.mt.GetCollection(collectionID).Schema,
		State:         indexpb.JobState_JobStateInit,
		FailReason:    "",
	}

	taskSlot := gsi.calculateGlobalStatsTaskSlot(collectionID, vChannel)
	if err = gsi.mt.globalStatsMeta.AddGlobalStatsTask(globalStatsTask); err != nil {
		if errors.Is(err, merr.ErrTaskDuplicate) {
			log.RatedInfo(10, "stats task already exists", zap.Int64("taskID", taskID))
			return
		}
		return
	}
	gsi.scheduler.Enqueue(newGlobalStatsTask(
		globalStatsTask,
		taskSlot,
		gsi.mt,
		gsi.handler,
		gsi.allocator,
	))

	log.Info("created global stats task",
		zap.Int64("taskID", taskID),
		zap.Int64("collectionID", collectionID),
		zap.String("vChannel", vChannel),
		zap.Int("segmentCount", len(segmentIDs)))
}

func (gsi *globalStatsInspector) cleanupGlobalStatsTasksLoop() {
	log.Info("start cleanupGlobalStatsTasksLoop...")
	defer gsi.loopWg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.GCInterval.GetAsDuration(time.Second))
	defer ticker.Stop()

	for {
		select {
		case <-gsi.ctx.Done():
			log.Warn("DataCoord context done, exit cleanupGlobalStatsTasksLoop...")
			return
		case <-ticker.C:
			gsi.cleanupGlobalStatsTasks()
		}
	}
}

func (gsi *globalStatsInspector) cleanupGlobalStatsTasks() {
	log.Info("start cleanupGlobalStatsTasks...")
	defer gsi.loopWg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.GCInterval.GetAsDuration(time.Second))
	defer ticker.Stop()

	for {
		select {
		case <-gsi.ctx.Done():
			log.Warn("DataCoord context done, exit cleanupGlobalStatsTasks...")
			return
		case <-ticker.C:
			start := time.Now()
			log.Info("start cleanupUnusedStatsTasks...", zap.Time("startAt", start))

			taskIDs := gsi.mt.globalStatsMeta.CanCleanedTasks()
			for _, taskID := range taskIDs {
				if err := gsi.mt.globalStatsMeta.DropGlobalStatsTask(gsi.ctx, taskID); err != nil {
					log.Warn("clean up stats task failed", zap.Int64("taskID", taskID), zap.Error(err))
				}
			}
			log.Info("cleanupUnusedStatsTasks done", zap.Duration("timeCost", time.Since(start)))
		}
	}
}

func (gsi *globalStatsInspector) getFlushedSegmentsByChannel(vChannel string) []*SegmentInfo {
	segments := gsi.mt.GetRealSegmentsForChannel(vChannel)
	flushedSegments := make([]*SegmentInfo, 0)

	for _, segment := range segments {
		if segment.GetState() == commonpb.SegmentState_Flushed && !segment.GetIsInvisible() {
			flushedSegments = append(flushedSegments, segment)
		}
	}

	return flushedSegments
}

func (gsi *globalStatsInspector) calculateGlobalStatsTaskSlot(collectionID int64, vChannel string) int64 {
	segments := gsi.mt.GetRealSegmentsForChannel(vChannel)
	totalSize := int64(0)
	for _, segment := range segments {
		if segment.GetState() == commonpb.SegmentState_Flushed {
			totalSize += segment.GetNumOfRows()
		}
	}

	defaultSlots := int64(1)
	if totalSize > 1000000 {
		return max(defaultSlots*2, 1)
	} else if totalSize > 100000 {
		return max(defaultSlots, 1)
	} else if totalSize > 10000 {
		return max(defaultSlots/2, 1)
	}
	return max(defaultSlots/4, 1)
}
