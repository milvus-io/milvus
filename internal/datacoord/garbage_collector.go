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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// GcOption garbage collection options
type GcOption struct {
	cli              storage.ChunkManager // client
	enabled          bool                 // enable switch
	checkInterval    time.Duration        // each interval
	missingTolerance time.Duration        // key missing in meta tolerance time
	dropTolerance    time.Duration        // dropped segment related key tolerance time
	scanInterval     time.Duration        // interval for scan residue for interupted log wrttien

	broker           broker.Broker
	removeObjectPool *conc.Pool[struct{}]
}

// garbageCollector handles garbage files in object storage
// which could be dropped collection remanent or data node failure traces
type garbageCollector struct {
	ctx    context.Context
	cancel context.CancelFunc

	option  GcOption
	meta    *meta
	handler Handler

	startOnce  sync.Once
	stopOnce   sync.Once
	wg         sync.WaitGroup
	cmdCh      chan gcCmd
	pauseUntil atomic.Time
}
type gcCmd struct {
	cmdType  datapb.GcCommand
	duration time.Duration
	done     chan struct{}
}

// newGarbageCollector create garbage collector with meta and option
func newGarbageCollector(meta *meta, handler Handler, opt GcOption) *garbageCollector {
	log.Info("GC with option",
		zap.Bool("enabled", opt.enabled),
		zap.Duration("interval", opt.checkInterval),
		zap.Duration("scanInterval", opt.scanInterval),
		zap.Duration("missingTolerance", opt.missingTolerance),
		zap.Duration("dropTolerance", opt.dropTolerance))
	opt.removeObjectPool = conc.NewPool[struct{}](Params.DataCoordCfg.GCRemoveConcurrent.GetAsInt(), conc.WithExpiryDuration(time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	return &garbageCollector{
		ctx:     ctx,
		cancel:  cancel,
		meta:    meta,
		handler: handler,
		option:  opt,
		cmdCh:   make(chan gcCmd),
	}
}

// start a goroutine and perform gc check every `checkInterval`
func (gc *garbageCollector) start() {
	if gc.option.enabled {
		if gc.option.cli == nil {
			log.Warn("DataCoord gc enabled, but SSO client is not provided")
			return
		}
		gc.startOnce.Do(func() {
			gc.work(gc.ctx)
		})
	}
}

func (gc *garbageCollector) Pause(ctx context.Context, pauseDuration time.Duration) error {
	if !gc.option.enabled {
		log.Info("garbage collection not enabled")
		return nil
	}
	done := make(chan struct{})
	select {
	case gc.cmdCh <- gcCmd{
		cmdType:  datapb.GcCommand_Pause,
		duration: pauseDuration,
		done:     done,
	}:
		<-done
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (gc *garbageCollector) Resume(ctx context.Context) error {
	if !gc.option.enabled {
		log.Warn("garbage collection not enabled, cannot resume")
		return merr.WrapErrServiceUnavailable("garbage collection not enabled")
	}
	done := make(chan struct{})
	select {
	case gc.cmdCh <- gcCmd{
		cmdType: datapb.GcCommand_Resume,
		done:    done,
	}:
		<-done
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// work contains actual looping check logic
func (gc *garbageCollector) work(ctx context.Context) {
	// TODO: fast cancel for gc when closing.
	// Run gc tasks in parallel.
	gc.wg.Add(3)
	go func() {
		defer gc.wg.Done()
		gc.runRecycleTaskWithPauser(ctx, "meta", gc.option.checkInterval, func(ctx context.Context) {
			gc.recycleDroppedSegments(ctx)
			gc.recycleChannelCPMeta(ctx)
			gc.recycleUnusedIndexes(ctx)
			gc.recycleUnusedSegIndexes(ctx)
			gc.recycleUnusedAnalyzeFiles(ctx)
		})
	}()
	go func() {
		defer gc.wg.Done()
		gc.runRecycleTaskWithPauser(ctx, "orphan", gc.option.scanInterval, func(ctx context.Context) {
			gc.recycleUnusedBinlogFiles(ctx)
			gc.recycleUnusedIndexFiles(ctx)
		})
	}()
	go func() {
		defer gc.wg.Done()
		gc.startControlLoop(ctx)
	}()
}

// startControlLoop start a control loop for garbageCollector.
func (gc *garbageCollector) startControlLoop(_ context.Context) {
	for {
		select {
		case cmd := <-gc.cmdCh:
			switch cmd.cmdType {
			case datapb.GcCommand_Pause:
				pauseUntil := time.Now().Add(cmd.duration)
				if pauseUntil.After(gc.pauseUntil.Load()) {
					log.Info("garbage collection paused", zap.Duration("duration", cmd.duration), zap.Time("pauseUntil", pauseUntil))
					gc.pauseUntil.Store(pauseUntil)
				} else {
					log.Info("new pause until before current value", zap.Duration("duration", cmd.duration), zap.Time("pauseUntil", pauseUntil), zap.Time("oldPauseUntil", gc.pauseUntil.Load()))
				}
			case datapb.GcCommand_Resume:
				// reset to zero value
				gc.pauseUntil.Store(time.Time{})
				log.Info("garbage collection resumed")
			}
			close(cmd.done)
		case <-gc.ctx.Done():
			log.Warn("garbage collector control loop quit")
			return
		}
	}
}

// runRecycleTaskWithPauser is a helper function to create a task with pauser
func (gc *garbageCollector) runRecycleTaskWithPauser(ctx context.Context, name string, interval time.Duration, task func(ctx context.Context)) {
	logger := log.With(zap.String("gcType", name)).With(zap.Duration("interval", interval))
	timer := time.NewTicker(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if time.Now().Before(gc.pauseUntil.Load()) {
				logger.Info("garbage collector paused", zap.Time("until", gc.pauseUntil.Load()))
				continue
			}
			logger.Info("garbage collector recycle task start...")
			start := time.Now()
			task(ctx)
			logger.Info("garbage collector recycle task done", zap.Duration("timeCost", time.Since(start)))
		}
	}
}

// close stop the garbage collector.
func (gc *garbageCollector) close() {
	gc.stopOnce.Do(func() {
		gc.cancel()
		gc.wg.Wait()
	})
}

// recycleUnusedBinlogFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) recycleUnusedBinlogFiles(ctx context.Context) {
	start := time.Now()
	log := log.With(zap.String("gcName", "recycleUnusedBinlogFiles"), zap.Time("startAt", start))
	log.Info("start recycleUnusedBinlogFiles...")
	defer func() { log.Info("recycleUnusedBinlogFiles done", zap.Duration("timeCost", time.Since(start))) }()

	type scanTask struct {
		prefix  string
		checker func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool
		label   string
	}
	scanTasks := []scanTask{
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentInsertLogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				return segment != nil
			},
			label: metrics.InsertFileLabel,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentStatslogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				logID, err := binlog.GetLogIDFromBingLogPath(objectInfo.FilePath)
				if err != nil {
					log.Warn("garbageCollector find dirty stats log", zap.String("filePath", objectInfo.FilePath), zap.Error(err))
					return false
				}
				return segment != nil && segment.IsStatsLogExists(logID)
			},
			label: metrics.StatFileLabel,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentDeltaLogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				logID, err := binlog.GetLogIDFromBingLogPath(objectInfo.FilePath)
				if err != nil {
					log.Warn("garbageCollector find dirty dleta log", zap.String("filePath", objectInfo.FilePath), zap.Error(err))
					return false
				}
				return segment != nil && segment.IsDeltaLogExists(logID)
			},
			label: metrics.DeleteFileLabel,
		},
	}

	for _, task := range scanTasks {
		gc.recycleUnusedBinLogWithChecker(ctx, task.prefix, task.label, task.checker)
	}
	metrics.GarbageCollectorRunCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Add(1)
}

// recycleUnusedBinLogWithChecker scans the prefix and checks the path with checker.
// GC the file if checker returns false.
func (gc *garbageCollector) recycleUnusedBinLogWithChecker(ctx context.Context, prefix string, label string, checker func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool) {
	logger := log.With(zap.String("prefix", prefix))
	logger.Info("garbageCollector recycleUnusedBinlogFiles start", zap.String("prefix", prefix))
	lastFilePath := ""
	total := 0
	valid := 0
	unexpectedFailure := atomic.NewInt32(0)
	removed := atomic.NewInt32(0)
	start := time.Now()

	futures := make([]*conc.Future[struct{}], 0)
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(chunkInfo *storage.ChunkObjectInfo) bool {
		total++
		lastFilePath = chunkInfo.FilePath

		// Check file tolerance first to avoid unnecessary operation.
		if time.Since(chunkInfo.ModifyTime) <= gc.option.missingTolerance {
			logger.Info("garbageCollector recycleUnusedBinlogFiles skip file since it is not expired", zap.String("filePath", chunkInfo.FilePath), zap.Time("modifyTime", chunkInfo.ModifyTime))
			return true
		}

		// Parse segmentID from file path.
		// TODO: Does all files in the same segment have the same segmentID?
		segmentID, err := storage.ParseSegmentIDByBinlog(gc.option.cli.RootPath(), chunkInfo.FilePath)
		if err != nil {
			unexpectedFailure.Inc()
			logger.Warn("garbageCollector recycleUnusedBinlogFiles parse segment id error",
				zap.String("filePath", chunkInfo.FilePath),
				zap.Error(err))
			return true
		}

		segment := gc.meta.GetSegment(segmentID)
		if checker(chunkInfo, segment) {
			valid++
			logger.Info("garbageCollector recycleUnusedBinlogFiles skip file since it is valid", zap.String("filePath", chunkInfo.FilePath), zap.Int64("segmentID", segmentID))
			return true
		}

		// ignore error since it could be cleaned up next time
		file := chunkInfo.FilePath
		future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
			logger := logger.With(zap.String("file", file))
			logger.Info("garbageCollector recycleUnusedBinlogFiles remove file...")

			if err = gc.option.cli.Remove(ctx, file); err != nil {
				log.Warn("garbageCollector recycleUnusedBinlogFiles remove file failed", zap.Error(err))
				unexpectedFailure.Inc()
				return struct{}{}, err
			}
			log.Info("garbageCollector recycleUnusedBinlogFiles remove file success")
			removed.Inc()
			return struct{}{}, nil
		})
		futures = append(futures, future)
		return true
	})
	// Wait for all remove tasks done.
	if err := conc.BlockOnAll(futures...); err != nil {
		// error is logged, and can be ignored here.
		logger.Warn("some task failure in remove object pool", zap.Error(err))
	}

	cost := time.Since(start)
	logger.Info("garbageCollector recycleUnusedBinlogFiles done",
		zap.Int("total", total),
		zap.Int("valid", valid),
		zap.Int("unexpectedFailure", int(unexpectedFailure.Load())),
		zap.Int("removed", int(removed.Load())),
		zap.String("lastFilePath", lastFilePath),
		zap.Duration("cost", cost),
		zap.Error(err))

	metrics.GarbageCollectorFileScanDuration.
		WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), label).
		Observe(float64(cost.Milliseconds()))
}

func (gc *garbageCollector) checkDroppedSegmentGC(segment *SegmentInfo,
	childSegment *SegmentInfo,
	indexSet typeutil.UniqueSet,
	cpTimestamp Timestamp,
) bool {
	log := log.With(zap.Int64("segmentID", segment.ID))

	if !gc.isExpire(segment.GetDroppedAt()) {
		return false
	}
	isCompacted := childSegment != nil || segment.GetCompacted()
	if isCompacted {
		// For compact A, B -> C, don't GC A or B if C is not indexed,
		// guarantee replacing A, B with C won't downgrade performance
		// If the child is GC'ed first, then childSegment will be nil.
		if childSegment != nil && !indexSet.Contain(childSegment.GetID()) {
			log.WithRateGroup("GC_FAIL_COMPACT_TO_NOT_INDEXED", 1, 60).
				RatedInfo(60, "skipping GC when compact target segment is not indexed",
					zap.Int64("child segment ID", childSegment.GetID()))
			return false
		}
	}

	segInsertChannel := segment.GetInsertChannel()
	// Ignore segments from potentially dropped collection. Check if collection is to be dropped by checking if channel is dropped.
	// We do this because collection meta drop relies on all segment being GCed.
	if gc.meta.catalog.ChannelExists(context.Background(), segInsertChannel) &&
		segment.GetDmlPosition().GetTimestamp() > cpTimestamp {
		// segment gc shall only happen when channel cp is after segment dml cp.
		log.WithRateGroup("GC_FAIL_CP_BEFORE", 1, 60).
			RatedInfo(60, "dropped segment dml position after channel cp, skip meta gc",
				zap.Uint64("dmlPosTs", segment.GetDmlPosition().GetTimestamp()),
				zap.Uint64("channelCpTs", cpTimestamp),
			)
		return false
	}
	return true
}

// recycleDroppedSegments scans all segments and remove those dropped segments from meta and oss.
func (gc *garbageCollector) recycleDroppedSegments(ctx context.Context) {
	start := time.Now()
	log := log.With(zap.String("gcName", "recycleDroppedSegments"), zap.Time("startAt", start))
	log.Info("start clear dropped segments...")
	defer func() { log.Info("clear dropped segments done", zap.Duration("timeCost", time.Since(start))) }()

	all := gc.meta.SelectSegments()
	drops := make(map[int64]*SegmentInfo, 0)
	compactTo := make(map[int64]*SegmentInfo)
	channels := typeutil.NewSet[string]()
	for _, segment := range all {
		cloned := segment.Clone()
		binlog.DecompressBinLogs(cloned.SegmentInfo)
		if cloned.GetState() == commonpb.SegmentState_Dropped {
			drops[cloned.GetID()] = cloned
			channels.Insert(cloned.GetInsertChannel())
			// continue
			// A(indexed), B(indexed) -> C(no indexed), D(no indexed) -> E(no indexed), A, B can not be GC
		}
		for _, from := range cloned.GetCompactionFrom() {
			compactTo[from] = cloned
		}
	}

	droppedCompactTo := make(map[*SegmentInfo]struct{})
	for id := range drops {
		if to, ok := compactTo[id]; ok {
			droppedCompactTo[to] = struct{}{}
		}
	}
	indexedSegments := FilterInIndexedSegments(gc.handler, gc.meta, lo.Keys(droppedCompactTo)...)
	indexedSet := make(typeutil.UniqueSet)
	for _, segment := range indexedSegments {
		indexedSet.Insert(segment.GetID())
	}

	channelCPs := make(map[string]uint64)
	for channel := range channels {
		pos := gc.meta.GetChannelCheckpoint(channel)
		channelCPs[channel] = pos.GetTimestamp()
	}

	log.Info("start to GC segments", zap.Int("drop_num", len(drops)))
	for segmentID, segment := range drops {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}

		log := log.With(zap.Int64("segmentID", segmentID))
		segInsertChannel := segment.GetInsertChannel()
		if !gc.checkDroppedSegmentGC(segment, compactTo[segment.GetID()], indexedSet, channelCPs[segInsertChannel]) {
			continue
		}

		logs := getLogs(segment)
		log.Info("GC segment start...", zap.Int("insert_logs", len(segment.GetBinlogs())),
			zap.Int("delta_logs", len(segment.GetDeltalogs())),
			zap.Int("stats_logs", len(segment.GetStatslogs())))
		if err := gc.removeObjectFiles(ctx, logs); err != nil {
			log.Warn("GC segment remove logs failed", zap.Error(err))
			continue
		}

		if err := gc.meta.DropSegment(segment.GetID()); err != nil {
			log.Warn("GC segment meta failed to drop segment", zap.Error(err))
			continue
		}
		log.Info("GC segment meta drop segment done")
	}
}

func (gc *garbageCollector) recycleChannelCPMeta(ctx context.Context) {
	channelCPs, err := gc.meta.catalog.ListChannelCheckpoint(ctx)
	if err != nil {
		log.Warn("list channel cp fail during GC", zap.Error(err))
		return
	}

	collectionID2GcStatus := make(map[int64]bool)
	skippedCnt := 0

	log.Info("start to GC channel cp", zap.Int("vchannelCPCnt", len(channelCPs)))
	for vChannel := range channelCPs {
		collectionID := funcutil.GetCollectionIDFromVChannel(vChannel)

		// !!! Skip to GC if vChannel format is illegal, it will lead meta leak in this case
		if collectionID == -1 {
			skippedCnt++
			log.Warn("parse collection id fail, skip to gc channel cp", zap.String("vchannel", vChannel))
			continue
		}

		_, ok := collectionID2GcStatus[collectionID]
		if !ok {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			has, err := gc.option.broker.HasCollection(ctx, collectionID)
			if err == nil && !has {
				collectionID2GcStatus[collectionID] = gc.meta.catalog.GcConfirm(ctx, collectionID, -1)
			} else {
				// skip checkpoints GC of this cycle if describe collection fails or the collection state is available.
				log.Debug("skip channel cp GC, the collection state is available",
					zap.Int64("collectionID", collectionID),
					zap.Bool("dropped", has), zap.Error(err))
				collectionID2GcStatus[collectionID] = false
			}
		}

		// Skip to GC if all segments meta of the corresponding collection are not removed
		if gcConfirmed, _ := collectionID2GcStatus[collectionID]; !gcConfirmed {
			skippedCnt++
			continue
		}

		err := gc.meta.DropChannelCheckpoint(vChannel)
		if err != nil {
			// Try to GC in the next gc cycle if drop channel cp meta fail.
			log.Warn("failed to drop channelcp check point during gc", zap.String("vchannel", vChannel), zap.Error(err))
		} else {
			log.Info("GC channel cp", zap.String("vchannel", vChannel))
		}
	}

	log.Info("GC channel cp done", zap.Int("skippedChannelCP", skippedCnt))
}

func (gc *garbageCollector) isExpire(dropts Timestamp) bool {
	droptime := time.Unix(0, int64(dropts))
	return time.Since(droptime) > gc.option.dropTolerance
}

func getLogs(sinfo *SegmentInfo) map[string]struct{} {
	logs := make(map[string]struct{})
	for _, flog := range sinfo.GetBinlogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	for _, flog := range sinfo.GetStatslogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	for _, flog := range sinfo.GetDeltalogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	return logs
}

// removeObjectFiles remove file from oss storage, return error if any log failed to remove.
func (gc *garbageCollector) removeObjectFiles(ctx context.Context, filePaths map[string]struct{}) error {
	futures := make([]*conc.Future[struct{}], 0)
	for filePath := range filePaths {
		filePath := filePath
		future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
			err := gc.option.cli.Remove(ctx, filePath)
			// ignore the error Key Not Found
			if err != nil {
				if !errors.Is(err, merr.ErrIoKeyNotFound) {
					return struct{}{}, err
				}
				log.Info("remove log failed, key not found, may be removed at previous GC, ignore the error",
					zap.String("path", filePath),
					zap.Error(err))
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	return conc.BlockOnAll(futures...)
}

// recycleUnusedIndexes is used to delete those indexes that is deleted by collection.
func (gc *garbageCollector) recycleUnusedIndexes(ctx context.Context) {
	start := time.Now()
	log := log.With(zap.String("gcName", "recycleUnusedIndexes"), zap.Time("startAt", start))
	log.Info("start recycleUnusedIndexes...")
	defer func() { log.Info("recycleUnusedIndexes done", zap.Duration("timeCost", time.Since(start))) }()

	deletedIndexes := gc.meta.indexMeta.GetDeletedIndexes()
	for _, index := range deletedIndexes {
		if ctx.Err() != nil {
			// process canceled.
			return
		}

		log := log.With(zap.Int64("collectionID", index.CollectionID), zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID))
		if err := gc.meta.indexMeta.RemoveIndex(index.CollectionID, index.IndexID); err != nil {
			log.Warn("remove index on collection fail", zap.Error(err))
			continue
		}
		log.Info("remove index on collection done")
	}
}

// recycleUnusedSegIndexes remove the index of segment if index is deleted or segment itself is deleted.
func (gc *garbageCollector) recycleUnusedSegIndexes(ctx context.Context) {
	start := time.Now()
	log := log.With(zap.String("gcName", "recycleUnusedSegIndexes"), zap.Time("startAt", start))
	log.Info("start recycleUnusedSegIndexes...")
	defer func() { log.Info("recycleUnusedSegIndexes done", zap.Duration("timeCost", time.Since(start))) }()

	segIndexes := gc.meta.indexMeta.GetAllSegIndexes()
	for _, segIdx := range segIndexes {
		if ctx.Err() != nil {
			// process canceled.
			return
		}

		// 1. segment belongs to is deleted.
		// 2. index is deleted.
		if gc.meta.GetSegment(segIdx.SegmentID) == nil || !gc.meta.indexMeta.IsIndexExist(segIdx.CollectionID, segIdx.IndexID) {
			indexFiles := gc.getAllIndexFilesOfIndex(segIdx)
			log := log.With(zap.Int64("collectionID", segIdx.CollectionID),
				zap.Int64("partitionID", segIdx.PartitionID),
				zap.Int64("segmentID", segIdx.SegmentID),
				zap.Int64("indexID", segIdx.IndexID),
				zap.Int64("buildID", segIdx.BuildID),
				zap.Int64("nodeID", segIdx.NodeID),
				zap.Int("indexFiles", len(indexFiles)))
			log.Info("GC Segment Index file start...")

			// Remove index files first.
			if err := gc.removeObjectFiles(ctx, indexFiles); err != nil {
				log.Warn("fail to remove index files for index", zap.Error(err))
				continue
			}

			// Remove meta from index meta.
			if err := gc.meta.indexMeta.RemoveSegmentIndex(segIdx.CollectionID, segIdx.PartitionID, segIdx.SegmentID, segIdx.IndexID, segIdx.BuildID); err != nil {
				log.Warn("delete index meta from etcd failed, wait to retry", zap.Error(err))
				continue
			}
			log.Info("index meta recycle success")
		}
	}
}

// recycleUnusedIndexFiles is used to delete those index files that no longer exist in the meta.
func (gc *garbageCollector) recycleUnusedIndexFiles(ctx context.Context) {
	start := time.Now()
	log := log.With(zap.String("gcName", "recycleUnusedIndexFiles"), zap.Time("startAt", start))
	log.Info("start recycleUnusedIndexFiles...")

	prefix := path.Join(gc.option.cli.RootPath(), common.SegmentIndexPath) + "/"
	// list dir first
	keyCount := 0
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, false, func(indexPathInfo *storage.ChunkObjectInfo) bool {
		key := indexPathInfo.FilePath
		keyCount++
		logger := log.With(zap.String("prefix", prefix), zap.String("key", key))

		buildID, err := parseBuildIDFromFilePath(key)
		if err != nil {
			logger.Warn("garbageCollector recycleUnusedIndexFiles parseIndexFileKey", zap.Error(err))
			return true
		}
		logger = logger.With(zap.Int64("buildID", buildID))
		logger.Info("garbageCollector will recycle index files")
		canRecycle, segIdx := gc.meta.indexMeta.CheckCleanSegmentIndex(buildID)
		if !canRecycle {
			// Even if the index is marked as deleted, the index file will not be recycled, wait for the next gc,
			// and delete all index files about the buildID at one time.
			logger.Info("garbageCollector can not recycle index files")
			return true
		}
		if segIdx == nil {
			// buildID no longer exists in meta, remove all index files
			logger.Info("garbageCollector recycleUnusedIndexFiles find meta has not exist, remove index files")
			err = gc.option.cli.RemoveWithPrefix(ctx, key)
			if err != nil {
				logger.Warn("garbageCollector recycleUnusedIndexFiles remove index files failed", zap.Error(err))
				return true
			}
			logger.Info("garbageCollector recycleUnusedIndexFiles remove index files success")
			return true
		}
		filesMap := gc.getAllIndexFilesOfIndex(segIdx)

		logger.Info("recycle index files", zap.Int("meta files num", len(filesMap)))
		deletedFilesNum := atomic.NewInt32(0)
		fileNum := 0

		futures := make([]*conc.Future[struct{}], 0)
		err = gc.option.cli.WalkWithPrefix(ctx, key, true, func(indexFile *storage.ChunkObjectInfo) bool {
			fileNum++
			file := indexFile.FilePath
			if _, ok := filesMap[file]; !ok {
				future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
					logger := logger.With(zap.String("file", file))
					logger.Info("garbageCollector recycleUnusedIndexFiles remove file...")

					if err := gc.option.cli.Remove(ctx, file); err != nil {
						logger.Warn("garbageCollector recycleUnusedIndexFiles remove file failed", zap.Error(err))
						return struct{}{}, err
					}
					deletedFilesNum.Inc()
					logger.Info("garbageCollector recycleUnusedIndexFiles remove file success")
					return struct{}{}, nil
				})
				futures = append(futures, future)
			}
			return true
		})
		// Wait for all remove tasks done.
		if err := conc.BlockOnAll(futures...); err != nil {
			// error is logged, and can be ignored here.
			logger.Warn("some task failure in remove object pool", zap.Error(err))
		}

		logger = logger.With(zap.Int("deleteIndexFilesNum", int(deletedFilesNum.Load())), zap.Int("walkFileNum", fileNum))
		if err != nil {
			logger.Warn("index files recycle failed when walk with prefix", zap.Error(err))
			return true
		}
		logger.Info("index files recycle done")
		return true
	})
	log = log.With(zap.Duration("timeCost", time.Since(start)), zap.Int("keyCount", keyCount), zap.Error(err))
	if err != nil {
		log.Warn("garbageCollector recycleUnusedIndexFiles failed", zap.Error(err))
		return
	}
	log.Info("recycleUnusedIndexFiles done")
}

// getAllIndexFilesOfIndex returns the all index files of index.
func (gc *garbageCollector) getAllIndexFilesOfIndex(segmentIndex *model.SegmentIndex) map[string]struct{} {
	filesMap := make(map[string]struct{})
	for _, fileID := range segmentIndex.IndexFileKeys {
		filepath := metautil.BuildSegmentIndexFilePath(gc.option.cli.RootPath(), segmentIndex.BuildID, segmentIndex.IndexVersion,
			segmentIndex.PartitionID, segmentIndex.SegmentID, fileID)
		filesMap[filepath] = struct{}{}
	}
	return filesMap
}

// recycleUnusedAnalyzeFiles is used to delete those analyze stats files that no longer exist in the meta.
func (gc *garbageCollector) recycleUnusedAnalyzeFiles(ctx context.Context) {
	log.Info("start recycleUnusedAnalyzeFiles")
	startTs := time.Now()
	prefix := path.Join(gc.option.cli.RootPath(), common.AnalyzeStatsPath) + "/"
	// list dir first
	keys := make([]string, 0)
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, false, func(chunkInfo *storage.ChunkObjectInfo) bool {
		keys = append(keys, chunkInfo.FilePath)
		return true
	})
	if err != nil {
		log.Warn("garbageCollector recycleUnusedAnalyzeFiles list keys from chunk manager failed", zap.Error(err))
		return
	}
	log.Info("recycleUnusedAnalyzeFiles, finish list object", zap.Duration("time spent", time.Since(startTs)), zap.Int("task ids", len(keys)))
	for _, key := range keys {
		if ctx.Err() != nil {
			// process canceled
			return
		}

		log.Debug("analyze keys", zap.String("key", key))
		taskID, err := parseBuildIDFromFilePath(key)
		if err != nil {
			log.Warn("garbageCollector recycleUnusedAnalyzeFiles parseAnalyzeResult failed", zap.String("key", key), zap.Error(err))
			continue
		}
		log.Info("garbageCollector will recycle analyze stats files", zap.Int64("taskID", taskID))
		canRecycle, task := gc.meta.analyzeMeta.CheckCleanAnalyzeTask(taskID)
		if !canRecycle {
			// Even if the analysis task is marked as deleted, the analysis stats file will not be recycled, wait for the next gc,
			// and delete all index files about the taskID at one time.
			log.Info("garbageCollector no need to recycle analyze stats files", zap.Int64("taskID", taskID))
			continue
		}
		if task == nil {
			// taskID no longer exists in meta, remove all analysis files
			log.Info("garbageCollector recycleUnusedAnalyzeFiles find meta has not exist, remove index files",
				zap.Int64("taskID", taskID))
			err = gc.option.cli.RemoveWithPrefix(ctx, key)
			if err != nil {
				log.Warn("garbageCollector recycleUnusedAnalyzeFiles remove analyze stats files failed",
					zap.Int64("taskID", taskID), zap.String("prefix", key), zap.Error(err))
				continue
			}
			log.Info("garbageCollector recycleUnusedAnalyzeFiles remove analyze stats files success",
				zap.Int64("taskID", taskID), zap.String("prefix", key))
			continue
		}

		log.Info("remove analyze stats files which version is less than current task",
			zap.Int64("taskID", taskID), zap.Int64("current version", task.Version))
		var i int64
		for i = 0; i < task.Version; i++ {
			if ctx.Err() != nil {
				// process canceled.
				return
			}
			removePrefix := prefix + fmt.Sprintf("%d/", task.Version)
			if err := gc.option.cli.RemoveWithPrefix(ctx, removePrefix); err != nil {
				log.Warn("garbageCollector recycleUnusedAnalyzeFiles remove files with prefix failed",
					zap.Int64("taskID", taskID), zap.String("removePrefix", removePrefix))
				continue
			}
		}
		log.Info("analyze stats files recycle success", zap.Int64("taskID", taskID))
	}
}
