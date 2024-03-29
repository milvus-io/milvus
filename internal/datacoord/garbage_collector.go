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
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
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

	removeLogPool *conc.Pool[struct{}]
}

// garbageCollector handles garbage files in object storage
// which could be dropped collection remanent or data node failure traces
type garbageCollector struct {
	option  GcOption
	meta    *meta
	handler Handler

	startOnce  sync.Once
	stopOnce   sync.Once
	wg         sync.WaitGroup
	closeCh    chan struct{}
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
	opt.removeLogPool = conc.NewPool[struct{}](Params.DataCoordCfg.GCRemoveConcurrent.GetAsInt(), conc.WithExpiryDuration(time.Minute))
	return &garbageCollector{
		meta:    meta,
		handler: handler,
		option:  opt,
		closeCh: make(chan struct{}),
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
			gc.wg.Add(1)
			go gc.work()
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
func (gc *garbageCollector) work() {
	defer gc.wg.Done()

	// TODO: fast cancel for gc when closing.
	// Run gc tasks in parallel.
	gc.wg.Add(1)
	go gc.runRecycleTaskWithPauser("meta", gc.option.checkInterval, func() {
		gc.clearEtcd()
		gc.recycleUnusedIndexes()
		gc.recycleUnusedSegIndexes()
	})

	gc.wg.Add(1)
	go gc.runRecycleTaskWithPauser("file", gc.option.scanInterval, func() {
		gc.recycleUnusedBinlogFiles()
		gc.recycleUnusedIndexFiles()
	})

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
		case <-gc.closeCh:
			log.Warn("garbage collector quit")
			return
		}
	}
}

// runRecycleTaskWithPauser is a helper function to create a task with pauser
func (gc *garbageCollector) runRecycleTaskWithPauser(name string, interval time.Duration, task func()) {
	logger := log.With(zap.String("gcName", name)).With(zap.Duration("interval", interval))
	timer := time.NewTimer(interval)
	defer func() {
		logger.Info("garbage collector recycle task quit")
		timer.Stop()
		gc.wg.Done()
	}()

	for {
		select {
		case <-gc.closeCh:
			return
		case <-timer.C:
			if time.Now().Before(gc.pauseUntil.Load()) {
				logger.Info("garbage collector paused", zap.Time("until", gc.pauseUntil.Load()))
				continue
			}
			logger.Info("garbage collector recycle task start")
			started := time.Now()
			task()
			logger.Info("garbage collector recycle task done", zap.Duration("cost", time.Since(started)))
		}
	}
}

func (gc *garbageCollector) close() {
	gc.stopOnce.Do(func() {
		close(gc.closeCh)
		gc.wg.Wait()
	})
}

// recycleUnusedBinlogFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) recycleUnusedBinlogFiles() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	wg := &sync.WaitGroup{}
	start := time.Now()

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
		wg.Add(1)
		gc.option.removeLogPool.Submit(func() (nop struct{}, err error) {
			logger.Info("garbageCollector recycleUnusedBinlogFiles remove file", zap.String("file", file))
			defer func() {
				logger.Info("garbageCollector recycleUnusedBinlogFiles remove file done", zap.String("file", file), zap.Error(err))
				if err != nil {
					unexpectedFailure.Inc()
				} else {
					removed.Inc()
				}
				wg.Done()
			}()
			err = gc.option.cli.Remove(ctx, file)
			return struct{}{}, err
		})
		return true
	})
	wg.Wait()
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
	} else {
		if !gc.isExpire(segment.GetDroppedAt()) {
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

func (gc *garbageCollector) clearEtcd() {
	all := gc.meta.SelectSegments(func(si *SegmentInfo) bool { return true })
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

	dropIDs := lo.Keys(drops)
	sort.Slice(dropIDs, func(i, j int) bool {
		return dropIDs[i] < dropIDs[j]
	})

	log.Info("start to GC segments", zap.Int("drop_num", len(dropIDs)))
	for _, segmentID := range dropIDs {
		segment, ok := drops[segmentID]
		if !ok {
			log.Warn("segmentID is not in drops", zap.Int64("segmentID", segmentID))
			continue
		}

		segInsertChannel := segment.GetInsertChannel()
		if !gc.checkDroppedSegmentGC(segment, compactTo[segment.GetID()], indexedSet, channelCPs[segInsertChannel]) {
			continue
		}

		logs := getLogs(segment)
		log.Info("GC segment", zap.Int64("segmentID", segment.GetID()),
			zap.Int("insert_logs", len(segment.GetBinlogs())),
			zap.Int("delta_logs", len(segment.GetDeltalogs())),
			zap.Int("stats_logs", len(segment.GetStatslogs())))
		if gc.removeLogs(logs) {
			err := gc.meta.DropSegment(segment.GetID())
			if err != nil {
				log.Info("GC segment meta failed to drop segment", zap.Int64("segment id", segment.GetID()), zap.Error(err))
			} else {
				log.Info("GC segment meta drop semgent", zap.Int64("segment id", segment.GetID()))
			}
		}
		if segList := gc.meta.GetSegmentsByChannel(segInsertChannel); len(segList) == 0 &&
			!gc.meta.catalog.ChannelExists(context.Background(), segInsertChannel) {
			log.Info("empty channel found during gc, manually cleanup channel checkpoints", zap.String("vChannel", segInsertChannel))
			if err := gc.meta.DropChannelCheckpoint(segInsertChannel); err != nil {
				log.Info("failed to drop channel check point during segment garbage collection", zap.String("vchannel", segInsertChannel), zap.Error(err))
			}
		}
	}
}

func (gc *garbageCollector) isExpire(dropts Timestamp) bool {
	droptime := time.Unix(0, int64(dropts))
	return time.Since(droptime) > gc.option.dropTolerance
}

func getLogs(sinfo *SegmentInfo) []*datapb.Binlog {
	var logs []*datapb.Binlog
	for _, flog := range sinfo.GetBinlogs() {
		logs = append(logs, flog.GetBinlogs()...)
	}

	for _, flog := range sinfo.GetStatslogs() {
		logs = append(logs, flog.GetBinlogs()...)
	}

	for _, flog := range sinfo.GetDeltalogs() {
		logs = append(logs, flog.GetBinlogs()...)
	}
	return logs
}

func (gc *garbageCollector) removeLogs(logs []*datapb.Binlog) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var w sync.WaitGroup
	w.Add(len(logs))
	for _, l := range logs {
		tmpLog := l
		gc.option.removeLogPool.Submit(func() (struct{}, error) {
			defer w.Done()
			select {
			case <-ctx.Done():
				return struct{}{}, nil
			default:
				err := gc.option.cli.Remove(ctx, tmpLog.GetLogPath())
				if err != nil {
					switch err.(type) {
					case minio.ErrorResponse:
						errResp := minio.ToErrorResponse(err)
						if errResp.Code != "" && errResp.Code != "NoSuchKey" {
							cancel()
						}
					default:
						cancel()
					}
				}
				return struct{}{}, nil
			}
		})
	}
	w.Wait()
	select {
	case <-ctx.Done():
		return false
	default:
		return true
	}
}

func (gc *garbageCollector) recycleUnusedIndexes() {
	log.Info("start recycleUnusedIndexes")
	deletedIndexes := gc.meta.indexMeta.GetDeletedIndexes()
	for _, index := range deletedIndexes {
		if err := gc.meta.indexMeta.RemoveIndex(index.CollectionID, index.IndexID); err != nil {
			log.Warn("remove index on collection fail", zap.Int64("collectionID", index.CollectionID),
				zap.Int64("indexID", index.IndexID), zap.Error(err))
			continue
		}
	}
}

func (gc *garbageCollector) recycleUnusedSegIndexes() {
	segIndexes := gc.meta.indexMeta.GetAllSegIndexes()
	for _, segIdx := range segIndexes {
		if gc.meta.GetSegment(segIdx.SegmentID) == nil || !gc.meta.indexMeta.IsIndexExist(segIdx.CollectionID, segIdx.IndexID) {
			if err := gc.meta.indexMeta.RemoveSegmentIndex(segIdx.CollectionID, segIdx.PartitionID, segIdx.SegmentID, segIdx.IndexID, segIdx.BuildID); err != nil {
				log.Warn("delete index meta from etcd failed, wait to retry", zap.Int64("buildID", segIdx.BuildID),
					zap.Int64("segmentID", segIdx.SegmentID), zap.Int64("nodeID", segIdx.NodeID), zap.Error(err))
				continue
			}
			log.Info("index meta recycle success", zap.Int64("buildID", segIdx.BuildID),
				zap.Int64("segmentID", segIdx.SegmentID))
		}
	}
}

// recycleUnusedIndexFiles is used to delete those index files that no longer exist in the meta.
func (gc *garbageCollector) recycleUnusedIndexFiles() {
	log.Info("start recycleUnusedIndexFiles")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startTs := time.Now()
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
		canRecycle, segIdx := gc.meta.indexMeta.CleanSegmentIndex(buildID)
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
		filesMap := make(map[string]struct{})
		for _, fileID := range segIdx.IndexFileKeys {
			filepath := metautil.BuildSegmentIndexFilePath(gc.option.cli.RootPath(), segIdx.BuildID, segIdx.IndexVersion,
				segIdx.PartitionID, segIdx.SegmentID, fileID)
			filesMap[filepath] = struct{}{}
		}
		logger.Info("recycle index files", zap.Int("meta files num", len(filesMap)))
		deletedFilesNum := atomic.NewInt32(0)
		fileNum := 0

		wg := &sync.WaitGroup{}
		if err := gc.option.cli.WalkWithPrefix(ctx, key, true, func(indexFile *storage.ChunkObjectInfo) bool {
			fileNum++
			file := indexFile.FilePath
			if _, ok := filesMap[file]; !ok {
				wg.Add(1)
				gc.option.removeLogPool.Submit(func() (nop struct{}, err error) {
					logger.Info("garbageCollector recycleUnusedIndexFiles remove file", zap.String("file", file))
					defer func() {
						logger.Info("garbageCollector recycleUnusedIndexFiles remove file done", zap.String("file", file), zap.Error(err))
						if err == nil {
							deletedFilesNum.Inc()
						}
						wg.Done()
					}()
					err = gc.option.cli.Remove(ctx, file)
					return struct{}{}, err
				})
			}
			return true
		}); err != nil {
			logger.Warn("garbageCollector recycleUnusedIndexFiles list files failed", zap.Error(err))
		}
		wg.Wait()
		logger.Info("index files recycle success", zap.Int("delete index files num", int(deletedFilesNum.Load())), zap.Int("chunkManager files num", fileNum))
		return true
	})
	log.Info("recycleUnusedIndexFiles, finish list object", zap.Duration("time spent", time.Since(startTs)), zap.Int("build ids", keyCount), zap.Error(err))
}
