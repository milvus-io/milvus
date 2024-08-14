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
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
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

	removeLogPool                 *conc.Pool[struct{}]
	useCollectionIdBasedIndexPath bool
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
	ctx, cancel := context.WithCancel(context.Background())
	opt.useCollectionIdBasedIndexPath = Params.CommonCfg.UseCollectionIdBasedIndexPath.GetAsBool()
	log.Info("GC with option",
		zap.Bool("enabled", opt.enabled),
		zap.Duration("interval", opt.checkInterval),
		zap.Duration("scanInterval", opt.scanInterval),
		zap.Duration("missingTolerance", opt.missingTolerance),
		zap.Duration("dropTolerance", opt.dropTolerance),
		zap.Bool("useCollectionIdBasedIndexPath", opt.useCollectionIdBasedIndexPath))
	opt.removeLogPool = conc.NewPool[struct{}](Params.DataCoordCfg.GCRemoveConcurrent.GetAsInt(), conc.WithExpiryDuration(time.Minute))
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
	case <-gc.ctx.Done():
		return errors.New("garbage collector already closed")
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
	case <-gc.ctx.Done():
		return errors.New("garbage collector already closed")
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
	ticker := time.NewTicker(gc.option.checkInterval)
	defer ticker.Stop()
	scanTicker := time.NewTicker(gc.option.scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if time.Now().Before(gc.pauseUntil.Load()) {
				log.Info("garbage collector paused", zap.Time("until", gc.pauseUntil.Load()))
				continue
			}
			gc.clearEtcd()
			gc.recycleUnusedIndexes()
			gc.recycleUnusedSegIndexes()
			gc.recycleUnusedIndexFiles()
		case <-scanTicker.C:
			log.Info("Garbage collector start to scan interrupted write residue")
			gc.scan()
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
			log.Warn("garbage collector quit")
			return
		}
	}
}

func (gc *garbageCollector) close() {
	gc.stopOnce.Do(func() {
		gc.cancel()
		gc.wg.Wait()
	})
}

// scan load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) scan() {
	var (
		total   = 0
		valid   = 0
		missing = 0
	)
	getMetaMap := func() (typeutil.UniqueSet, typeutil.Set[string]) {
		segmentMap := typeutil.NewUniqueSet()
		filesMap := typeutil.NewSet[string]()
		segments := gc.meta.GetAllSegmentsUnsafe()
		for _, segment := range segments {
			segmentMap.Insert(segment.GetID())
			for _, log := range getLogs(segment) {
				filesMap.Insert(log.GetLogPath())
			}
		}
		return segmentMap, filesMap
	}

	// walk only data cluster related prefixes
	prefixes := make([]string, 0, 3)
	prefixes = append(prefixes, path.Join(gc.option.cli.RootPath(), common.SegmentInsertLogPath))
	prefixes = append(prefixes, path.Join(gc.option.cli.RootPath(), common.SegmentStatslogPath))
	prefixes = append(prefixes, path.Join(gc.option.cli.RootPath(), common.SegmentDeltaLogPath))
	labels := []string{metrics.InsertFileLabel, metrics.StatFileLabel, metrics.DeleteFileLabel}
	var removedKeys []string

	for idx, prefix := range prefixes {
		if gc.ctx.Err() != nil {
			// garbage collector stopped
			return
		}
		startTs := time.Now()
		infoKeys, modTimes, err := gc.option.cli.ListWithPrefix(gc.ctx, prefix, true)
		if err != nil {
			log.Error("failed to list files with prefix",
				zap.String("prefix", prefix),
				zap.Error(err),
			)
			continue
		}
		cost := time.Since(startTs)
		segmentMap, filesMap := getMetaMap()
		metrics.GarbageCollectorListLatency.
			WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), labels[idx]).
			Observe(float64(cost.Milliseconds()))
		log.Info("gc scan finish list object", zap.String("prefix", prefix), zap.Duration("time spent", cost), zap.Int("keys", len(infoKeys)))
		for i, infoKey := range infoKeys {
			if gc.ctx.Err() != nil {
				// garbage collector stopped
				return
			}

			total++
			_, has := filesMap[infoKey]
			if has {
				valid++
				continue
			}

			segmentID, err := storage.ParseSegmentIDByBinlog(gc.option.cli.RootPath(), infoKey)
			if err != nil {
				missing++
				log.Warn("parse segment id error",
					zap.String("infoKey", infoKey),
					zap.Error(err))
				continue
			}

			if strings.Contains(prefix, common.SegmentInsertLogPath) &&
				segmentMap.Contain(segmentID) {
				valid++
				continue
			}

			// not found in meta, check last modified time exceeds tolerance duration
			if time.Since(modTimes[i]) > gc.option.missingTolerance {
				// ignore error since it could be cleaned up next time
				removedKeys = append(removedKeys, infoKey)
				err = gc.option.cli.Remove(gc.ctx, infoKey)
				if err != nil {
					missing++
					log.Error("failed to remove object",
						zap.String("infoKey", infoKey),
						zap.Error(err))
				}
			}
		}
	}
	metrics.GarbageCollectorRunCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Add(1)
	log.Info("scan file to do garbage collection",
		zap.Int("total", total),
		zap.Int("valid", valid),
		zap.Int("missing", missing),
		zap.Strings("removedKeys", removedKeys))
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
		if segment.GetState() == commonpb.SegmentState_Dropped {
			drops[segment.GetID()] = segment
			channels.Insert(segment.GetInsertChannel())
			// continue
			// A(indexed), B(indexed) -> C(no indexed), D(no indexed) -> E(no indexed), A, B can not be GC
		}
		for _, from := range segment.GetCompactionFrom() {
			compactTo[from] = segment
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
		if gc.ctx.Err() != nil {
			// process stopped
			return
		}

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
			!gc.meta.catalog.ChannelExists(gc.ctx, segInsertChannel) {
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
	ctx, cancel := context.WithCancel(gc.ctx)
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

func (gc *garbageCollector) getBuildIdKeys(ctx context.Context, prefix string) ([]string, error) {
	keys, _, err := gc.option.cli.ListWithPrefix(ctx, prefix, false)
	if err != nil {
		log.Warn("garbageCollector recycleUnusedIndexFiles list keys from chunk manager failed", zap.Error(err))
		return nil, err
	}

	/*
		this means keys has paths till collection id, we need to go one more level below
	*/
	if gc.option.useCollectionIdBasedIndexPath {
		totalBuildIdKeys := make([]string, 0)
		for _, key := range keys {
			log.Debug("listing path for garbage collection", zap.String("prefix", prefix))
			buildIdKeysForCollection, _, errWhileCollectionIndexList := gc.option.cli.ListWithPrefix(ctx, key, false)
			if errWhileCollectionIndexList != nil {
				log.Warn("garbageCollector recycleUnusedIndexFiles list keys from chunk manager failed", zap.Error(errWhileCollectionIndexList))
				return totalBuildIdKeys, errWhileCollectionIndexList
			}
			totalBuildIdKeys = append(totalBuildIdKeys, buildIdKeysForCollection...)
		}
		return totalBuildIdKeys, err
	} else {
		return keys, err
	}
}

// recycleUnusedIndexFiles is used to delete those index files that no longer exist in the meta.
func (gc *garbageCollector) recycleUnusedIndexFiles() {
	log.Info("start recycleUnusedIndexFiles")
	startTs := time.Now()
	prefix := path.Join(gc.option.cli.RootPath(), common.SegmentIndexPath) + "/"
	// list dir first
	keys, err := gc.getBuildIdKeys(gc.ctx, prefix)
	if err != nil {
		log.Warn("garbageCollector recycleUnusedIndexFiles list keys from chunk manager failed", zap.Error(err))
		return
	}
	log.Info("recycleUnusedIndexFiles, finish list object", zap.Duration("time spent", time.Since(startTs)), zap.Int("build ids", len(keys)))
	for _, key := range keys {
		if gc.ctx.Err() != nil {
			// garbage collector stopped
			return
		}
		log.Debug("indexFiles keys", zap.String("key", key))
		buildID, err := parseBuildIDFromFilePath(key)
		if err != nil {
			log.Warn("garbageCollector recycleUnusedIndexFiles parseIndexFileKey", zap.String("key", key), zap.Error(err))
			continue
		}
		log.Info("garbageCollector will recycle index files", zap.Int64("buildID", buildID))
		canRecycle, segIdx := gc.meta.indexMeta.CleanSegmentIndex(buildID)
		if !canRecycle {
			// Even if the index is marked as deleted, the index file will not be recycled, wait for the next gc,
			// and delete all index files about the buildID at one time.
			log.Info("garbageCollector can not recycle index files", zap.Int64("buildID", buildID))
			continue
		}
		if segIdx == nil {
			// buildID no longer exists in meta, remove all index files
			log.Info("garbageCollector recycleUnusedIndexFiles find meta has not exist, remove index files",
				zap.Int64("buildID", buildID))
			err = gc.option.cli.RemoveWithPrefix(gc.ctx, key)
			if err != nil {
				log.Warn("garbageCollector recycleUnusedIndexFiles remove index files failed",
					zap.Int64("buildID", buildID), zap.String("prefix", key), zap.Error(err))
				continue
			}
			log.Info("garbageCollector recycleUnusedIndexFiles remove index files success",
				zap.Int64("buildID", buildID), zap.String("prefix", key))
			continue
		}
		filesMap := make(map[string]struct{})
		for _, fileID := range segIdx.IndexFileKeys {
			var filepath string
			if gc.option.useCollectionIdBasedIndexPath {
				filepath = metautil.BuildSegmentIndexFilePathWithCollectionID(gc.option.cli.RootPath(), segIdx.CollectionID, segIdx.BuildID, segIdx.IndexVersion, segIdx.PartitionID, segIdx.SegmentID, fileID)
			} else {
				filepath = metautil.BuildSegmentIndexFilePath(gc.option.cli.RootPath(), segIdx.BuildID, segIdx.IndexVersion, segIdx.PartitionID, segIdx.SegmentID, fileID)
			}
			filesMap[filepath] = struct{}{}
		}
		files, _, err := gc.option.cli.ListWithPrefix(gc.ctx, key, true)
		if err != nil {
			log.Warn("garbageCollector recycleUnusedIndexFiles list files failed",
				zap.Int64("buildID", buildID), zap.String("prefix", key), zap.Error(err))
			continue
		}
		log.Info("recycle index files", zap.Int64("buildID", buildID), zap.Int("meta files num", len(filesMap)),
			zap.Int("chunkManager files num", len(files)))
		deletedFilesNum := 0
		for _, file := range files {
			if gc.ctx.Err() != nil {
				// garbage collector stopped
				return
			}
			if _, ok := filesMap[file]; !ok {
				log.Debug("Removing file ", zap.String("file", file))
				if err = gc.option.cli.Remove(gc.ctx, file); err != nil {
					log.Warn("garbageCollector recycleUnusedIndexFiles remove file failed",
						zap.Int64("buildID", buildID), zap.String("file", file), zap.Error(err))
					continue
				}
				deletedFilesNum++
			}
		}
		log.Info("index files recycle success", zap.Int64("buildID", buildID),
			zap.Int("delete index files num", deletedFilesNum))
	}
}
