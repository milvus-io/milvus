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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	//TODO silverxia change to configuration
	insertLogPrefix = `insert_log`
	statsLogPrefix  = `stats_log`
	deltaLogPrefix  = `delta_log`
)

// GcOption garbage collection options
type GcOption struct {
	cli              storage.ChunkManager // client
	enabled          bool                 // enable switch
	checkInterval    time.Duration        // each interval
	missingTolerance time.Duration        // key missing in meta tolerance time
	dropTolerance    time.Duration        // dropped segment related key tolerance time
}

// garbageCollector handles garbage files in object storage
// which could be dropped collection remanent or data node failure traces
type garbageCollector struct {
	option     GcOption
	meta       *meta
	handler    Handler
	segRefer   *SegmentReferenceManager
	indexCoord types.IndexCoord

	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup

	pauseUntil atomic.Time

	closeCh chan struct{}
	cmdCh   chan gcCmd
}

type gcCmd struct {
	cmdType  datapb.GcCommand
	duration time.Duration
	done     chan struct{}
}

// newGarbageCollector create garbage collector with meta and option
func newGarbageCollector(meta *meta, handler Handler, segRefer *SegmentReferenceManager, indexCoord types.IndexCoord, opt GcOption) *garbageCollector {
	log.Info("GC with option", zap.Bool("enabled", opt.enabled), zap.Duration("interval", opt.checkInterval),
		zap.Duration("missingTolerance", opt.missingTolerance), zap.Duration("dropTolerance", opt.dropTolerance))
	return &garbageCollector{
		meta:       meta,
		handler:    handler,
		segRefer:   segRefer,
		indexCoord: indexCoord,
		option:     opt,
		closeCh:    make(chan struct{}),
		cmdCh:      make(chan gcCmd),
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

// work contains actual looping check logic
func (gc *garbageCollector) work() {
	defer gc.wg.Done()
	ticker := time.Tick(gc.option.checkInterval)
	for {
		select {
		case <-ticker:
			if time.Now().Before(gc.pauseUntil.Load()) {
				log.Info("garbage collector paused", zap.Time("until", gc.pauseUntil.Load()))
				continue
			}
			gc.clearEtcd()
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
			}
			close(cmd.done)
		case <-gc.closeCh:
			log.Warn("garbage collector quit")
			return
		}
	}
}

func (gc *garbageCollector) close() {
	gc.stopOnce.Do(func() {
		close(gc.closeCh)
		gc.wg.Wait()
	})
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
		return errors.New("garbage collection not enabled, cannot resume")
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

// scan load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) scan() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	prefixes = append(prefixes, path.Join(gc.option.cli.RootPath(), insertLogPrefix))
	prefixes = append(prefixes, path.Join(gc.option.cli.RootPath(), statsLogPrefix))
	prefixes = append(prefixes, path.Join(gc.option.cli.RootPath(), deltaLogPrefix))
	var removedKeys []string

	for _, prefix := range prefixes {
		startTs := time.Now()
		infoKeys, modTimes, err := gc.option.cli.ListWithPrefix(ctx, prefix, true)
		if err != nil {
			log.Error("failed to list files with prefix",
				zap.String("prefix", prefix),
				zap.String("error", err.Error()),
			)
		}
		segmentMap, filesMap := getMetaMap()
		log.Info("gc scan finish list object", zap.String("prefix", prefix), zap.Duration("time spent", time.Since(startTs)), zap.Int("keys", len(infoKeys)))
		for i, infoKey := range infoKeys {
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

			if gc.segRefer.HasSegmentLock(segmentID) {
				valid++
				continue
			}

			if strings.Contains(prefix, statsLogPrefix) &&
				segmentMap.Contain(segmentID) {
				valid++
				continue
			}

			// not found in meta, check last modified time exceeds tolerance duration
			if time.Since(modTimes[i]) > gc.option.missingTolerance {
				// ignore error since it could be cleaned up next time
				removedKeys = append(removedKeys, infoKey)
				err = gc.option.cli.Remove(ctx, infoKey)
				if err != nil {
					missing++
					log.Error("failed to remove object",
						zap.String("infoKey", infoKey),
						zap.Error(err))
				}
			}
		}
	}
	log.Info("scan file to do garbage collection",
		zap.Int("total", total),
		zap.Int("valid", valid),
		zap.Int("missing", missing),
		zap.Strings("removedKeys", removedKeys))
}

func (gc *garbageCollector) clearEtcd() {
	all := gc.meta.SelectSegments(func(si *SegmentInfo) bool { return true })
	drops := make(map[int64]*SegmentInfo, 0)
	compactTo := make(map[int64]*SegmentInfo)
	channels := typeutil.NewSet[string]()
	for _, segment := range all {
		if segment.GetState() == commonpb.SegmentState_Dropped && !gc.segRefer.HasSegmentLock(segment.ID) {
			drops[segment.GetID()] = segment
			channels.Insert(segment.GetInsertChannel())
			//continue
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
	indexedSegments, err := FilterInIndexedSegments(gc.handler, gc.indexCoord, lo.Keys(droppedCompactTo)...)
	if err != nil {
		log.Warn("failed to get indexed segments doing garbage collection", zap.Error(err))
		return
	}
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

	for _, segmentID := range dropIDs {
		segment, ok := drops[segmentID]
		if !ok {
			log.Warn("segmentID is not in drops", zap.Int64("segmentID", segmentID))
			// can't to here
			continue
		}
		log := log.With(zap.Int64("segmentID", segment.ID))
		to, isCompacted := compactTo[segment.GetID()]
		// for compacted segment, try to clean up the files as long as target segment is there
		if !isCompacted && !gc.isExpire(segment.GetDroppedAt()) {
			continue
		}
		segInsertChannel := segment.GetInsertChannel()
		// Ignore segments from potentially dropped collection. Check if collection is to be dropped by checking if channel is dropped.
		// We do this because collection meta drop relies on all segment being GCed.
		if gc.meta.catalog.ChannelExists(context.Background(), segInsertChannel) &&
			segment.GetDmlPosition().GetTimestamp() > channelCPs[segInsertChannel] {
			// segment gc shall only happen when channel cp is after segment dml cp.
			log.WithRateGroup("GC_FAIL_CP_BEFORE", 1, 60).
				RatedInfo(60, "dropped segment dml position after channel cp, skip meta gc",
					zap.Uint64("dmlPosTs", segment.GetDmlPosition().GetTimestamp()),
					zap.Uint64("channelCpTs", channelCPs[segInsertChannel]),
				)
			continue
		}
		// For compact A, B -> C, don't GC A or B if C is not indexed,
		// guarantee replacing A, B with C won't downgrade performance
		if isCompacted && !indexedSet.Contain(to.GetID()) {
			log.WithRateGroup("GC_FAIL_COMPACT_TO_NOT_INDEXED", 1, 60).
				RatedWarn(60, "skipping GC when compact target segment is not indexed",
					zap.Int64("segmentID", to.GetID()))
			continue
		}
		logs := getLogs(segment)
		log.Info("GC segment", zap.Int64("segmentID", segment.GetID()))
		if gc.removeLogs(logs) {
			err := gc.meta.DropSegment(segment.GetID())
			log.Warn("failed to drop segment", zap.Int64("segment id", segment.GetID()), zap.Error(err))
		}
		if segList := gc.meta.GetSegmentsByChannel(segInsertChannel); len(segList) == 0 &&
			!gc.meta.catalog.ChannelExists(context.Background(), segInsertChannel) {
			log.Info("empty channel found during gc, manually cleanup channel checkpoints", zap.String("vChannel", segInsertChannel))

			if err := gc.meta.DropChannelCheckpoint(segInsertChannel); err != nil {
				log.Warn("failed to drop channel check point during segment garbage collection",
					zap.Error(err))
				// Fail-open as there's nothing to do.
				log.Warn("failed to drop channel check point during segment garbage collection", zap.String("vchannel", segInsertChannel), zap.Error(err))
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
	delFlag := true
	for _, l := range logs {
		err := gc.option.cli.Remove(ctx, l.GetLogPath())
		if err != nil {
			switch err.(type) {
			case minio.ErrorResponse:
				errResp := minio.ToErrorResponse(err)
				if errResp.Code != "" && errResp.Code != "NoSuchKey" {
					delFlag = false
				}
			default:
				delFlag = false
			}
		}
	}
	return delFlag
}
