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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

	startOnce        sync.Once
	stopOnce         sync.Once
	wg               sync.WaitGroup
	cmdCh            chan gcCmd
	pauseUntil       *gcPauseRecords
	pausedCollection *typeutil.ConcurrentMap[int64, *gcPauseRecords]
	controlChannels  map[string]chan gcCmd

	systemMetricsListener *hardware.SystemMetricsListener
}

type gcCmd struct {
	cmdType      datapb.GcCommand
	duration     time.Duration
	collectionID int64
	ticket       string
	done         chan error
	timeout      <-chan struct{}
}

type gcPauseRecord struct {
	ticket     string
	pauseUntil time.Time
}

type gcPauseRecords struct {
	mut     sync.RWMutex
	maxLen  int
	records typeutil.Heap[gcPauseRecord]
}

func (gc *gcPauseRecords) PauseUntil() time.Time {
	// nil protection
	if gc == nil {
		return time.Time{}
	}
	gc.mut.RLock()
	defer gc.mut.RUnlock()
	// no pause records, return zero value
	if gc.records.Len() == 0 {
		return time.Time{}
	}

	return gc.records.Peek().pauseUntil
}

func (gc *gcPauseRecords) Insert(ticket string, pauseUntil time.Time) error {
	gc.mut.Lock()
	defer gc.mut.Unlock()

	// heap small enough, short path
	if gc.records.Len() < gc.maxLen {
		gc.records.Push(gcPauseRecord{
			ticket:     ticket,
			pauseUntil: pauseUntil,
		})
		return nil
	}

	records := make([]gcPauseRecord, 0, gc.records.Len())
	now := time.Now()
	for gc.records.Len() > 0 {
		record := gc.records.Pop()
		if record.pauseUntil.After(now) {
			records = append(records, record)
		}
	}
	gc.records = typeutil.NewObjectArrayBasedMaximumHeap(records, func(r gcPauseRecord) int64 {
		return r.pauseUntil.UnixNano()
	})

	if gc.records.Len() < gc.maxLen {
		gc.records.Push(gcPauseRecord{
			ticket:     ticket,
			pauseUntil: pauseUntil,
		})
		return nil
	}

	// too many pause records, refresh heap
	return merr.WrapErrTooManyRequests(64, "too many pause records")
}

func (gc *gcPauseRecords) Delete(ticket string) {
	gc.mut.Lock()
	defer gc.mut.Unlock()
	now := time.Now()
	records := make([]gcPauseRecord, 0, gc.records.Len())
	for gc.records.Len() > 0 {
		record := gc.records.Pop()
		if now.Before(record.pauseUntil) && record.ticket != ticket {
			records = append(records, record)
		}
	}
	gc.records = typeutil.NewObjectArrayBasedMaximumHeap(records, func(r gcPauseRecord) int64 {
		return r.pauseUntil.UnixNano()
	})
}

func (gc *gcPauseRecords) Len() int {
	gc.mut.RLock()
	defer gc.mut.RUnlock()
	return gc.records.Len()
}

func NewGCPauseRecords() *gcPauseRecords {
	return &gcPauseRecords{
		records: typeutil.NewObjectArrayBasedMaximumHeap[gcPauseRecord, int64]([]gcPauseRecord{}, func(r gcPauseRecord) int64 {
			return r.pauseUntil.UnixNano()
		}),
		maxLen: 64,
	}
}

// newSystemMetricsListener creates a system metrics listener for garbage collector.
// used to slow down the garbage collector when cpu usage is high.
func newSystemMetricsListener(opt *GcOption) *hardware.SystemMetricsListener {
	return &hardware.SystemMetricsListener{
		Cooldown:  15 * time.Second,
		Context:   false,
		Condition: func(metrics hardware.SystemMetrics, listener *hardware.SystemMetricsListener) bool { return true },
		Callback: func(metrics hardware.SystemMetrics, listener *hardware.SystemMetricsListener) {
			isSlowDown := listener.Context.(bool)
			if metrics.UsedRatio() > paramtable.Get().DataCoordCfg.GCSlowDownCPUUsageThreshold.GetAsFloat() {
				if !isSlowDown {
					mlog.Info(context.TODO(), "garbage collector slow down...", mlog.Float64("cpuUsage", metrics.UsedRatio()))
					opt.removeObjectPool.Resize(1)
					listener.Context = true
				}
				return
			}
			if isSlowDown {
				mlog.Info(context.TODO(), "garbage collector slow down finished", mlog.Float64("cpuUsage", metrics.UsedRatio()))
				opt.removeObjectPool.Resize(paramtable.Get().DataCoordCfg.GCRemoveConcurrent.GetAsInt())
				listener.Context = false
			}
		},
	}
}

// newGarbageCollector create garbage collector with meta and option
func newGarbageCollector(meta *meta, handler Handler, opt GcOption) *garbageCollector {
	mlog.Info(context.TODO(), "GC with option",
		mlog.Bool("enabled", opt.enabled),
		mlog.Duration("interval", opt.checkInterval),
		mlog.Duration("scanInterval", opt.scanInterval),
		mlog.Duration("missingTolerance", opt.missingTolerance),
		mlog.Duration("dropTolerance", opt.dropTolerance))
	opt.removeObjectPool = conc.NewPool[struct{}](Params.DataCoordCfg.GCRemoveConcurrent.GetAsInt(), conc.WithExpiryDuration(time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	metaSignal := make(chan gcCmd)
	orphanSignal := make(chan gcCmd)
	lobSignal := make(chan gcCmd)
	controlChannels := map[string]chan gcCmd{
		"meta":   metaSignal,
		"orphan": orphanSignal,
		"lob":    lobSignal,
	}
	return &garbageCollector{
		ctx:                   ctx,
		cancel:                cancel,
		meta:                  meta,
		handler:               handler,
		option:                opt,
		cmdCh:                 make(chan gcCmd),
		systemMetricsListener: newSystemMetricsListener(&opt),
		pauseUntil:            NewGCPauseRecords(),
		pausedCollection:      typeutil.NewConcurrentMap[int64, *gcPauseRecords](),
		controlChannels:       controlChannels,
	}
}

// start a goroutine and perform gc check every `checkInterval`
func (gc *garbageCollector) start() {
	if gc.option.enabled {
		if gc.option.cli == nil {
			mlog.Warn(gc.ctx, "DataCoord gc enabled, but SSO client is not provided")
			return
		}
		gc.startOnce.Do(func() {
			gc.work(gc.ctx)
		})
	}
}

// GcStatus holds the current status of the garbage collector.
type GcStatus struct {
	IsPaused      bool
	TimeRemaining time.Duration
}

// GetStatus returns the current status of the garbage collector.
func (gc *garbageCollector) GetStatus() GcStatus {
	pauseUntil := gc.pauseUntil.PauseUntil()
	now := time.Now()

	if now.Before(pauseUntil) {
		return GcStatus{
			IsPaused:      true,
			TimeRemaining: pauseUntil.Sub(now),
		}
	}

	return GcStatus{
		IsPaused:      false,
		TimeRemaining: 0,
	}
}

func (gc *garbageCollector) Pause(ctx context.Context, collectionID int64, ticket string, pauseDuration time.Duration) error {
	if !gc.option.enabled {
		mlog.Info(ctx, "garbage collection not enabled")
		return nil
	}
	done := make(chan error, 1)
	select {
	case gc.cmdCh <- gcCmd{
		cmdType:      datapb.GcCommand_Pause,
		duration:     pauseDuration,
		collectionID: collectionID,
		ticket:       ticket,
		done:         done,
		timeout:      ctx.Done(),
	}:
		return <-done
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (gc *garbageCollector) Resume(ctx context.Context, collectionID int64, ticket string) error {
	if !gc.option.enabled {
		mlog.Warn(ctx, "garbage collection not enabled, cannot resume")
		return merr.WrapErrServiceUnavailable("garbage collection not enabled")
	}
	done := make(chan error)
	select {
	case gc.cmdCh <- gcCmd{
		cmdType:      datapb.GcCommand_Resume,
		done:         done,
		collectionID: collectionID,
		ticket:       ticket,
		timeout:      ctx.Done(),
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
	gc.wg.Add(4)
	go func() {
		defer gc.wg.Done()
		gc.runRecycleTaskWithPauser(ctx, "meta", gc.option.checkInterval, func(ctx context.Context, signal <-chan gcCmd) {
			gc.recycleDroppedSegments(ctx, signal)
			gc.recycleChannelCPMeta(ctx, signal)
			gc.recycleUnusedIndexes(ctx, signal)
			gc.recycleUnusedSegIndexes(ctx, signal)
			gc.recycleUnusedAnalyzeFiles(ctx, signal)
			gc.recycleUnusedTextIndexFiles(ctx, signal)
			gc.recycleUnusedJSONIndexFiles(ctx, signal)
			gc.recycleUnusedJSONStatsFiles(ctx, signal)
			gc.recycleSnapshots(ctx, signal)
		})
	}()
	go func() {
		defer gc.wg.Done()
		gc.runRecycleTaskWithPauser(ctx, "orphan", gc.option.scanInterval, func(ctx context.Context, signal <-chan gcCmd) {
			// orphan file not controlled by collection level pause for now
			gc.recycleUnusedBinlogFiles(ctx)
			gc.recycleUnusedIndexFilesV0(ctx)
			gc.recycleUnusedIndexFilesV1(ctx)
		})
	}()
	go func() {
		defer gc.wg.Done()
		// LOB (TEXT column) file GC runs on its own interval
		lobCheckInterval := Params.DataCoordCfg.GCLOBCheckInterval.GetAsDuration(time.Second)
		gc.runRecycleTaskWithPauser(ctx, "lob", lobCheckInterval, func(ctx context.Context, signal <-chan gcCmd) {
			gc.recycleUnusedLOBFiles(ctx)
		})
	}()
	go func() {
		defer gc.wg.Done()
		gc.startControlLoop(ctx)
	}()
}

func (gc *garbageCollector) ackSignal(signal <-chan gcCmd) {
	select {
	case cmd := <-signal:
		if cmd.done != nil {
			close(cmd.done)
		}
	default:
	}
}

// startControlLoop start a control loop for garbageCollector.
func (gc *garbageCollector) startControlLoop(_ context.Context) {
	hardware.RegisterSystemMetricsListener(gc.systemMetricsListener)
	defer hardware.UnregisterSystemMetricsListener(gc.systemMetricsListener)

	for {
		select {
		case cmd := <-gc.cmdCh:
			switch cmd.cmdType {
			case datapb.GcCommand_Pause:
				err := gc.pause(cmd)
				cmd.done <- err
			case datapb.GcCommand_Resume:
				gc.resume(cmd)
			}
			close(cmd.done)
		case <-gc.ctx.Done():
			mlog.Warn(gc.ctx, "garbage collector control loop quit")
			return
		}
	}
}

func (gc *garbageCollector) pause(cmd gcCmd) error {
	log := mlog.With(
		mlog.Int64("collectionID", cmd.collectionID),
		mlog.String("ticket", cmd.ticket),
	)
	reqPauseUntil := time.Now().Add(cmd.duration)
	log = log.With(
		mlog.Time("pauseUntil", reqPauseUntil),
		mlog.Duration("duration", cmd.duration),
	)
	var err error
	if cmd.collectionID <= 0 { // legacy pause all
		err = gc.pauseUntil.Insert(cmd.ticket, reqPauseUntil)
		log.Info(gc.ctx, "global pause ticket recorded")
	} else {
		curr, has := gc.pausedCollection.Get(cmd.collectionID)
		if !has {
			curr = NewGCPauseRecords()
			gc.pausedCollection.Insert(cmd.collectionID, curr)
		}
		err = curr.Insert(cmd.ticket, reqPauseUntil)
		log.Info(gc.ctx, "collection new pause ticket recorded")
	}
	if err != nil {
		return err
	}
	signalCh := gc.controlChannels["meta"]
	// send signal to worker
	// make sure worker ack the pause command before returning
	signal := gcCmd{
		done:    make(chan error),
		timeout: cmd.timeout,
	}
	select {
	case signalCh <- signal:
		<-signal.done
	case <-cmd.timeout:
		// timeout, resume the pause
		gc.resume(cmd)
	}
	return nil
}

func (gc *garbageCollector) resume(cmd gcCmd) {
	// reset to zero value
	var afterResume time.Time
	if cmd.collectionID <= 0 {
		gc.pauseUntil.Delete(cmd.ticket)
		afterResume = gc.pauseUntil.PauseUntil()
	} else {
		curr, has := gc.pausedCollection.Get(cmd.collectionID)
		if has {
			curr.Delete(cmd.ticket)
			afterResume = curr.PauseUntil()
			if curr.Len() == 0 || time.Now().After(afterResume) {
				gc.pausedCollection.Remove(cmd.collectionID)
			}
		}
	}
	stillPaused := time.Now().Before(afterResume)
	mlog.Info(gc.ctx, "garbage collection resumed", mlog.Bool("stillPaused", stillPaused))
}

// runRecycleTaskWithPauser is a helper function to create a task with pauser
func (gc *garbageCollector) runRecycleTaskWithPauser(ctx context.Context, name string, interval time.Duration, task func(ctx context.Context, signal <-chan gcCmd)) {
	logger := mlog.With(mlog.String("gcType", name)).With(mlog.Duration("interval", interval))
	timer := time.NewTicker(interval)
	defer timer.Stop()
	// get signal channel, ok if nil, means no control
	signal := gc.controlChannels[name]
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-signal:
			// notify signal received
			close(cmd.done)
		case <-timer.C:
			globalPauseUntil := gc.pauseUntil.PauseUntil()
			if time.Now().Before(globalPauseUntil) {
				logger.Info(ctx, "garbage collector paused", mlog.Time("until", globalPauseUntil))
				continue
			}
			logger.Info(ctx, "garbage collector recycle task start...")
			start := time.Now()
			task(ctx, signal)
			logger.Info(ctx, "garbage collector recycle task done", mlog.Duration("timeCost", time.Since(start)))
		}
	}
}

func (gc *garbageCollector) collectionGCPaused(collectionID int64) bool {
	collPauseUntil, has := gc.pausedCollection.Get(collectionID)
	return has && time.Now().Before(collPauseUntil.PauseUntil())
}

// close stop the garbage collector.
func (gc *garbageCollector) close() {
	gc.stopOnce.Do(func() {
		gc.cancel()
		gc.wg.Wait()
		if gc.option.removeObjectPool != nil {
			gc.option.removeObjectPool.Release()
		}
	})
}

// recycleUnusedBinlogFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) recycleUnusedBinlogFiles(ctx context.Context) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedBinlogFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedBinlogFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedBinlogFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	type scanTask struct {
		prefix            string
		checker           func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool
		segmentIDFromPath func(rootPath, filePath string) (int64, error)
		label             string
	}
	scanTasks := []scanTask{
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentInsertLogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				return segment != nil
			},
			segmentIDFromPath: storage.ParseSegmentIDByBinlog,
			label:             metrics.InsertFileLabel,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentStatslogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				logID, err := binlog.GetLogIDFromBingLogPath(objectInfo.FilePath)
				if err != nil {
					log.Warn(ctx, "garbageCollector find dirty stats log", mlog.String("filePath", objectInfo.FilePath), mlog.Err(err))
					return false
				}
				return segment != nil && segment.IsStatsLogExists(logID)
			},
			segmentIDFromPath: storage.ParseSegmentIDByBinlog,
			label:             metrics.StatFileLabel,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentDeltaLogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				logID, err := binlog.GetLogIDFromBingLogPath(objectInfo.FilePath)
				if err != nil {
					log.Warn(ctx, "garbageCollector find dirty dleta log", mlog.String("filePath", objectInfo.FilePath), mlog.Err(err))
					return false
				}
				return segment != nil && segment.IsDeltaLogExists(logID)
			},
			segmentIDFromPath: storage.ParseSegmentIDByBinlog,
			label:             metrics.DeleteFileLabel,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.TextIndexPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				if segment == nil {
					return false
				}
				_, ok := getTextLogPaths(segment, gc.option.cli.RootPath())[objectInfo.FilePath]
				return ok
			},
			segmentIDFromPath: parseSegmentIDFromTextIndexPath,
			label:             common.TextIndexPath,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.JSONStatsPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				if segment == nil {
					return false
				}
				_, ok := getJSONKeyLogs(segment, gc)[objectInfo.FilePath]
				return ok
			},
			segmentIDFromPath: parseSegmentIDFromJSONStatsPath,
			label:             common.JSONStatsPath,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.JSONIndexPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				if segment == nil {
					return false
				}
				_, ok := getJSONKeyLogs(segment, gc)[objectInfo.FilePath]
				return ok
			},
			segmentIDFromPath: parseSegmentIDFromJSONIndexPath,
			label:             common.JSONIndexPath,
		},
	}

	for _, task := range scanTasks {
		gc.recycleUnusedBinLogWithChecker(ctx, task.prefix, task.label, task.segmentIDFromPath, task.checker)
	}
	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleUnusedBinLogWithChecker scans the prefix and checks the path with checker.
// GC the file if checker returns false.
func (gc *garbageCollector) recycleUnusedBinLogWithChecker(ctx context.Context, prefix string, label string, segmentIDFromPath func(rootPath, filePath string) (int64, error), checker func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool) {
	logger := mlog.With(mlog.String("prefix", prefix))
	logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles start", mlog.String("prefix", prefix))
	lastFilePath := ""
	total := 0
	valid := 0
	unexpectedFailure := atomic.NewInt32(0)
	removed := atomic.NewInt32(0)
	start := time.Now()

	// isSnapshotProtected checks if a segment should be skipped from GC due to snapshot references.
	// Returns true if the segment is protected (should NOT be deleted).
	//
	// Delegates to snapshotMeta.IsSegmentGCBlocked, which is O(1) and handles the
	// fail-closed layering (unloaded RefIndex → coarse collection-level block, else
	// point query on the pre-computed segmentReferencedByGC set). The per-call caching
	// that used to be needed here is no longer necessary because the lookups are now
	// direct set/map reads.
	snapshotMeta := gc.meta.GetSnapshotMeta()
	isSnapshotProtected := func(segmentID, collectionID int64) bool {
		if snapshotMeta == nil {
			return false
		}
		return snapshotMeta.IsSegmentGCBlocked(collectionID, segmentID)
	}

	futures := make([]*conc.Future[struct{}], 0)
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(chunkInfo *storage.ChunkObjectInfo) bool {
		total++
		lastFilePath = chunkInfo.FilePath

		// Check file tolerance first to avoid unnecessary operation.
		if time.Since(chunkInfo.ModifyTime) <= gc.option.missingTolerance {
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles skip file since it is not expired", mlog.String("filePath", chunkInfo.FilePath), mlog.Time("modifyTime", chunkInfo.ModifyTime))
			return true
		}

		// Parse segmentID from file path.
		// TODO: Does all files in the same segment have the same segmentID?
		segmentID, err := segmentIDFromPath(gc.option.cli.RootPath(), chunkInfo.FilePath)
		if err != nil {
			// Try V3 path format: insert_log/{coll}/{part}/{seg}/...
			v3SegID, parseErr := parseV3SegmentID(gc.option.cli.RootPath(), chunkInfo.FilePath)
			if parseErr != nil {
				unexpectedFailure.Inc()
				logger.Warn(ctx, "garbageCollector recycleUnusedBinlogFiles parse segment id error",
					mlog.String("filePath", chunkInfo.FilePath),
					mlog.Err(err))
				return true
			}
			if v3Seg := gc.meta.GetSegment(ctx, v3SegID); v3Seg != nil {
				if v3Seg.GetStorageVersion() == storage.StorageV3 {
					// registered V3 segment file — skip, live files are managed by
					// loon and dropped V3 segments are recycled with the whole
					// basePath by recycleDroppedSegments
					valid++
					return true
				}
				unexpectedFailure.Inc()
				logger.Warn(ctx, "garbageCollector recycleUnusedBinlogFiles parse segment id error",
					mlog.String("filePath", chunkInfo.FilePath),
					mlog.Err(err))
				return true
			}
			// Orphan V3 file: its segment was never registered in meta, e.g.
			// output uploaded by a failed sort/mix compaction attempt (issue
			// #50962). Nothing manages it, so recycle it like V1/V2 orphans:
			// fall through to the shared checker/removal path below.
			segmentID = v3SegID
		}

		segment := gc.meta.GetSegment(ctx, segmentID)

		// Skip V3 segments — orphan files managed by loon
		if segment != nil && segment.GetStorageVersion() == storage.StorageV3 {
			valid++
			return true
		}

		if checker(chunkInfo, segment) {
			valid++
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles skip file since it is valid", mlog.String("filePath", chunkInfo.FilePath), mlog.Int64("segmentID", segmentID))
			return true
		}

		// Check if segment is referenced by any snapshot before deleting its binlog.
		collectionID := int64(-1)
		if segment != nil {
			collectionID = segment.GetCollectionID()
		}
		if isSnapshotProtected(segmentID, collectionID) {
			logger.Info(ctx, "skip GC binlog files since segment is protected by snapshot",
				mlog.Int64("segmentID", segmentID))
			valid++
			return true
		}

		// ignore error since it could be cleaned up next time
		file := chunkInfo.FilePath

		future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
			logger := logger.With(mlog.String("file", file))
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles remove file...")

			if err = gc.option.cli.Remove(ctx, file); err != nil {
				logger.Warn(ctx, "garbageCollector recycleUnusedBinlogFiles remove file failed", mlog.Err(err))
				unexpectedFailure.Inc()
				return struct{}{}, err
			}
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles remove file success")
			removed.Inc()
			return struct{}{}, nil
		})
		futures = append(futures, future)
		return true
	})
	// Wait for all remove tasks done.
	if err := conc.BlockOnAll(futures...); err != nil {
		// error is logged, and can be ignored here.
		logger.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
	}

	cost := time.Since(start)
	logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles done",
		mlog.Int("total", total),
		mlog.Int("valid", valid),
		mlog.Int("unexpectedFailure", int(unexpectedFailure.Load())),
		mlog.Int("removed", int(removed.Load())),
		mlog.String("lastFilePath", lastFilePath),
		mlog.Duration("cost", cost),
		mlog.Err(err))

	metrics.GarbageCollectorFileScanDuration.
		WithLabelValues(paramtable.GetStringNodeID(), label).
		Observe(float64(cost.Milliseconds()))
}

func (gc *garbageCollector) checkDroppedSegmentGC(segment *SegmentInfo,
	childSegment *SegmentInfo,
	indexSet typeutil.UniqueSet,
	cpTimestamp Timestamp,
) bool {
	log := mlog.With(mlog.Int64("segmentID", segment.ID))

	if !gc.isExpire(segment.GetDroppedAt()) {
		return false
	}
	isCompacted := childSegment != nil || segment.GetCompacted()
	if isCompacted {
		// For compact A, B -> C, don't GC A or B if C is not indexed,
		// guarantee replacing A, B with C won't downgrade performance
		// If the child is GC'ed first, then childSegment will be nil.
		if childSegment != nil && !indexSet.Contain(childSegment.GetID()) {
			log.RatedInfo(gc.ctx, rate.Limit(60), "skipping GC when compact target segment is not indexed",
				mlog.Int64("child segment ID", childSegment.GetID()))
			return false
		}
	}

	segInsertChannel := segment.GetInsertChannel()
	// Ignore segments from potentially dropped collection. Check if collection is to be dropped by checking if channel is dropped.
	// We do this because collection meta drop relies on all segment being GCed.
	if gc.meta.catalog.ChannelExists(context.Background(), segInsertChannel) &&
		segmentEffectiveDmlTs(segment.SegmentInfo) > cpTimestamp {
		// segment gc shall only happen when channel cp is after segment dml cp.
		log.RatedInfo(gc.ctx, rate.Limit(60), "dropped segment dml position after channel cp, skip meta gc",
			mlog.Uint64("dmlPosTs", segmentEffectiveDmlTs(segment.SegmentInfo)),
			mlog.Uint64("channelCpTs", cpTimestamp),
		)
		return false
	}
	return true
}

// recycleDroppedSegments scans all segments and remove those dropped segments from meta and oss.
func (gc *garbageCollector) recycleDroppedSegments(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleDroppedSegments"), mlog.Time("startAt", start))
	log.Info(ctx, "start clear dropped segments...")
	defer func() {
		log.Info(ctx, "clear dropped segments done", mlog.Duration("timeCost", time.Since(start)))
	}()

	all := gc.meta.SelectSegments(ctx)
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

	droppedCompactTo := make(map[int64]*SegmentInfo)
	for id := range drops {
		if to, ok := compactTo[id]; ok {
			droppedCompactTo[to.GetID()] = to
		}
	}
	indexedSegments := FilterInIndexedSegments(ctx, gc.handler, gc.meta, false, lo.Values(droppedCompactTo)...)
	if ctx.Err() != nil {
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

	// try to get loaded segments
	loadedSegments := typeutil.NewSet[int64]()
	segments, err := gc.handler.ListLoadedSegments(ctx)
	if err != nil {
		log.Warn(ctx, "failed to get loaded segments", mlog.Err(err))
		return
	}
	for _, segmentID := range segments {
		loadedSegments.Insert(segmentID)
	}

	log.Info(ctx, "start to GC segments", mlog.Int("drop_num", len(drops)))
	for segmentID, segment := range drops {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}

		gc.ackSignal(signal)

		if gc.collectionGCPaused(segment.GetCollectionID()) {
			log.Info(ctx, "skip GC segment since collection is paused", mlog.Int64("segmentID", segmentID), mlog.Int64("collectionID", segment.GetCollectionID()))
			continue
		}

		log := mlog.With(mlog.Int64("segmentID", segmentID))
		segInsertChannel := segment.GetInsertChannel()
		if loadedSegments.Contain(segmentID) {
			log.Info(ctx, "skip GC segment since it is loaded", mlog.Int64("segmentID", segmentID))
			continue
		}

		// Skip segments protected by snapshot references. IsSegmentGCBlocked is O(1)
		// and embeds the "RefIndex not loaded → fail-closed" check, so we don't need
		// a separate loaded-state probe.
		if snapshotMeta := gc.meta.GetSnapshotMeta(); snapshotMeta != nil {
			if snapshotMeta.IsSegmentGCBlocked(segment.GetCollectionID(), segmentID) {
				log.Info(ctx, "skip GC segment since it is protected by snapshot",
					mlog.Int64("collectionID", segment.GetCollectionID()),
					mlog.Int64("partitionID", segment.GetPartitionID()),
					mlog.String("channel", segInsertChannel),
					mlog.Int64("segmentID", segmentID))
				continue
			}
		}

		if !gc.checkDroppedSegmentGC(segment, compactTo[segment.GetID()], indexedSet, channelCPs[segInsertChannel]) {
			continue
		}

		gc.recycleDroppedSegment(ctx, segmentID, segment)
	}
}

// recycleDroppedSegment deletes a single dropped segment's object files,
// segment-index files, segment-index meta, and segment meta in that order.
//
// The ordering matters: files first so a meta-only retry after partial
// file deletion can still observe the leftover keys; segment-index meta
// next so segment meta deletion (the final marker) is the only step that
// commits the GC; if any step fails the later state is preserved for the
// next GC cycle to retry.
//
// This path can race with recycleUnusedSegIndexes on the same BuildID
// whenever a dropped segment's parent field index has also been marked
// IsDeleted — both paths call removeObjectFiles for the same index files
// and call indexMeta.RemoveSegmentIndex(buildID). The races are safe today
// only because two invariants hold elsewhere:
//
//  1. removeObjectFiles swallows merr.ErrIoKeyNotFound (see the loop in
//     removeObjectFiles below), so a double file delete is a no-op for the
//     loser.
//  2. indexMeta.RemoveSegmentIndex acquires a per-buildID keyLock and
//     returns nil when segmentBuildInfo.Get(buildID) is !ok (see
//     index_meta.go), so a double catalog delete is a no-op for the loser.
//
// Any future refactor on either side — tightening removeObjectFiles to
// surface NotFound, or batching RemoveSegmentIndex past the per-buildID
// keyLock — must preserve these invariants, otherwise dropped-segment GC
// will silently break under load.
func (gc *garbageCollector) recycleDroppedSegment(ctx context.Context, segmentID int64, segment *SegmentInfo) {
	log := mlog.With(mlog.Int64("segmentID", segmentID), mlog.Int64("collectionID", segment.GetCollectionID()))

	if ctx.Err() != nil {
		return
	}

	segIndexes, indexFiles, indexSnapshotBlocked := gc.getDroppedSegmentIndexFiles(segmentID)
	if indexSnapshotBlocked {
		log.Info(ctx, "skip GC segment since segment index is protected by snapshot",
			mlog.Int("segmentIndexes", len(segIndexes)))
		return
	}

	cloned := segment.Clone()
	if err := gc.removeDroppedSegmentFiles(ctx, cloned, indexFiles); err != nil {
		log.Warn(ctx, "GC segment remove files failed", mlog.Err(err))
		return
	}

	if ctx.Err() != nil {
		return
	}

	if err := gc.removeDroppedSegmentIndexMeta(ctx, segIndexes); err != nil {
		log.Warn(ctx, "GC segment index meta failed, wait to retry", mlog.Err(err))
		return
	}

	if ctx.Err() != nil {
		return
	}

	if err := gc.meta.DropSegment(ctx, cloned.GetID()); err != nil {
		log.Warn(ctx, "GC segment meta failed to drop segment", mlog.Err(err))
		return
	}
	log.Info(ctx, "GC segment meta drop segment done", mlog.Int("segmentIndexes", len(segIndexes)))
}

func (gc *garbageCollector) getDroppedSegmentIndexFiles(segmentID int64) ([]*model.SegmentIndex, map[string]struct{}, bool) {
	segIndexes := gc.getAllSegmentIndexesForDroppedSegment(segmentID)
	if len(segIndexes) == 0 {
		return nil, nil, false
	}
	if snapshotMeta := gc.meta.GetSnapshotMeta(); snapshotMeta != nil {
		for _, segIdx := range segIndexes {
			if snapshotMeta.IsBuildIDGCBlocked(segIdx.CollectionID, segIdx.BuildID) {
				return segIndexes, nil, true
			}
		}
	}
	indexFiles := make(map[string]struct{}, len(segIndexes))
	for _, segIdx := range segIndexes {
		for key := range gc.getAllIndexFilesOfIndex(segIdx) {
			indexFiles[key] = struct{}{}
		}
	}
	return segIndexes, indexFiles, false
}

// getAllSegmentIndexesForDroppedSegment wraps indexMeta.GetAllSegmentIndexes
// with a defensive nil guard. Production newMeta always wires indexMeta, but
// the guard is cheap and turns any unexpected nil into "no index records"
// instead of a panic during a GC sweep — keeping a single misbuilt gc
// instance from taking the whole datacoord down.
func (gc *garbageCollector) getAllSegmentIndexesForDroppedSegment(segmentID int64) []*model.SegmentIndex {
	if gc.meta == nil || gc.meta.indexMeta == nil {
		return nil
	}
	return gc.meta.indexMeta.GetAllSegmentIndexes(segmentID)
}

func (gc *garbageCollector) removeDroppedSegmentFiles(ctx context.Context, cloned *SegmentInfo, indexFiles map[string]struct{}) error {
	log := mlog.With(mlog.Int64("segmentID", cloned.GetID()))

	// V3 segment data lives under the manifest base path. Segment index files still
	// live under index file prefixes and must be deleted from recorded file keys.
	if cloned.GetStorageVersion() == storage.StorageV3 {
		basePath, _, err := packed.UnmarshalManifestPath(cloned.GetManifestPath())
		if err != nil {
			log.Warn(ctx, "GC V3 segment failed to parse manifest path",
				mlog.String("manifestPath", cloned.GetManifestPath()),
				mlog.Err(err))
			return err
		}
		log.Info(ctx, "GC V3 segment start, removing basePath...",
			mlog.String("basePath", basePath),
			mlog.Int("indexFiles", len(indexFiles)))
		if err := gc.option.cli.RemoveWithPrefix(ctx, basePath); err != nil {
			log.Warn(ctx, "GC V3 segment remove basePath failed",
				mlog.String("basePath", basePath),
				mlog.Err(err))
			return err
		}
		if len(indexFiles) == 0 {
			log.Info(ctx, "GC V3 segment files done")
			return nil
		}
		if err := gc.removeObjectFiles(ctx, indexFiles); err != nil {
			log.Warn(ctx, "GC V3 segment remove index files failed", mlog.Err(err))
			return err
		}
		log.Info(ctx, "GC V3 segment files done")
		return nil
	}

	binlog.DecompressBinLogs(cloned.SegmentInfo)
	logs := getLogs(cloned)
	for key := range getTextLogs(cloned) {
		logs[key] = struct{}{}
	}
	for key := range getJSONKeyLogs(cloned, gc) {
		logs[key] = struct{}{}
	}
	for key := range indexFiles {
		logs[key] = struct{}{}
	}

	log.Info(ctx, "GC segment start...", mlog.Int("insert_logs", len(cloned.GetBinlogs())),
		mlog.Int("delta_logs", len(cloned.GetDeltalogs())),
		mlog.Int("stats_logs", len(cloned.GetStatslogs())),
		mlog.Int("bm25_logs", len(cloned.GetBm25Statslogs())),
		mlog.Int("text_logs", len(cloned.GetTextStatsLogs())),
		mlog.Int("json_key_logs", len(cloned.GetJsonKeyStats())),
		mlog.Int("index_files", len(indexFiles)))
	if err := gc.removeObjectFiles(ctx, logs); err != nil {
		log.Warn(ctx, "GC segment remove logs failed", mlog.Err(err))
		return err
	}
	return nil
}

func (gc *garbageCollector) removeDroppedSegmentIndexMeta(ctx context.Context, segIndexes []*model.SegmentIndex) error {
	if len(segIndexes) == 0 {
		return nil
	}
	for _, segIdx := range segIndexes {
		if err := gc.meta.indexMeta.RemoveSegmentIndex(ctx, segIdx.BuildID); err != nil {
			return err
		}
	}
	return nil
}

func (gc *garbageCollector) recycleChannelCPMeta(ctx context.Context, signal <-chan gcCmd) {
	channelCPs, err := gc.meta.catalog.ListChannelCheckpoint(ctx)
	if err != nil {
		mlog.Warn(ctx, "list channel cp fail during GC", mlog.Err(err))
		return
	}

	collectionID2GcStatus := make(map[int64]bool)
	skippedCnt := 0

	mlog.Info(ctx, "start to GC channel cp", mlog.Int("vchannelCPCnt", len(channelCPs)))
	for vChannel := range channelCPs {
		collectionID := funcutil.GetCollectionIDFromVChannel(vChannel)
		if gc.collectionGCPaused(collectionID) {
			continue
		}

		gc.ackSignal(signal)

		// !!! Skip to GC if vChannel format is illegal, it will lead meta leak in this case
		if collectionID == -1 {
			skippedCnt++
			mlog.Warn(ctx, "parse collection id fail, skip to gc channel cp", mlog.String("vchannel", vChannel))
			continue
		}

		_, ok := collectionID2GcStatus[collectionID]
		if !ok {
			if ctx.Err() != nil {
				// process canceled, stop.
				return
			}
			timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			has, err := gc.option.broker.HasCollection(timeoutCtx, collectionID)
			if err == nil && !has {
				collectionID2GcStatus[collectionID] = gc.meta.catalog.GcConfirm(ctx, collectionID, -1)
			} else {
				// skip checkpoints GC of this cycle if describe collection fails or the collection state is available.
				mlog.Debug(ctx, "skip channel cp GC, the collection state is available",
					mlog.Int64("collectionID", collectionID),
					mlog.Bool("dropped", has), mlog.Err(err))
				collectionID2GcStatus[collectionID] = false
			}
		}

		// Skip to GC if all segments meta of the corresponding collection are not removed
		if gcConfirmed := collectionID2GcStatus[collectionID]; !gcConfirmed {
			skippedCnt++
			continue
		}

		err := gc.meta.DropChannelCheckpoint(vChannel)
		if err != nil {
			// Try to GC in the next gc cycle if drop channel cp meta fail.
			mlog.Warn(ctx, "failed to drop channelcp check point during gc", mlog.String("vchannel", vChannel), mlog.Err(err))
		} else {
			mlog.Info(ctx, "GC channel cp", mlog.String("vchannel", vChannel))
		}
	}

	mlog.Info(ctx, "GC channel cp done", mlog.Int("skippedChannelCP", skippedCnt))
}

func (gc *garbageCollector) isExpire(dropts Timestamp) bool {
	droptime := time.Unix(0, int64(dropts))
	return time.Since(droptime) > gc.option.dropTolerance
}

// parseV3SegmentID attempts to parse segmentID from a V3 path format.
// V3 paths: {root}/insert_log/{coll}/{part}/{seg}/...
// Returns segmentID or error if path doesn't match.
func parseV3SegmentID(rootPath, filePath string) (int64, error) {
	if !strings.HasPrefix(filePath, rootPath) {
		return 0, merr.WrapErrServiceInternalMsg("path %q does not contain rootPath %q", filePath, rootPath)
	}
	p := strings.TrimPrefix(filePath[len(rootPath):], "/")
	parts := strings.Split(p, "/")
	if len(parts) < 5 || parts[0] != common.SegmentInsertLogPath {
		return 0, merr.WrapErrServiceInternalMsg("not a V3 insert_log path: %s", filePath)
	}
	return strconv.ParseInt(parts[3], 10, 64)
}

func parseSegmentIDFromAuxIndexPath(rootPath, filePath string) (int64, error) {
	if !strings.HasPrefix(filePath, rootPath) {
		return 0, merr.WrapErrServiceInternalMsg("path %q does not contain rootPath %q", filePath, rootPath)
	}
	p := strings.TrimPrefix(filePath[len(rootPath):], "/")
	parts := strings.Split(p, "/")
	if len(parts) < 8 || (parts[0] != common.TextIndexPath && parts[0] != common.JSONIndexPath) {
		return 0, merr.WrapErrServiceInternalMsg("not an auxiliary index path: %s", filePath)
	}
	return strconv.ParseInt(parts[5], 10, 64)
}

func parseSegmentIDFromJSONStatsPath(rootPath, filePath string) (int64, error) {
	if !strings.HasPrefix(filePath, rootPath) {
		return 0, merr.WrapErrServiceInternalMsg("path %q does not contain rootPath %q", filePath, rootPath)
	}
	p := strings.TrimPrefix(filePath[len(rootPath):], "/")
	parts := strings.Split(p, "/")
	if len(parts) < 9 || parts[0] != common.JSONStatsPath {
		return 0, merr.WrapErrServiceInternalMsg("not a json stats path: %s", filePath)
	}
	return strconv.ParseInt(parts[6], 10, 64)
}

func parseSegmentIDFromTextIndexPath(rootPath, filePath string) (int64, error) {
	return parseSegmentIDFromAuxIndexPath(rootPath, filePath)
}

func parseSegmentIDFromJSONIndexPath(rootPath, filePath string) (int64, error) {
	return parseSegmentIDFromAuxIndexPath(rootPath, filePath)
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
	for _, flog := range sinfo.GetBm25Statslogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	return logs
}

func getTextLogs(sinfo *SegmentInfo) map[string]struct{} {
	return getTextLogPaths(sinfo, "")
}

func getTextLogPaths(sinfo *SegmentInfo, rootPath string) map[string]struct{} {
	textLogs := make(map[string]struct{})
	for _, flog := range sinfo.GetTextStatsLogs() {
		files := flog.GetFiles()
		if rootPath != "" {
			basePath := metautil.BuildTextIndexPrefix(rootPath,
				flog.GetBuildID(), flog.GetVersion(),
				sinfo.GetCollectionID(), sinfo.GetPartitionID(), sinfo.GetID(), flog.GetFieldID())
			files = metautil.BuildStatsFilePaths(basePath, files)
		}
		for _, file := range files {
			textLogs[file] = struct{}{}
		}
	}

	return textLogs
}

func getJSONKeyLogs(sinfo *SegmentInfo, gc *garbageCollector) map[string]struct{} {
	jsonkeyLogs := make(map[string]struct{})
	for _, flog := range sinfo.GetJsonKeyStats() {
		for _, file := range flog.GetFiles() {
			var prefix string
			if flog.GetJsonKeyStatsDataFormat() >= 2 {
				prefix = metautil.BuildJSONKeyStatsPrefix(
					gc.option.cli.RootPath(),
					flog.GetJsonKeyStatsDataFormat(),
					flog.GetBuildID(),
					flog.GetVersion(),
					sinfo.GetCollectionID(),
					sinfo.GetPartitionID(),
					sinfo.GetID(),
					flog.GetFieldID(),
				)
			} else {
				prefix = fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d", gc.option.cli.RootPath(), common.JSONIndexPath,
					flog.GetBuildID(), flog.GetVersion(), sinfo.GetCollectionID(), sinfo.GetPartitionID(), sinfo.GetID(), flog.GetFieldID())
			}
			file = path.Join(prefix, file)
			jsonkeyLogs[file] = struct{}{}
		}
	}

	return jsonkeyLogs
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
				mlog.Info(ctx, "remove log failed, key not found, may be removed at previous GC, ignore the error",
					mlog.String("path", filePath),
					mlog.Err(err))
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	return conc.BlockOnAll(futures...)
}

// recycleUnusedIndexes is used to delete those indexes that is deleted by collection.
func (gc *garbageCollector) recycleUnusedIndexes(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedIndexes"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedIndexes...")
	defer func() {
		log.Info(ctx, "recycleUnusedIndexes done", mlog.Duration("timeCost", time.Since(start)))
	}()

	deletedIndexes := gc.meta.indexMeta.GetDeletedIndexes()
	for _, index := range deletedIndexes {
		if ctx.Err() != nil {
			// process canceled.
			return
		}
		if gc.collectionGCPaused(index.CollectionID) {
			continue
		}
		gc.ackSignal(signal)

		log := mlog.With(mlog.Int64("collectionID", index.CollectionID), mlog.Int64("fieldID", index.FieldID), mlog.Int64("indexID", index.IndexID))
		if err := gc.meta.indexMeta.RemoveIndex(ctx, index.CollectionID, index.IndexID); err != nil {
			log.Warn(ctx, "remove index on collection fail", mlog.Err(err))
			continue
		}
		log.Info(ctx, "remove index on collection done")
	}
}

// recycleUnusedSegIndexes remove the index of segment if index is deleted or segment itself is deleted.
func (gc *garbageCollector) recycleUnusedSegIndexes(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedSegIndexes"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedSegIndexes...")
	defer func() {
		log.Info(ctx, "recycleUnusedSegIndexes done", mlog.Duration("timeCost", time.Since(start)))
	}()

	segIndexes := gc.meta.indexMeta.GetAllSegIndexes()
	for _, candidate := range segIndexes {
		if ctx.Err() != nil {
			// process canceled.
			return
		}
		// GetAllSegIndexes returns a point-in-time snapshot. Refresh by buildID
		// before making deletion decisions so GC does not act on stale task state
		// or stale index file keys.
		segIdx, ok := gc.getLatestSegmentIndexForGC(candidate)
		if !ok {
			continue
		}
		if gc.collectionGCPaused(segIdx.CollectionID) {
			continue
		}
		gc.ackSignal(signal)

		// 1. segment belongs to is deleted.
		// 2. index is deleted.
		if gc.meta.GetSegment(ctx, segIdx.SegmentID) == nil || !gc.meta.indexMeta.IsIndexExist(segIdx.CollectionID, segIdx.IndexID) {
			// Non-terminal tasks may still be writing index files or updating meta.
			// Keep the SegmentIndex until the task reaches a terminal state.
			if segIdx.IndexState == commonpb.IndexState_Unissued ||
				segIdx.IndexState == commonpb.IndexState_InProgress ||
				segIdx.IndexState == commonpb.IndexState_Retry {
				log.Info(ctx, "skip GC segment index since index task is not terminal",
					mlog.Int64("collectionID", segIdx.CollectionID),
					mlog.Int64("partitionID", segIdx.PartitionID),
					mlog.Int64("segmentID", segIdx.SegmentID),
					mlog.Int64("indexID", segIdx.IndexID),
					mlog.Int64("buildID", segIdx.BuildID),
					mlog.String("state", segIdx.IndexState.String()))
				continue
			}
			indexFiles := gc.getAllIndexFilesOfIndex(segIdx)
			// Empty indexFiles is valid for fake-finished small/no-train indexes.
			// removeObjectFiles is a no-op in that case; the stale meta still needs
			// to be removed when its segment or field index is gone.
			log := mlog.With(mlog.Int64("collectionID", segIdx.CollectionID),
				mlog.Int64("partitionID", segIdx.PartitionID),
				mlog.Int64("segmentID", segIdx.SegmentID),
				mlog.Int64("indexID", segIdx.IndexID),
				mlog.Int64("buildID", segIdx.BuildID),
				mlog.Int64("nodeID", segIdx.NodeID),
				mlog.Int("indexFiles", len(indexFiles)))

			// Skip buildIDs protected by snapshot references. IsBuildIDGCBlocked is O(1)
			// and embeds the "RefIndex not loaded → fail-closed" check.
			if snapshotMeta := gc.meta.GetSnapshotMeta(); snapshotMeta != nil {
				if snapshotMeta.IsBuildIDGCBlocked(segIdx.CollectionID, segIdx.BuildID) {
					log.Info(ctx, "skip GC segment index since buildID is protected by snapshot",
						mlog.Int64("collectionID", segIdx.CollectionID),
						mlog.Int64("buildID", segIdx.BuildID))
					continue
				}
			}

			log.Info(ctx, "GC Segment Index file start...")

			// Remove index files first.
			if err := gc.removeObjectFiles(ctx, indexFiles); err != nil {
				log.Warn(ctx, "fail to remove index files for index", mlog.Err(err))
				continue
			}

			// Remove meta from index meta.
			if err := gc.meta.indexMeta.RemoveSegmentIndex(ctx, segIdx.BuildID); err != nil {
				log.Warn(ctx, "delete index meta from etcd failed, wait to retry", mlog.Err(err))
				continue
			}
			log.Info(ctx, "index meta recycle success")
		}
	}
}

// getLatestSegmentIndexForGC takes a SegmentIndex candidate from GC scanning and
// returns the latest SegmentIndex meta for the same buildID. The bool return
// value is false when the candidate is nil or the buildID no longer exists.
func (gc *garbageCollector) getLatestSegmentIndexForGC(candidate *model.SegmentIndex) (*model.SegmentIndex, bool) {
	if candidate == nil {
		return nil, false
	}
	if gc.meta == nil || gc.meta.indexMeta == nil || gc.meta.indexMeta.segmentBuildInfo == nil {
		return candidate, true
	}
	return gc.meta.indexMeta.GetIndexJob(candidate.BuildID)
}

// recycleUnusedIndexFilesV0 deletes orphan files under the legacy v0 index_files prefix.
// v0 paths are rooted by buildID, so the first-level directory can be parsed and
// checked against index meta directly.
func (gc *garbageCollector) recycleUnusedIndexFilesV0(ctx context.Context) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedIndexFilesV0"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedIndexFilesV0...")

	prefix := path.Join(gc.option.cli.RootPath(), common.SegmentIndexV0Path) + "/"

	// Resolve snapshotMeta once. Both IsBuildIDGCBlocked paths below are O(1) so
	// no caching of intermediate state is needed.
	snapshotMeta := gc.meta.GetSnapshotMeta()

	// list dir first
	keyCount := 0
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, false, func(indexPathInfo *storage.ChunkObjectInfo) bool {
		key := indexPathInfo.FilePath
		keyCount++
		logger := mlog.With(mlog.String("prefix", prefix), mlog.String("key", key))

		// This recycler only walks index_files/ (v0). Its first path level is buildID;
		// v1 collectionID directories live under index_v1/ and are handled below.
		buildID, err := parseBuildIDFromFilePath(key)
		if err != nil {
			logger.Warn(ctx, "garbageCollector recycleUnusedIndexFilesV0 parseIndexFileKey", mlog.Err(err))
			return true
		}
		logger = logger.With(mlog.Int64("buildID", buildID))
		logger.Info(ctx, "garbageCollector will recycle index files")
		canRecycle, segIdx := gc.meta.indexMeta.CheckCleanSegmentIndex(buildID)
		if !canRecycle {
			// Even if the index is marked as deleted, the index file will not be recycled, wait for the next gc,
			// and delete all index files about the buildID at one time.
			logger.Info(ctx, "garbageCollector can not recycle index files")
			return true
		}
		if segIdx == nil {
			// buildID no longer exists in meta. Orphan buildID walk has no collection context,
			// so IsBuildIDGCBlocked(-1, buildID) fail-closes on ANY unloaded RefIndex globally.
			if snapshotMeta != nil && snapshotMeta.IsBuildIDGCBlocked(-1, buildID) {
				logger.Info(ctx, "skip GC index files since buildID is protected by snapshot",
					mlog.Int64("buildID", buildID))
				return true
			}

			// buildID no longer exists in meta, remove all index files
			logger.Info(ctx, "garbageCollector recycleUnusedIndexFilesV0 find meta has not exist, remove index files")
			err = gc.option.cli.RemoveWithPrefix(ctx, key)
			if err != nil {
				logger.Warn(ctx, "garbageCollector recycleUnusedIndexFilesV0 remove index files failed", mlog.Err(err))
				return true
			}
			logger.Info(ctx, "garbageCollector recycleUnusedIndexFilesV0 remove index files success")
			return true
		}

		// Skip buildIDs protected by snapshot references. IsBuildIDGCBlocked is O(1)
		// and embeds the "RefIndex not loaded → fail-closed" check.
		if snapshotMeta != nil {
			if snapshotMeta.IsBuildIDGCBlocked(segIdx.CollectionID, segIdx.BuildID) {
				logger.Info(ctx, "skip GC index files since buildID is protected by snapshot",
					mlog.Int64("collectionID", segIdx.CollectionID),
					mlog.Int64("buildID", segIdx.BuildID))
				return true
			}
		}

		filesMap := gc.getAllIndexFilesOfIndex(segIdx)

		logger.Info(ctx, "recycle index files", mlog.Int("meta files num", len(filesMap)))
		deletedFilesNum := atomic.NewInt32(0)
		fileNum := 0

		futures := make([]*conc.Future[struct{}], 0)
		err = gc.option.cli.WalkWithPrefix(ctx, key, true, func(indexFile *storage.ChunkObjectInfo) bool {
			fileNum++
			file := indexFile.FilePath
			if _, ok := filesMap[file]; !ok {
				future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
					logger := logger.With(mlog.String("file", file))
					logger.Info(ctx, "garbageCollector recycleUnusedIndexFilesV0 remove file...")

					if err := gc.option.cli.Remove(ctx, file); err != nil {
						logger.Warn(ctx, "garbageCollector recycleUnusedIndexFilesV0 remove file failed", mlog.Err(err))
						return struct{}{}, err
					}
					deletedFilesNum.Inc()
					logger.Info(ctx, "garbageCollector recycleUnusedIndexFilesV0 remove file success")
					return struct{}{}, nil
				})
				futures = append(futures, future)
			}
			return true
		})
		// Wait for all remove tasks done.
		if err := conc.BlockOnAll(futures...); err != nil {
			// error is logged, and can be ignored here.
			logger.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
		}

		logger = logger.With(mlog.Int("deleteIndexFilesNum", int(deletedFilesNum.Load())), mlog.Int("walkFileNum", fileNum))
		if err != nil {
			logger.Warn(ctx, "index files recycle failed when walk with prefix", mlog.Err(err))
			return true
		}
		logger.Info(ctx, "index files recycle done")
		return true
	})
	log = log.With(mlog.Duration("timeCost", time.Since(start)), mlog.Int("keyCount", keyCount), mlog.Err(err))
	if err != nil {
		log.Warn(ctx, "garbageCollector recycleUnusedIndexFilesV0 failed", mlog.Err(err))
		return
	}
	log.Info(ctx, "recycleUnusedIndexFilesV0 done")
}

// getAllIndexFilesOfIndex returns all expected index files using the path version
// recorded on the SegmentIndex: v0 builds index_files paths, v1 builds index_v1 paths.
func (gc *garbageCollector) getAllIndexFilesOfIndex(segmentIndex *model.SegmentIndex) map[string]struct{} {
	builder := metautil.NewIndexPathBuilder(gc.option.cli.RootPath(),
		segmentIndex.IndexStorePathVersion, segmentIndex.CollectionID,
		segmentIndex.PartitionID, segmentIndex.SegmentID,
		segmentIndex.BuildID, segmentIndex.IndexVersion)
	filesMap := make(map[string]struct{})
	for _, fileID := range segmentIndex.IndexFileKeys {
		filesMap[builder.BuildFilePath(fileID)] = struct{}{}
	}
	return filesMap
}

// recycleUnusedIndexFilesV1 cleans index files for v1 format entries (collection-partitioned paths).
// v1 uses the separate index_v1 prefix and puts collectionID before buildID,
// so GC iterates deleted metadata entries instead of trying to parse buildID from a prefix walk.
func (gc *garbageCollector) recycleUnusedIndexFilesV1(ctx context.Context) {
	log := mlog.With(mlog.String("gcName", "recycleUnusedIndexFilesV1"))

	snapshotMeta := gc.meta.GetSnapshotMeta()
	deletedIndexes := gc.meta.indexMeta.GetDeletedIndexesWithV1Path()
	if len(deletedIndexes) == 0 {
		return
	}

	log.Info(ctx, "start recycleUnusedIndexFilesV1", mlog.Int("deletedCount", len(deletedIndexes)))
	futures := make([]*conc.Future[struct{}], 0, len(deletedIndexes))
	for _, segIdx := range deletedIndexes {
		segIdx := segIdx
		if snapshotMeta != nil && snapshotMeta.IsBuildIDGCBlocked(segIdx.CollectionID, segIdx.BuildID) {
			log.Info(ctx, "skip GC v1 index files since buildID is protected by snapshot",
				mlog.Int64("collectionID", segIdx.CollectionID),
				mlog.Int64("buildID", segIdx.BuildID))
			continue
		}

		future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
			builder := metautil.NewIndexPathBuilder(gc.option.cli.RootPath(),
				segIdx.IndexStorePathVersion, segIdx.CollectionID,
				segIdx.PartitionID, segIdx.SegmentID,
				segIdx.BuildID, segIdx.IndexVersion)
			prefix := builder.BuildPrefix() + "/"

			if err := gc.option.cli.RemoveWithPrefix(ctx, prefix); err != nil {
				log.Warn(ctx, "recycleUnusedIndexFilesV1 remove failed",
					mlog.Int64("collectionID", segIdx.CollectionID),
					mlog.Int64("partitionID", segIdx.PartitionID),
					mlog.Int64("segmentID", segIdx.SegmentID),
					mlog.Int64("buildID", segIdx.BuildID),
					mlog.Int64("indexID", segIdx.IndexID),
					mlog.Stringer("pathVersion", segIdx.IndexStorePathVersion),
					mlog.String("prefix", prefix),
					mlog.Err(err))
				return struct{}{}, err
			}
			if err := gc.meta.indexMeta.RemoveSegmentIndex(ctx, segIdx.BuildID); err != nil {
				log.Warn(ctx, "recycleUnusedIndexFilesV1 remove segment index meta failed",
					mlog.Int64("collectionID", segIdx.CollectionID),
					mlog.Int64("partitionID", segIdx.PartitionID),
					mlog.Int64("segmentID", segIdx.SegmentID),
					mlog.Int64("buildID", segIdx.BuildID),
					mlog.Int64("indexID", segIdx.IndexID),
					mlog.Stringer("pathVersion", segIdx.IndexStorePathVersion),
					mlog.String("prefix", prefix),
					mlog.Err(err))
				return struct{}{}, err
			}
			log.Info(ctx, "recycleUnusedIndexFilesV1 removed index files and meta",
				mlog.Int64("collectionID", segIdx.CollectionID),
				mlog.Int64("partitionID", segIdx.PartitionID),
				mlog.Int64("segmentID", segIdx.SegmentID),
				mlog.Int64("buildID", segIdx.BuildID),
				mlog.Int64("indexID", segIdx.IndexID),
				mlog.Stringer("pathVersion", segIdx.IndexStorePathVersion),
				mlog.String("prefix", prefix))
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	if err := conc.BlockOnAll(futures...); err != nil {
		log.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
	}
}

// recycleUnusedAnalyzeFiles is used to delete those analyze stats files that no longer exist in the meta.
func (gc *garbageCollector) recycleUnusedAnalyzeFiles(ctx context.Context, signal <-chan gcCmd) {
	mlog.Info(ctx, "start recycleUnusedAnalyzeFiles")
	startTs := time.Now()
	prefix := path.Join(gc.option.cli.RootPath(), common.AnalyzeStatsPath) + "/"
	// list dir first
	keys := make([]string, 0)
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, false, func(chunkInfo *storage.ChunkObjectInfo) bool {
		keys = append(keys, chunkInfo.FilePath)
		return true
	})
	if err != nil {
		mlog.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles list keys from chunk manager failed", mlog.Err(err))
		return
	}
	mlog.Info(ctx, "recycleUnusedAnalyzeFiles, finish list object", mlog.Duration("time spent", time.Since(startTs)), mlog.Int("task ids", len(keys)))
	for _, key := range keys {
		if ctx.Err() != nil {
			// process canceled
			return
		}
		// collection gc pause not affect analyze file for now
		gc.ackSignal(signal)

		mlog.Debug(ctx, "analyze keys", mlog.String("key", key))
		taskID, err := parseBuildIDFromFilePath(key)
		if err != nil {
			mlog.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles parseAnalyzeResult failed", mlog.String("key", key), mlog.Err(err))
			continue
		}
		mlog.Info(ctx, "garbageCollector will recycle analyze stats files", mlog.Int64("taskID", taskID))
		canRecycle, task := gc.meta.analyzeMeta.CheckCleanAnalyzeTask(taskID)
		if !canRecycle {
			// Even if the analysis task is marked as deleted, the analysis stats file will not be recycled, wait for the next gc,
			// and delete all index files about the taskID at one time.
			mlog.Info(ctx, "garbageCollector no need to recycle analyze stats files", mlog.Int64("taskID", taskID))
			continue
		}
		if task == nil {
			// taskID no longer exists in meta, remove all analysis files
			mlog.Info(ctx, "garbageCollector recycleUnusedAnalyzeFiles find meta has not exist, remove index files",
				mlog.Int64("taskID", taskID))
			err = gc.option.cli.RemoveWithPrefix(ctx, key)
			if err != nil {
				mlog.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles remove analyze stats files failed",
					mlog.Int64("taskID", taskID), mlog.String("prefix", key), mlog.Err(err))
				continue
			}
			mlog.Info(ctx, "garbageCollector recycleUnusedAnalyzeFiles remove analyze stats files success",
				mlog.Int64("taskID", taskID), mlog.String("prefix", key))
			continue
		}

		mlog.Info(ctx, "remove analyze stats files which version is less than current task",
			mlog.Int64("taskID", taskID), mlog.Int64("current version", task.Version))
		var i int64
		for i = 0; i < task.Version; i++ {
			if ctx.Err() != nil {
				// process canceled.
				return
			}
			// analyze stats files are laid out as analyze_stats/{taskID}/{version}/...
			removePrefix := prefix + fmt.Sprintf("%d/%d/", taskID, i)
			if err := gc.option.cli.RemoveWithPrefix(ctx, removePrefix); err != nil {
				mlog.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles remove files with prefix failed",
					mlog.Int64("taskID", taskID), mlog.String("removePrefix", removePrefix))
				continue
			}
		}
		mlog.Info(ctx, "analyze stats files recycle success", mlog.Int64("taskID", taskID))
	}
}

// recycleUnusedTextIndexFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) recycleUnusedTextIndexFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedTextIndexFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedTextIndexFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedTextIndexFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	hasTextIndexSegments := gc.meta.SelectSegments(ctx, SegmentFilterFunc(func(info *SegmentInfo) bool {
		return len(info.GetTextStatsLogs()) != 0
	}))
	fileNum := 0
	deletedFilesNum := atomic.NewInt32(0)

	snapshotMeta := gc.meta.GetSnapshotMeta()

	for _, seg := range hasTextIndexSegments {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}
		if gc.collectionGCPaused(seg.GetCollectionID()) {
			log.Info(ctx, "skip GC segment since collection is paused", mlog.Int64("segmentID", seg.GetID()), mlog.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		// Skip segments whose files are still referenced by snapshots. IsSegmentGCBlocked
		// is O(1) and embeds the "RefIndex not loaded → fail-closed" check.
		if snapshotMeta != nil && snapshotMeta.IsSegmentGCBlocked(seg.GetCollectionID(), seg.GetID()) {
			log.Info(ctx, "skip GC text index files since segment is protected by snapshot",
				mlog.Int64("segmentID", seg.GetID()),
				mlog.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		gc.ackSignal(signal)
		for _, fieldStats := range seg.GetTextStatsLogs() {
			log := mlog.With(mlog.Int64("segmentID", seg.GetID()), mlog.Int64("fieldID", fieldStats.GetFieldID()))
			// clear low version task
			for i := int64(1); i < fieldStats.GetVersion(); i++ {
				prefix := metautil.BuildTextIndexPrefix(gc.option.cli.RootPath(),
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					fileNum++
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log := mlog.With(mlog.String("file", file))
						log.Info(ctx, "garbageCollector recycleUnusedTextIndexFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedTextIndexFiles remove file failed", mlog.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedTextIndexFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
				}

				log = log.With(mlog.Int("deleteIndexFilesNum", int(deletedFilesNum.Load())), mlog.Int("walkFileNum", fileNum))
				if err != nil {
					log.Warn(ctx, "text index files recycle failed when walk with prefix", mlog.Err(err))
					return
				}
			}
		}
	}
	log.Info(ctx, "text index files recycle done")

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleUnusedJSONStatsFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) recycleUnusedJSONStatsFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedJSONStatsFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedJSONStatsFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedJSONStatsFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	hasJSONStatsSegments := gc.meta.SelectSegments(ctx, SegmentFilterFunc(func(info *SegmentInfo) bool {
		return len(info.GetJsonKeyStats()) != 0
	}))
	fileNum := 0
	deletedFilesNum := atomic.NewInt32(0)

	snapshotMeta := gc.meta.GetSnapshotMeta()

	for _, seg := range hasJSONStatsSegments {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}
		if gc.collectionGCPaused(seg.GetCollectionID()) {
			log.Info(ctx, "skip GC segment since collection is paused", mlog.Int64("segmentID", seg.GetID()), mlog.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		// Skip segments whose files are still referenced by snapshots.
		if snapshotMeta != nil && snapshotMeta.IsSegmentGCBlocked(seg.GetCollectionID(), seg.GetID()) {
			log.Info(ctx, "skip GC JSON stats files since segment is protected by snapshot",
				mlog.Int64("segmentID", seg.GetID()),
				mlog.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		gc.ackSignal(signal)
		for _, fieldStats := range seg.GetJsonKeyStats() {
			log := mlog.With(mlog.Int64("segmentID", seg.GetID()), mlog.Int64("fieldID", fieldStats.GetFieldID()))
			// clear low version task
			for i := int64(1); i < fieldStats.GetVersion(); i++ {
				prefix := metautil.BuildJSONKeyStatsPrefix(gc.option.cli.RootPath(), fieldStats.GetJsonKeyStatsDataFormat(),
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					fileNum++
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log := mlog.With(mlog.String("file", file))
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file failed", mlog.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
				}

				if err != nil {
					log.Warn(ctx, "json stats files recycle failed when walk with prefix", mlog.Err(err))
					return
				}
			}

			// clear low data format version stats file
			// for upgrade from old version to new version, we need to clear the old data format version stats file
			for i := int64(1); i < fieldStats.GetJsonKeyStatsDataFormat(); i++ {
				prefix := fmt.Sprintf("%s/%s/%d", gc.option.cli.RootPath(), common.JSONStatsPath, i)
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					fileNum++
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log := mlog.With(mlog.String("file", file))
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file failed", mlog.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
				}

				if err != nil {
					log.Warn(ctx, "json stats lower data format files recycle failed when walk with prefix", mlog.Err(err))
					return
				}
			}
		}
	}
	log.Info(ctx, "json stats files recycle done",
		mlog.Int("deleteJSONStatsNum", int(deletedFilesNum.Load())),
		mlog.Int("walkFileNum", fileNum))

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleUnusedJSONIndexFiles load meta file info and compares OSS keys
func (gc *garbageCollector) recycleUnusedJSONIndexFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedJSONIndexFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedJSONIndexFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedJSONIndexFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	hasJSONIndexSegments := gc.meta.SelectSegments(ctx, SegmentFilterFunc(func(info *SegmentInfo) bool {
		return len(info.GetJsonKeyStats()) != 0
	}))
	fileNum := 0
	deletedFilesNum := atomic.NewInt32(0)

	for _, seg := range hasJSONIndexSegments {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}
		if gc.collectionGCPaused(seg.GetCollectionID()) {
			log.Info(ctx, "skip GC segment since collection is paused", mlog.Int64("segmentID", seg.GetID()), mlog.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		// Skip segments whose files are still referenced by snapshots.
		if snapshotMeta := gc.meta.GetSnapshotMeta(); snapshotMeta != nil {
			if snapshotMeta.IsSegmentGCBlocked(seg.GetCollectionID(), seg.GetID()) {
				log.Info(ctx, "skip GC JSON index files since segment is protected by snapshot",
					mlog.Int64("segmentID", seg.GetID()),
					mlog.Int64("collectionID", seg.GetCollectionID()))
				continue
			}
		}

		gc.ackSignal(signal)
		for _, fieldStats := range seg.GetJsonKeyStats() {
			log := mlog.With(mlog.Int64("segmentID", seg.GetID()), mlog.Int64("fieldID", fieldStats.GetFieldID()))
			// clear low version task
			for i := int64(1); i < fieldStats.GetVersion(); i++ {
				prefix := fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d", gc.option.cli.RootPath(), common.JSONIndexPath,
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					fileNum++
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log := mlog.With(mlog.String("file", file))
						log.Info(ctx, "garbageCollector recycleUnusedJSONIndexFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedJSONIndexFiles remove file failed", mlog.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedJSONIndexFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
				}

				if err != nil {
					log.Warn(ctx, "json index files recycle failed when walk with prefix", mlog.Err(err))
					return
				}
			}
		}
	}
	log.Info(ctx, "json index files recycle done", mlog.Int("deleteJSONKeyIndexNum", int(deletedFilesNum.Load())), mlog.Int("walkFileNum", fileNum))

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleSnapshots cleans up snapshot resources in three phases:
//  1. PENDING snapshots: Failed 2PC commits that exceeded timeout — clean S3 + catalog.
//  2. DELETING snapshots: DropSnapshot succeeded but S3 cleanup failed — retry S3 + catalog.
//  3. Orphan snapshots: Snapshots whose collection was dropped — clean expired pins, then drop.
//
// Process flow:
//  1. Get all PENDING snapshots from catalog that have exceeded timeout.
//  2. For each pending snapshot:
//     a. Compute manifest directory and metadata file path from snapshot ID.
//     b. Delete manifest directory using RemoveWithPrefix.
//     c. Delete metadata file.
//     d. Delete catalog (etcd) record.
//
// Failure handling:
//   - For PENDING snapshots, if any S3 cleanup step fails (b/c), GC will NOT
//     delete the catalog record. This keeps the snapshot eligible for retry in
//     the next GC cycle, ensuring we do not lose the ability to clean up S3
//     artifacts.
func (gc *garbageCollector) recycleSnapshots(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleSnapshots"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleSnapshots...")
	defer func() {
		log.Info(ctx, "recycleSnapshots done", mlog.Duration("timeCost", time.Since(start)))
	}()

	snapshotMeta := gc.meta.GetSnapshotMeta()
	if snapshotMeta == nil {
		log.Warn(ctx, "snapshotMeta is nil, skip recycleSnapshots")
		return
	}

	// Get pending timeout from config
	pendingTimeout := paramtable.Get().DataCoordCfg.SnapshotPendingTimeout.GetAsDuration(time.Minute)

	// Get all pending snapshots that have exceeded timeout
	pendingSnapshots, err := snapshotMeta.GetPendingSnapshots(ctx, pendingTimeout)
	if err != nil {
		log.Warn(ctx, "failed to get pending snapshots", mlog.Err(err))
		return
	}

	if len(pendingSnapshots) > 0 {
		log.Info(ctx, "found pending snapshots to cleanup", mlog.Int("count", len(pendingSnapshots)))
		cleanedCount := 0

		for _, snapshot := range pendingSnapshots {
			snapshotLog := mlog.With(
				mlog.String("snapshotName", snapshot.GetName()),
				mlog.Int64("snapshotID", snapshot.GetId()),
				mlog.Int64("collectionID", snapshot.GetCollectionId()),
			)

			gc.ackSignal(signal)
			// Compute paths from collection_id + snapshot_id
			manifestDir, metadataPath := GetSnapshotPaths(
				gc.option.cli.RootPath(),
				snapshot.GetCollectionId(),
				snapshot.GetId(),
			)

			snapshotLog.Info(ctx, "cleaning up pending snapshot",
				mlog.String("manifestDir", manifestDir),
				mlog.String("metadataPath", metadataPath))

			// Delete manifest directory using RemoveWithPrefix (no list needed)
			// This removes all segment manifest files: manifests/{snapshot_id}/*.avro
			if err := gc.option.cli.RemoveWithPrefix(ctx, manifestDir); err != nil {
				snapshotLog.Warn(ctx, "failed to remove pending snapshot manifest directory", mlog.Err(err))
				// Keep catalog record for retry in next GC cycle.
				continue
			}

			// Delete metadata file
			if err := gc.option.cli.Remove(ctx, metadataPath); err != nil {
				snapshotLog.Warn(ctx, "failed to remove pending snapshot metadata file", mlog.Err(err))
				// Keep catalog record for retry in next GC cycle.
				continue
			}

			// Delete etcd record
			if err := snapshotMeta.CleanupPendingSnapshot(ctx, snapshot); err != nil {
				snapshotLog.Warn(ctx, "failed to drop pending snapshot from catalog", mlog.Err(err))
				continue
			}

			snapshotLog.Info(ctx, "successfully cleaned up pending snapshot")
			cleanedCount++
		}

		log.Info(ctx, "pending snapshots cleanup completed",
			mlog.Int("totalPending", len(pendingSnapshots)),
			mlog.Int("cleanedCount", cleanedCount))
	}

	// Clean up DELETING snapshots (two-phase delete cleanup)
	// These are snapshots that were marked for deletion but S3 cleanup failed
	deletingSnapshots, err := snapshotMeta.GetDeletingSnapshots(ctx)
	if err != nil {
		log.Warn(ctx, "failed to get deleting snapshots", mlog.Err(err))
	} else if len(deletingSnapshots) > 0 {
		log.Info(ctx, "found deleting snapshots to cleanup", mlog.Int("count", len(deletingSnapshots)))
		deletingCleanedCount := 0

		for _, snapshot := range deletingSnapshots {
			snapshotLog := mlog.With(
				mlog.String("snapshotName", snapshot.GetName()),
				mlog.Int64("snapshotID", snapshot.GetId()),
				mlog.Int64("collectionID", snapshot.GetCollectionId()),
			)

			gc.ackSignal(signal)

			// Compute paths from collection_id + snapshot_id
			manifestDir, metadataPath := GetSnapshotPaths(
				gc.option.cli.RootPath(),
				snapshot.GetCollectionId(),
				snapshot.GetId(),
			)

			snapshotLog.Info(ctx, "cleaning up deleting snapshot",
				mlog.String("manifestDir", manifestDir),
				mlog.String("metadataPath", metadataPath))

			// Delete manifest directory
			if err := gc.option.cli.RemoveWithPrefix(ctx, manifestDir); err != nil {
				snapshotLog.Warn(ctx, "failed to remove deleting snapshot manifest directory", mlog.Err(err))
				// Continue with metadata and etcd cleanup even if S3 cleanup fails
			}

			// Delete metadata file
			if err := gc.option.cli.Remove(ctx, metadataPath); err != nil {
				snapshotLog.Warn(ctx, "failed to remove deleting snapshot metadata file", mlog.Err(err))
				// Continue with etcd cleanup even if S3 cleanup fails
			}

			// Delete etcd record
			if err := snapshotMeta.CleanupDeletingSnapshot(ctx, snapshot); err != nil {
				snapshotLog.Warn(ctx, "failed to drop deleting snapshot from catalog", mlog.Err(err))
				continue
			}

			snapshotLog.Info(ctx, "successfully cleaned up deleting snapshot")
			deletingCleanedCount++
		}

		log.Info(ctx, "deleting snapshots cleanup completed",
			mlog.Int("totalDeleting", len(deletingSnapshots)),
			mlog.Int("cleanedCount", deletingCleanedCount))
	}

	// GC fallback: Two responsibilities per collection:
	//   1. For EVERY collection with snapshot records, reap expired pin entries
	//      from SnapshotInfo to bound etcd storage growth. Orphan pins
	//      (crashed restores, swallowed Unpin errors) would otherwise accumulate
	//      forever since Pin/Unpin only touch their own entries.
	//   2. For collections whose owning collection was DROPPED, cascade-delete
	//      the orphan snapshots. Handles the case where the drop-collection
	//      cascade callback failed to fully clean up.
	activeCollectionIDs := snapshotMeta.GetActiveCollectionIDs()

	if len(activeCollectionIDs) > 0 {
		orphanCleanedCount := 0
		for _, collectionID := range activeCollectionIDs {
			gc.ackSignal(signal)

			if ctx.Err() != nil {
				log.Warn(ctx, "context canceled, stop snapshot cleanup")
				break
			}

			// Step 1: reap expired pins regardless of collection liveness.
			for _, r := range snapshotMeta.cleanExpiredPinsForCollection(ctx, collectionID) {
				setSnapshotActivePinsGauge(r.CollectionID, r.SnapshotName, r.ActivePins)
			}

			// Step 2: if the collection was dropped, cascade-delete orphan snapshots.
			timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			has, err := gc.option.broker.HasCollection(timeoutCtx, collectionID)
			cancel()
			if err != nil {
				log.Warn(ctx, "failed to check collection existence for orphan snapshot cleanup",
					mlog.Int64("collectionID", collectionID),
					mlog.Err(err))
				continue
			}
			if has {
				// Collection still exists, not an orphan — expired pins already reaped above.
				continue
			}

			log.Info(ctx, "found orphan snapshots for dropped collection, cleaning up",
				mlog.Int64("collectionID", collectionID))

			dropped, err := snapshotMeta.DropSnapshotsByCollection(ctx, collectionID)
			for _, n := range dropped {
				setSnapshotActivePinsGauge(collectionID, n, 0)
			}
			if err != nil {
				log.Warn(ctx, "failed to drop orphan snapshots for collection",
					mlog.Int64("collectionID", collectionID),
					mlog.Err(err))
				continue
			}

			log.Info(ctx, "successfully cleaned up orphan snapshots for dropped collection",
				mlog.Int64("collectionID", collectionID))
			orphanCleanedCount++
		}

		if orphanCleanedCount > 0 {
			log.Info(ctx, "orphan snapshots cleanup completed",
				mlog.Int("totalOrphanCollections", len(activeCollectionIDs)),
				mlog.Int("cleanedCount", orphanCleanedCount))
		}
	}

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}
