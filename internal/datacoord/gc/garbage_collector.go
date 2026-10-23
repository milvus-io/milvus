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

package gc

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/analyze"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
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
	Cli              storage.ChunkManager // client
	Enabled          bool                 // enable switch
	CheckInterval    time.Duration        // each interval
	MissingTolerance time.Duration        // key missing in meta tolerance time
	DropTolerance    time.Duration        // dropped segment related key tolerance time
	ScanInterval     time.Duration        // interval for scan residue for interupted log wrttien

	Broker           broker.Broker
	RemoveObjectPool *conc.Pool[struct{}]
	DataViewGC       DataViewGarbageCollector

	// importJobAlive reports whether an ImportJob still exists for jobID. It is
	// wired by the server from ImportMeta so the orphan scan never deletes an
	// active job's import_v3/{job_id}/ prefix. nil disables the import_v3 scan.
	ImportJobAlive func(ctx context.Context, jobID int64) bool

	// indexMeta, analyzeMeta and snapshotMeta are the domain metas the collector reads.
	IndexMeta    *index.IndexMeta
	AnalyzeMeta  *analyze.AnalyzeMeta
	SnapshotMeta *snapshot.SnapshotMeta
}

type DataViewGarbageCollector interface {
	GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error
}

// garbageCollector handles garbage files in object storage
// which could be dropped collection remanent or data node failure traces
type GarbageCollector struct {
	ctx    context.Context
	cancel context.CancelFunc

	Option  GcOption
	Meta    *meta.Meta
	handler meta.Handler

	startOnce        sync.Once
	stopOnce         sync.Once
	wg               sync.WaitGroup
	cmdCh            chan gcCmd
	pauseUntil       *gcPauseRecords
	PausedCollection *typeutil.ConcurrentMap[int64, *gcPauseRecords]
	controlChannels  map[string]chan gcCmd

	systemMetricsListener *hardware.SystemMetricsListener
}

type gcCmd struct {
	cmdType      datapb.GcCommand
	duration     time.Duration
	collectionID int64
	ticket       string
	done         chan error
	ctx          context.Context
	timeout      <-chan struct{}
}

type gcPauseRecord struct {
	// id uniquely identifies this record within its gcPauseRecords. Tickets are
	// not unique -- the REST route in restful_mgr_routes.go issues every pause
	// with an empty ticket -- so rollback must delete by id, not by ticket.
	id         int64
	ticket     string
	pauseUntil time.Time
}

type gcPauseRecords struct {
	mut     sync.RWMutex
	maxLen  int
	nextID  int64
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

// Insert records a pause ticket and returns the id of the record it created, so
// that a failed pause can roll back exactly its own record via DeleteByID.
func (gc *gcPauseRecords) Insert(ticket string, pauseUntil time.Time) (int64, error) {
	gc.mut.Lock()
	defer gc.mut.Unlock()

	// heap small enough, short path
	if gc.records.Len() < gc.maxLen {
		return gc.pushLocked(ticket, pauseUntil), nil
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
		return gc.pushLocked(ticket, pauseUntil), nil
	}

	// too many pause records, refresh heap
	return 0, merr.WrapErrTooManyRequests(64, "too many pause records")
}

// pushLocked appends a new record with a freshly allocated id. Caller must hold mut.
func (gc *gcPauseRecords) pushLocked(ticket string, pauseUntil time.Time) int64 {
	gc.nextID++
	gc.records.Push(gcPauseRecord{
		id:         gc.nextID,
		ticket:     ticket,
		pauseUntil: pauseUntil,
	})
	return gc.nextID
}

// Delete drops every record holding the given ticket. This is the user-facing
// resume semantic ("release my ticket"); use DeleteByID to undo a single record.
func (gc *gcPauseRecords) Delete(ticket string) {
	gc.deleteMatching(func(r gcPauseRecord) bool { return r.ticket == ticket })
}

// DeleteByID drops the single record with the given id, leaving records that
// merely share its ticket untouched.
func (gc *gcPauseRecords) DeleteByID(id int64) {
	gc.deleteMatching(func(r gcPauseRecord) bool { return r.id == id })
}

// Clear drops every record, whatever ticket it holds. It backs the ticket-less
// resume, which has no ticket to match on and means "GC must not be paused".
func (gc *gcPauseRecords) Clear() {
	gc.deleteMatching(func(gcPauseRecord) bool { return true })
}

// deleteMatching rebuilds the heap without the matching records, dropping
// already-expired records along the way.
func (gc *gcPauseRecords) deleteMatching(match func(gcPauseRecord) bool) {
	gc.mut.Lock()
	defer gc.mut.Unlock()
	now := time.Now()
	records := make([]gcPauseRecord, 0, gc.records.Len())
	for gc.records.Len() > 0 {
		record := gc.records.Pop()
		if now.Before(record.pauseUntil) && !match(record) {
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
					opt.RemoveObjectPool.Resize(1)
					listener.Context = true
				}
				return
			}
			if isSlowDown {
				mlog.Info(context.TODO(), "garbage collector slow down finished", mlog.Float64("cpuUsage", metrics.UsedRatio()))
				opt.RemoveObjectPool.Resize(paramtable.Get().DataCoordCfg.GCRemoveConcurrent.GetAsInt())
				listener.Context = false
			}
		},
	}
}

// newGarbageCollector create garbage collector with meta and option
func NewGarbageCollector(mt *meta.Meta, handler meta.Handler, opt GcOption) *GarbageCollector {
	mlog.Info(context.TODO(), "GC with option",
		mlog.Bool("enabled", opt.Enabled),
		mlog.Duration("interval", opt.CheckInterval),
		mlog.Duration("scanInterval", opt.ScanInterval),
		mlog.Duration("missingTolerance", opt.MissingTolerance),
		mlog.Duration("dropTolerance", opt.DropTolerance))
	opt.RemoveObjectPool = conc.NewPool[struct{}](Params.DataCoordCfg.GCRemoveConcurrent.GetAsInt(), conc.WithExpiryDuration(time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	metaSignal := make(chan gcCmd)
	orphanSignal := make(chan gcCmd)
	lobSignal := make(chan gcCmd)
	controlChannels := map[string]chan gcCmd{
		"meta":   metaSignal,
		"orphan": orphanSignal,
		"lob":    lobSignal,
	}
	return &GarbageCollector{
		ctx:                   ctx,
		cancel:                cancel,
		Meta:                  mt,
		handler:               handler,
		Option:                opt,
		cmdCh:                 make(chan gcCmd),
		systemMetricsListener: newSystemMetricsListener(&opt),
		pauseUntil:            NewGCPauseRecords(),
		PausedCollection:      typeutil.NewConcurrentMap[int64, *gcPauseRecords](),
		controlChannels:       controlChannels,
	}
}

// start a goroutine and perform gc check every `checkInterval`
func (gc *GarbageCollector) Start() {
	if gc.Option.Enabled {
		if gc.Option.Cli == nil {
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
func (gc *GarbageCollector) GetStatus() GcStatus {
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

func (gc *GarbageCollector) Pause(ctx context.Context, collectionID int64, ticket string, pauseDuration time.Duration) error {
	if !gc.Option.Enabled {
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
		ctx:          ctx,
		timeout:      ctx.Done(),
	}:
		return <-done
	case <-ctx.Done():
		return ctx.Err()
	case <-gc.ctx.Done():
		// cmdCh is unbuffered and startControlLoop has already returned, so the
		// send above can never be received; without this arm the caller parks
		// until its own ctx is canceled (forever for a Done()-less ctx).
		return merr.WrapErrServiceUnavailable("garbage collector is closing")
	}
}

func (gc *GarbageCollector) Resume(ctx context.Context, collectionID int64, ticket string) error {
	if !gc.Option.Enabled {
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
	case <-gc.ctx.Done():
		// see Pause: the control loop is gone, nothing will receive this send.
		return merr.WrapErrServiceUnavailable("garbage collector is closing")
	}
}

// work contains actual looping check logic
func (gc *GarbageCollector) work(ctx context.Context) {
	// TODO: fast cancel for gc when closing.
	// Run gc tasks in parallel.
	gc.wg.Add(4)
	go func() {
		defer gc.wg.Done()
		gc.runRecycleTaskWithPauser(ctx, "meta", gc.Option.CheckInterval, func(ctx context.Context, signal <-chan gcCmd) {
			gc.recycleDataViews(ctx, signal)
			gc.RecycleDroppedSegments(ctx, signal)
			gc.RecycleChannelCPMeta(ctx, signal)
			gc.recycleUnusedIndexes(ctx, signal)
			gc.RecycleUnusedSegIndexes(ctx, signal)
			gc.RecycleUnusedAnalyzeFiles(ctx, signal)
			gc.RecycleUnusedTextIndexFiles(ctx, signal)
			gc.RecycleUnusedJSONIndexFiles(ctx, signal)
			gc.RecycleUnusedJSONStatsFiles(ctx, signal)
			gc.recycleSnapshots(ctx, signal)
		})
	}()
	go func() {
		defer gc.wg.Done()
		gc.runRecycleTaskWithPauser(ctx, "orphan", gc.Option.ScanInterval, func(ctx context.Context, signal <-chan gcCmd) {
			// orphan file not controlled by collection level pause for now
			gc.RecycleUnusedBinlogFiles(ctx)
			gc.RecycleUnusedIndexFilesV0(ctx)
			gc.recycleUnusedIndexFilesV1(ctx)
			gc.recycleUnusedImportV3Prefixes(ctx)
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

func (gc *GarbageCollector) ackSignal(signal <-chan gcCmd) {
	select {
	case cmd := <-signal:
		if cmd.done != nil {
			close(cmd.done)
		}
	default:
	}
}

// startControlLoop start a control loop for garbageCollector.
func (gc *GarbageCollector) startControlLoop(_ context.Context) {
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

func (gc *GarbageCollector) pause(cmd gcCmd) error {
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
	var recordID int64
	if cmd.collectionID <= 0 { // legacy pause all
		recordID, err = gc.pauseUntil.Insert(cmd.ticket, reqPauseUntil)
		log.Info(gc.ctx, "global pause ticket recorded")
	} else {
		curr, has := gc.PausedCollection.Get(cmd.collectionID)
		if !has {
			curr = NewGCPauseRecords()
			gc.PausedCollection.Insert(cmd.collectionID, curr)
		}
		recordID, err = curr.Insert(cmd.ticket, reqPauseUntil)
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
		select {
		case <-signal.done:
		case <-cmd.timeout:
			gc.rollbackPause(cmd, recordID)
			return cmd.ctx.Err()
		}
	case <-cmd.timeout:
		// timeout, resume the pause
		gc.rollbackPause(cmd, recordID)
		return cmd.ctx.Err()
	case <-gc.ctx.Done():
		// The collector is closing. The meta worker may already have returned on
		// its own ctx.Done(), and signalCh is unbuffered, so the send above would
		// never be received: bound the wait by the collector's lifetime instead of
		// parking this control-loop goroutine and hanging gc.wg.Wait() in close().
		gc.rollbackPause(cmd, recordID)
		return merr.WrapErrServiceUnavailable("garbage collector is closing")
	}
	return nil
}

// rollbackPause undoes exactly the record this pause() call inserted. It must not
// go through resume(), whose delete is ticket-scoped: tickets are not unique --
// every pause issued by the REST route in restful_mgr_routes.go carries an empty
// ticket -- so a ticket-scoped delete would also drop a concurrent caller's
// still-valid pause record and resume GC while that caller believes it is paused.
func (gc *GarbageCollector) rollbackPause(cmd gcCmd, recordID int64) {
	if cmd.collectionID <= 0 {
		gc.pauseUntil.DeleteByID(recordID)
	} else if curr, has := gc.PausedCollection.Get(cmd.collectionID); has {
		curr.DeleteByID(recordID)
		if curr.Len() == 0 || time.Now().After(curr.PauseUntil()) {
			gc.PausedCollection.Remove(cmd.collectionID)
		}
	}
	mlog.Info(gc.ctx, "pause rolled back",
		mlog.Int64("collectionID", cmd.collectionID),
		mlog.String("ticket", cmd.ticket))
}

func (gc *GarbageCollector) resume(cmd gcCmd) {
	// A resume carrying no ticket is the pre collection level GC control
	// semantic: release every outstanding pause. Pauses issued through the proxy
	// route always carry a generated ticket, so a ticket-scoped delete would
	// match nothing here and leave GC paused while reporting success.
	if cmd.ticket == "" {
		gc.pauseUntil.Clear()
		gc.PausedCollection.Range(func(collectionID int64, _ *gcPauseRecords) bool {
			gc.PausedCollection.Remove(collectionID)
			return true
		})
		mlog.Info(gc.ctx, "garbage collection resumed", mlog.Bool("stillPaused", false))
		return
	}

	// reset to zero value
	var afterResume time.Time
	if cmd.collectionID <= 0 {
		gc.pauseUntil.Delete(cmd.ticket)
		afterResume = gc.pauseUntil.PauseUntil()
	} else {
		curr, has := gc.PausedCollection.Get(cmd.collectionID)
		if has {
			curr.Delete(cmd.ticket)
			afterResume = curr.PauseUntil()
			if curr.Len() == 0 || time.Now().After(afterResume) {
				gc.PausedCollection.Remove(cmd.collectionID)
			}
		}
	}
	stillPaused := time.Now().Before(afterResume)
	mlog.Info(gc.ctx, "garbage collection resumed", mlog.Bool("stillPaused", stillPaused))
}

// runRecycleTaskWithPauser is a helper function to create a task with pauser
func (gc *GarbageCollector) runRecycleTaskWithPauser(ctx context.Context, name string, interval time.Duration, task func(ctx context.Context, signal <-chan gcCmd)) {
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

func (gc *GarbageCollector) collectionGCPaused(collectionID int64) bool {
	collPauseUntil, has := gc.PausedCollection.Get(collectionID)
	return has && time.Now().Before(collPauseUntil.PauseUntil())
}

// close stop the garbage collector.
func (gc *GarbageCollector) Close() {
	gc.stopOnce.Do(func() {
		gc.cancel()
		gc.wg.Wait()
		if gc.Option.RemoveObjectPool != nil {
			gc.Option.RemoveObjectPool.Release()
		}
	})
}

// recycleUnusedBinlogFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *GarbageCollector) RecycleUnusedBinlogFiles(ctx context.Context) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedBinlogFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedBinlogFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedBinlogFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	type scanTask struct {
		prefix            string
		checker           func(objectInfo *storage.ChunkObjectInfo, segment *meta.SegmentInfo) bool
		segmentIDFromPath func(rootPath, filePath string) (int64, error)
		label             string
	}
	scanTasks := []scanTask{
		{
			prefix: path.Join(gc.Option.Cli.RootPath(), common.SegmentInsertLogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *meta.SegmentInfo) bool {
				return segment != nil
			},
			segmentIDFromPath: storage.ParseSegmentIDByBinlog,
			label:             metrics.InsertFileLabel,
		},
		{
			prefix: path.Join(gc.Option.Cli.RootPath(), common.SegmentStatslogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *meta.SegmentInfo) bool {
				if segment == nil {
					return false
				}
				// Import V3 pre-registers the segment as Importing but persists
				// Statslogs only at acceptance; the PK stats file already sits
				// under stats_log/ while the task retries. Keep it until the
				// segment leaves Importing, or acceptance would persist a log
				// ID whose file was already recycled.
				if segment.GetIsImporting() {
					return true
				}
				logID, err := binlog.GetLogIDFromBingLogPath(objectInfo.FilePath)
				if err != nil {
					log.Warn(ctx, "garbageCollector find dirty stats log", mlog.String("filePath", objectInfo.FilePath), mlog.Err(err))
					return false
				}
				return segment.IsStatsLogExists(logID)
			},
			segmentIDFromPath: storage.ParseSegmentIDByBinlog,
			label:             metrics.StatFileLabel,
		},
		{
			prefix: path.Join(gc.Option.Cli.RootPath(), common.SegmentBm25LogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *meta.SegmentInfo) bool {
				if segment == nil {
					return false
				}
				for _, fieldLogs := range segment.GetBm25Statslogs() {
					for _, statsLog := range fieldLogs.GetBinlogs() {
						key := statsLog.GetLogPath()
						if key == "" {
							// Catalog binlogs carry IDs, not paths. Match the field too:
							// compound stats reuse the same log ID across fields.
							key = metautil.BuildBm25LogPath(gc.Option.Cli.RootPath(),
								segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(),
								fieldLogs.GetFieldID(), statsLog.GetLogID())
						}
						if key == objectInfo.FilePath {
							return true
						}
					}
				}
				return false
			},
			segmentIDFromPath: storage.ParseSegmentIDByBinlog,
			label:             common.SegmentBm25LogPath,
		},
		{
			prefix: path.Join(gc.Option.Cli.RootPath(), common.SegmentDeltaLogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *meta.SegmentInfo) bool {
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
			prefix: path.Join(gc.Option.Cli.RootPath(), common.TextIndexPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *meta.SegmentInfo) bool {
				if segment == nil {
					return false
				}
				_, ok := getTextLogPaths(segment, gc.Option.Cli.RootPath())[objectInfo.FilePath]
				return ok
			},
			segmentIDFromPath: parseSegmentIDFromTextIndexPath,
			label:             common.TextIndexPath,
		},
		{
			prefix: path.Join(gc.Option.Cli.RootPath(), common.JSONStatsPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *meta.SegmentInfo) bool {
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
			prefix: path.Join(gc.Option.Cli.RootPath(), common.JSONIndexPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *meta.SegmentInfo) bool {
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

// recycleUnusedImportV3Prefixes removes import_v3/{job_id}/ prefixes that have
// no live ImportJob in the catalog and whose newest object is older than the
// orphan tolerance. It runs on the orphan scan cadence
// (dataCoord.gc.scanInterval, default 7 days); importJobAlive is optional and
// when nil the scan is skipped entirely. The import subsystem itself removes a
// terminal job's prefix in deleteImportJob; this scan only covers abnormal
// leftovers (crashed/aborted creations, late old-run writers after GC).
func (gc *GarbageCollector) recycleUnusedImportV3Prefixes(ctx context.Context) {
	if gc.Option.Cli == nil || gc.Option.ImportJobAlive == nil {
		return
	}
	root := path.Join(gc.Option.Cli.RootPath(), metautil.ImportV3RootPath) + "/"
	paths, modTimes, err := storage.ListAllChunkWithPrefix(ctx, gc.Option.Cli, root, true)
	if err != nil {
		mlog.Warn(ctx, "failed to list import v3 orphan prefixes", mlog.Err(err))
		return
	}
	type jobInfo struct {
		newest time.Time
	}
	jobs := make(map[int64]*jobInfo)
	for i, path := range paths {
		// Chunk managers do not agree on path shape: RemoteChunkManager returns
		// bucket-relative object keys (import_v3/{job}/...), while LocalChunkManager
		// returns absolute filesystem paths rooted above the storage root. Locate
		// the root component anywhere in the path instead of assuming a fixed prefix.
		marker := metautil.ImportV3RootPath + "/"
		idx := strings.Index(path, marker)
		if idx < 0 {
			continue
		}
		rel := path[idx+len(marker):]
		parts := strings.SplitN(rel, "/", 2)
		if len(parts) == 0 || parts[0] == "" {
			continue
		}
		jobID, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}
		info := jobs[jobID]
		if info == nil {
			info = &jobInfo{newest: modTimes[i]}
			jobs[jobID] = info
		} else if modTimes[i].After(info.newest) {
			info.newest = modTimes[i]
		}
	}
	for jobID, info := range jobs {
		if gc.Option.ImportJobAlive(ctx, jobID) {
			continue
		}
		// Only delete when nothing has been written to the prefix for at least
		// missingTolerance: a job that just lost its catalog record but is still
		// being written by a late worker must not have its fragments removed.
		if time.Since(info.newest) <= gc.Option.MissingTolerance {
			continue
		}
		prefix := path.Join(gc.Option.Cli.RootPath(), metautil.BuildImportV3JobPath(jobID)) + "/"
		if err := gc.Option.Cli.RemoveWithPrefix(ctx, prefix); err != nil {
			mlog.Warn(ctx, "remove orphan import v3 prefix failed",
				mlog.Int64("jobID", jobID), mlog.Err(err))
			continue
		}
		mlog.Info(ctx, "removed orphan import v3 prefix",
			mlog.Int64("jobID", jobID), mlog.String("prefix", prefix))
	}
}

// recycleUnusedBinLogWithChecker scans the prefix and checks the path with checker.
// GC the file if checker returns false.
func (gc *GarbageCollector) recycleUnusedBinLogWithChecker(ctx context.Context, prefix string, label string, segmentIDFromPath func(rootPath, filePath string) (int64, error), checker func(objectInfo *storage.ChunkObjectInfo, segment *meta.SegmentInfo) bool) {
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
	snapshotMeta := gc.Option.SnapshotMeta
	isSnapshotProtected := func(segmentID, collectionID int64) bool {
		if snapshotMeta == nil {
			return false
		}
		return snapshotMeta.IsSegmentGCBlocked(collectionID, segmentID)
	}

	futures := make([]*conc.Future[struct{}], 0)
	err := gc.Option.Cli.WalkWithPrefix(ctx, prefix, true, func(chunkInfo *storage.ChunkObjectInfo) bool {
		total++
		lastFilePath = chunkInfo.FilePath

		// Check file tolerance first to avoid unnecessary operation.
		if time.Since(chunkInfo.ModifyTime) <= gc.Option.MissingTolerance {
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles skip file since it is not expired", mlog.String("filePath", chunkInfo.FilePath), mlog.Time("modifyTime", chunkInfo.ModifyTime))
			return true
		}

		// Parse segmentID from file path.
		// TODO: Does all files in the same segment have the same segmentID?
		segmentID, err := segmentIDFromPath(gc.Option.Cli.RootPath(), chunkInfo.FilePath)
		if err != nil {
			// Try V3 path format: insert_log/{coll}/{part}/{seg}/...
			v3SegID, parseErr := parseV3SegmentID(gc.Option.Cli.RootPath(), chunkInfo.FilePath)
			if parseErr != nil {
				unexpectedFailure.Inc()
				logger.Warn(ctx, "garbageCollector recycleUnusedBinlogFiles parse segment id error",
					mlog.String("filePath", chunkInfo.FilePath),
					mlog.Err(err))
				return true
			}
			if v3Seg := gc.Meta.GetSegment(ctx, v3SegID); v3Seg != nil {
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

		segment := gc.Meta.GetSegment(ctx, segmentID)

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

		future := gc.Option.RemoveObjectPool.Submit(func() (struct{}, error) {
			logger := logger.With(mlog.String("file", file))
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles remove file...")

			if err = gc.Option.Cli.Remove(ctx, file); err != nil {
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

func (gc *GarbageCollector) recycleDataViews(ctx context.Context, signal <-chan gcCmd) {
	if gc.Meta == nil || gc.Option.DataViewGC == nil {
		return
	}
	start := time.Now()
	logger := mlog.With(mlog.String("gcName", "recycleDataViews"), mlog.Time("startAt", start))
	logger.Info(ctx, "start recycleDataViews")
	defer func() { logger.Info(ctx, "recycleDataViews done", mlog.Duration("timeCost", time.Since(start))) }()

	for _, collection := range gc.Meta.GetCollections() {
		if ctx.Err() != nil {
			return
		}
		gc.ackSignal(signal)

		collectionID := collection.ID
		if gc.collectionGCPaused(collectionID) {
			logger.Info(ctx, "skip DataView GC since collection is paused", mlog.FieldCollectionID(collectionID))
			continue
		}

		if err := gc.Option.DataViewGC.GarbageCollect(ctx, collectionID, 1); err != nil {
			logger.Warn(ctx, "DataView GC failed", mlog.FieldCollectionID(collectionID), mlog.Err(err))
		}
	}
}

func (gc *GarbageCollector) checkDroppedSegmentGC(segment *meta.SegmentInfo,
	childSegment *meta.SegmentInfo,
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

	dmlTs := meta.SegmentEffectiveDmlTs(segment.SegmentInfo)
	if dmlTs <= cpTimestamp {
		return true
	}

	// A removed channel no longer needs checkpoint protection. Its checkpoint
	// may stop advancing, while collection cleanup waits for segment GC.
	segInsertChannel := segment.GetInsertChannel()
	channelExists, err := gc.Meta.MetaStore.ChannelExists(gc.ctx, segInsertChannel)
	if err != nil {
		log.RatedWarn(gc.ctx, rate.Limit(60), "failed to check channel existence, skip dropped segment GC",
			mlog.FieldVChannel(segInsertChannel), mlog.Err(err))
		return false
	}
	if channelExists {
		log.RatedInfo(gc.ctx, rate.Limit(60), "dropped segment dml position after channel cp, skip meta gc",
			mlog.Uint64("dmlPosTs", dmlTs),
			mlog.Uint64("channelCpTs", cpTimestamp),
		)
		return false
	}
	return true
}

// recycleDroppedSegments scans all segments and remove those dropped segments from meta and oss.
func (gc *GarbageCollector) RecycleDroppedSegments(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleDroppedSegments"), mlog.Time("startAt", start))
	log.Info(ctx, "start clear dropped segments...")
	defer func() {
		log.Info(ctx, "clear dropped segments done", mlog.Duration("timeCost", time.Since(start)))
	}()

	all := gc.Meta.SelectSegments(ctx)
	drops := make(map[int64]*meta.SegmentInfo, 0)
	compactTo := make(map[int64]*meta.SegmentInfo)
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

	droppedCompactTo := make(map[int64]*meta.SegmentInfo)
	for id := range drops {
		if to, ok := compactTo[id]; ok {
			droppedCompactTo[to.GetID()] = to
		}
	}
	indexedSegments := index.FilterInIndexedSegments(ctx, gc.handler, gc.Option.IndexMeta, true, lo.Values(droppedCompactTo)...)
	if ctx.Err() != nil {
		return
	}
	indexedSet := make(typeutil.UniqueSet)
	for _, segment := range indexedSegments {
		indexedSet.Insert(segment.GetID())
	}

	channelCPs := make(map[string]uint64)
	for channel := range channels {
		pos := gc.Meta.GetChannelCheckpoint(channel)
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

	io := packed.NewManifestIOContext(1)
	defer io.Close()
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
		if snapshotMeta := gc.Option.SnapshotMeta; snapshotMeta != nil {
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

		gc.RecycleDroppedSegment(ctx, io, segmentID, segment)
	}
}

// recycleDroppedSegment marks its index tasks deleted before removing a
// dropped segment's files, segment-index records, and finally segment record.
//
// Persisted task markers keep a retained compaction ancestor from appearing
// ready after partial file cleanup or a failed catalog deletion, including
// across restart. Keep the marked records and their file keys until cleanup
// completes so subsequent GC cycles can retry the same files.
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
func (gc *GarbageCollector) RecycleDroppedSegment(ctx context.Context, io *packed.ManifestIOContext, segmentID int64, segment *meta.SegmentInfo) {
	log := mlog.With(mlog.Int64("segmentID", segmentID), mlog.Int64("collectionID", segment.GetCollectionID()))

	if ctx.Err() != nil {
		return
	}

	segIndexes, indexFiles, blockReason := gc.GetDroppedSegmentIndexFiles(ctx, io, segmentID)
	switch blockReason {
	case gcBlockedBySnapshot:
		log.Info(ctx, "skip GC segment since segment index is protected by snapshot",
			mlog.Int("segmentIndexes", len(segIndexes)))
		return
	case gcBlockedByManifest:
		// The class-specific message (transient read failure vs invalid
		// entry) is logged where the manifest error is classified.
		return
	}

	if err := gc.markSegmentIndexesDeleted(ctx, segIndexes); err != nil {
		log.Warn(ctx, "GC segment index tasks failed to mark deleted, wait to retry", mlog.Err(err))
		return
	}

	cloned := segment.Clone()
	if err := gc.RemoveDroppedSegmentFiles(ctx, cloned, indexFiles); err != nil {
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

	if err := gc.Meta.DropSegment(ctx, cloned.GetID()); err != nil {
		log.Warn(ctx, "GC segment meta failed to drop segment", mlog.Err(err))
		return
	}
	log.Info(ctx, "GC segment meta drop segment done", mlog.Int("segmentIndexes", len(segIndexes)))
}

// gcBlockReason says why getDroppedSegmentIndexFiles refused to release a
// dropped segment for recycling, so the caller reports the actual cause
// instead of folding every block into "protected by snapshot".
type GcBlockReason int

const (
	GcNotBlocked GcBlockReason = iota
	// gcBlockedBySnapshot: a snapshot still references one of the segment's
	// index buildIDs, from a SegmentIndex record or a manifest entry.
	gcBlockedBySnapshot
	// gcBlockedByManifest: the manifest could not be read, or carries an
	// entry that fails validation, so the full delete list is unknown.
	gcBlockedByManifest
)

func (gc *GarbageCollector) GetDroppedSegmentIndexFiles(ctx context.Context, io *packed.ManifestIOContext, segmentID int64) ([]*model.SegmentIndex, map[string]struct{}, GcBlockReason) {
	segIndexes := gc.getAllSegmentIndexesForDroppedSegment(segmentID)
	if snapshotMeta := gc.Option.SnapshotMeta; snapshotMeta != nil {
		for _, segIdx := range segIndexes {
			if snapshotMeta.IsBuildIDGCBlocked(segIdx.CollectionID, segIdx.BuildID) {
				return segIndexes, nil, gcBlockedBySnapshot
			}
		}
	}
	indexFiles := make(map[string]struct{}, len(segIndexes))
	for _, segIdx := range segIndexes {
		for key := range gc.GetAllIndexFilesOfIndex(segIdx) {
			indexFiles[key] = struct{}{}
		}
	}

	// A dropped StorageV3 segment can hold both kinds of index metadata at
	// once - a manifest-only entry published while the etcd write switch was
	// off, and a record-only one whose build finished after the drop - so the
	// delete list is always the union of records and manifest, mirroring
	// recycleUnusedSegIndexes.
	segment := gc.Meta.GetSegment(ctx, segmentID)
	manifestIndexFiles, blocked, err := gc.getManifestIndexFiles(ctx, io, segment)
	if err != nil {
		if errors.Is(err, errManifestIndexEntryInvalid) {
			// Deterministic: re-reading the same manifest cannot clear it,
			// and recycling anyway would leak whatever the entry names.
			metrics.GarbageCollectorInvalidManifestCount.WithLabelValues(paramtable.GetStringNodeID()).Inc()
			mlog.Error(ctx, "dropped segment manifest carries an invalid index entry; GC for this segment is blocked until the manifest is repaired",
				mlog.FieldSegmentID(segmentID),
				mlog.String("manifestPath", segment.GetManifestPath()),
				mlog.Err(err))
			return segIndexes, nil, gcBlockedByManifest
		}
		// An unreadable manifest may still reference artifacts that live
		// outside the segment directory, and no scan reclaims those once the
		// segment record is gone. Wait for the next cycle instead - unless the
		// manifest file itself is gone, in which case it is not unreadable but
		// absent, there is nothing left to protect, and blocking would strand
		// this segment's metadata forever.
		if gc.manifestExists(ctx, segment) {
			mlog.Warn(ctx, "failed to read dropped segment manifest index files, blocking GC, will retry",
				mlog.FieldSegmentID(segmentID), mlog.Err(err))
			return segIndexes, nil, gcBlockedByManifest
		}
		mlog.Info(ctx, "dropped segment manifest is already gone, recycling without manifest index metadata",
			mlog.FieldSegmentID(segmentID), mlog.Err(err))
	}
	if blocked {
		return segIndexes, nil, gcBlockedBySnapshot
	}
	for file := range manifestIndexFiles {
		indexFiles[file] = struct{}{}
	}
	if len(indexFiles) == 0 {
		indexFiles = nil
	}
	return segIndexes, indexFiles, GcNotBlocked
}

// manifestExists reports whether the segment's manifest revision is still in
// object storage. The FFI collapses every manifest read failure into one
// transient class, so this is what separates "the manifest could not be read
// right now" from "the manifest is gone". Anything other than a definitive
// "absent" answer counts as present: under doubt, retry the segment later.
func (gc *GarbageCollector) manifestExists(ctx context.Context, segment *meta.SegmentInfo) bool {
	manifestFilePath, err := packed.ManifestFilePath(segment.GetManifestPath())
	if err != nil {
		return true
	}
	exists, err := gc.Option.Cli.Exist(ctx, manifestFilePath)
	if err != nil {
		mlog.Warn(ctx, "failed to check dropped segment manifest existence",
			mlog.FieldSegmentID(segment.GetID()),
			mlog.String("manifestFilePath", manifestFilePath), mlog.Err(err))
		return true
	}
	return exists
}

// errManifestIndexEntryInvalid classifies a manifest index entry that fails
// manifestIndexFilePathInfo validation. Unlike a read failure this is
// deterministic: retrying the read cannot clear it, only a repaired manifest
// revision can.
var errManifestIndexEntryInvalid = errors.New("manifest carries an invalid index entry")

// getManifestIndexFiles returns the object-storage paths of every completed
// index artifact a StorageV3 segment's manifest still references. Any read or
// validation failure is returned so callers can fail closed; a validation
// rejection wraps errManifestIndexEntryInvalid so callers can tell "retry the
// read" apart from "the manifest needs repair".
func (gc *GarbageCollector) getManifestIndexFiles(ctx context.Context, io *packed.ManifestIOContext, segment *meta.SegmentInfo) (map[string]struct{}, bool, error) {
	if segment == nil || segment.GetStorageVersion() != storage.StorageV3 || segment.GetManifestPath() == "" || !segment.GetManifestHasIndex() {
		return nil, false, nil
	}
	manifestIndexes, err := gc.Meta.ReadManifestIndexesWithIO(ctx, io, segment.GetManifestPath(), meta.CreateStorageConfig())
	if err != nil {
		return nil, false, merr.Wrap(err, "failed to read manifest index metadata")
	}
	files := make(map[string]struct{})
	for _, manifestIndex := range manifestIndexes {
		if snapshotMeta := gc.Option.SnapshotMeta; snapshotMeta != nil &&
			snapshotMeta.IsBuildIDGCBlocked(segment.GetCollectionID(), manifestIndex.BuildID) {
			return nil, true, nil
		}
		info, ok := meta.ManifestIndexFilePathInfoForSegment(gc.Option.Cli.RootPath(), segment.SegmentInfo, manifestIndex)
		if !ok {
			return nil, false, errors.Wrapf(errManifestIndexEntryInvalid,
				"segment %d index %d build %d", segment.GetID(), manifestIndex.IndexID, manifestIndex.BuildID)
		}
		for _, file := range info.GetIndexFilePaths() {
			files[file] = struct{}{}
		}
	}
	return files, false, nil
}

// markSegmentIndexesDeleted persists every task's deletion marker before any
// segment artifacts are removed. A failure leaves all files for the next GC
// cycle; markers already persisted remain authoritative and are idempotent.
func (gc *GarbageCollector) markSegmentIndexesDeleted(ctx context.Context, segIndexes []*model.SegmentIndex) error {
	for _, segIdx := range segIndexes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := gc.Option.IndexMeta.DeleteTask(segIdx.BuildID); err != nil {
			mlog.Warn(ctx, "failed to mark segment index task deleted before GC",
				mlog.FieldSegmentID(segIdx.SegmentID), mlog.FieldBuildID(segIdx.BuildID), mlog.Err(err))
			return err
		}
	}
	return nil
}

// getAllSegmentIndexesForDroppedSegment wraps indexMeta.GetAllSegmentIndexes
// with a defensive nil guard. Production initGarbageCollection always wires indexMeta, but
// the guard is cheap and turns any unexpected nil into "no index records"
// instead of a panic during a GC sweep — keeping a single misbuilt gc
// instance from taking the whole datacoord down.
func (gc *GarbageCollector) getAllSegmentIndexesForDroppedSegment(segmentID int64) []*model.SegmentIndex {
	if gc.Option.IndexMeta == nil {
		return nil
	}
	return gc.Option.IndexMeta.GetAllSegmentIndexes(segmentID)
}

// v3SegmentGCPrefix accepts canonical and legacy namespaces beneath the root,
// but only for this segment. Remote keys retain their literal spelling.
func (gc *GarbageCollector) v3SegmentGCPrefix(basePath string, segment *meta.SegmentInfo) (string, error) {
	root := path.Clean(gc.Option.Cli.RootPath())
	_, local := gc.Option.Cli.(*storage.LocalChunkManager)
	if path.IsAbs(basePath) != local {
		return "", merr.WrapErrDataIntegrityMsg("GC V3 segment %d has an invalid storage path %q", segment.GetID(), basePath)
	}
	basePath = strings.TrimRight(basePath, "/")
	if local {
		basePath = path.Clean(basePath)
		rel, err := filepath.Rel(root, basePath)
		if err != nil || rel == "." || !filepath.IsLocal(rel) {
			return "", merr.WrapErrDataIntegrityMsg("GC V3 segment %d path %q is outside storage root %q", segment.GetID(), basePath, root)
		}
	} else if root != "." && (basePath == root || !strings.HasPrefix(basePath, root+"/")) {
		return "", merr.WrapErrDataIntegrityMsg("GC V3 segment %d path %q is outside storage root %q", segment.GetID(), basePath, root)
	}

	if !meta.SegmentBaseMatches(basePath, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID()) {
		return "", merr.WrapErrDataIntegrityMsg("GC V3 segment %d manifest base path does not match segment identity", segment.GetID())
	}
	// Keep segment 2001 from matching sibling 20010 during prefix deletion.
	return basePath + "/", nil
}

func (gc *GarbageCollector) RemoveDroppedSegmentFiles(ctx context.Context, cloned *meta.SegmentInfo, indexFiles map[string]struct{}) error {
	log := mlog.With(mlog.Int64("segmentID", cloned.GetID()))

	// V3 segment data lives under the manifest base path. Segment index files
	// still live under index file prefixes outside it, while the manifest that
	// names them lives inside it: index files go first, so a partial deletion
	// failure leaves the manifest for the next cycle to re-derive the
	// survivors from (re-deleting removed keys is a no-op - removeObjectFiles
	// swallows NotFound), and the basePath prefix goes last.
	if cloned.GetStorageVersion() == storage.StorageV3 {
		// An empty ManifestPath means the segment was allocated but never
		// accepted (import retry, failure, or cleanup), so there are no data
		// files to remove; still delete any recorded index files, then proceed
		// to the meta drop instead of erroring.
		if cloned.GetManifestPath() == "" {
			if len(indexFiles) == 0 {
				log.Info(ctx, "GC V3 segment files done")
				return nil
			}
			if err := gc.RemoveObjectFiles(ctx, indexFiles); err != nil {
				log.Warn(ctx, "GC V3 segment remove index files failed", mlog.Err(err))
				return err
			}
			log.Info(ctx, "GC V3 segment files done")
			return nil
		}
		basePath, _, err := packed.UnmarshalManifestPath(cloned.GetManifestPath())
		if err != nil {
			log.Warn(ctx, "GC V3 segment failed to parse manifest path",
				mlog.String("manifestPath", cloned.GetManifestPath()),
				mlog.Err(err))
			return err
		}
		basePath, err = gc.v3SegmentGCPrefix(basePath, cloned)
		if err != nil {
			return err
		}
		log.Info(ctx, "GC V3 segment start, removing basePath...",
			mlog.String("basePath", basePath),
			mlog.Int("indexFiles", len(indexFiles)))
		if err := gc.RemoveObjectFiles(ctx, indexFiles); err != nil {
			log.Warn(ctx, "GC V3 segment remove index files failed", mlog.Err(err))
			return err
		}
		prefix := strings.TrimSuffix(basePath, "/") + "/"
		if err := gc.Option.Cli.RemoveWithPrefix(ctx, prefix); err != nil && !copysegment.IsAbsentLocalPrefix(gc.Option.Cli, prefix, err) {
			log.Warn(ctx, "GC V3 segment remove basePath failed",
				mlog.String("basePath", basePath),
				mlog.Err(err))
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
	if err := gc.RemoveObjectFiles(ctx, logs); err != nil {
		log.Warn(ctx, "GC segment remove logs failed", mlog.Err(err))
		return err
	}
	return nil
}

func (gc *GarbageCollector) removeDroppedSegmentIndexMeta(ctx context.Context, segIndexes []*model.SegmentIndex) error {
	if len(segIndexes) == 0 {
		return nil
	}
	for _, segIdx := range segIndexes {
		if err := gc.Option.IndexMeta.RemoveSegmentIndex(ctx, segIdx.BuildID); err != nil {
			return err
		}
	}
	return nil
}

func (gc *GarbageCollector) RecycleChannelCPMeta(ctx context.Context, signal <-chan gcCmd) {
	channelCPs, err := gc.Meta.Catalog.ListChannelCheckpoint(ctx)
	if err != nil {
		mlog.Warn(ctx, "recycleChannelCPMeta: failed to list channel checkpoints", mlog.Err(err))
		return
	}

	collectionID2GcStatus := make(map[int64]bool)
	availability, _ := gc.Option.Broker.(broker.CollectionAvailability)
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

		if ctx.Err() != nil {
			return
		}
		// A positive resident lookup avoids schema conversion, task scheduling,
		// per-collection timeouts and retaining live IDs for the entire sweep.
		if availability != nil && availability.IsCollectionAvailable(collectionID) {
			skippedCnt++
			continue
		}

		_, ok := collectionID2GcStatus[collectionID]
		if !ok {
			timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			has, err := gc.Option.Broker.HasCollection(timeoutCtx, collectionID)
			cancel()
			if err == nil && !has {
				collectionID2GcStatus[collectionID] = gc.Meta.MetaStore.GcConfirm(ctx, collectionID, -1)
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

		err := gc.Meta.DropChannelCheckpoint(vChannel)
		if err != nil {
			// Try to GC in the next gc cycle if drop channel cp meta fail.
			mlog.Warn(ctx, "failed to drop channelcp check point during gc", mlog.String("vchannel", vChannel), mlog.Err(err))
		} else {
			mlog.Info(ctx, "GC channel cp", mlog.String("vchannel", vChannel))
		}
	}

	mlog.Info(ctx, "GC channel cp done", mlog.Int("skippedChannelCP", skippedCnt))
}

func (gc *GarbageCollector) isExpire(dropts Timestamp) bool {
	droptime := time.Unix(0, int64(dropts))
	return time.Since(droptime) > gc.Option.DropTolerance
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

func getLogs(sinfo *meta.SegmentInfo) map[string]struct{} {
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

func getTextLogs(sinfo *meta.SegmentInfo) map[string]struct{} {
	return getTextLogPaths(sinfo, "")
}

func getTextLogPaths(sinfo *meta.SegmentInfo, rootPath string) map[string]struct{} {
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

func getJSONKeyLogs(sinfo *meta.SegmentInfo, gc *GarbageCollector) map[string]struct{} {
	jsonkeyLogs := make(map[string]struct{})
	for _, flog := range sinfo.GetJsonKeyStats() {
		for _, file := range flog.GetFiles() {
			var prefix string
			if flog.GetJsonKeyStatsDataFormat() >= 2 {
				prefix = metautil.BuildJSONKeyStatsPrefix(
					gc.Option.Cli.RootPath(),
					flog.GetJsonKeyStatsDataFormat(),
					flog.GetBuildID(),
					flog.GetVersion(),
					sinfo.GetCollectionID(),
					sinfo.GetPartitionID(),
					sinfo.GetID(),
					flog.GetFieldID(),
				)
			} else {
				prefix = fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d", gc.Option.Cli.RootPath(), common.JSONIndexPath,
					flog.GetBuildID(), flog.GetVersion(), sinfo.GetCollectionID(), sinfo.GetPartitionID(), sinfo.GetID(), flog.GetFieldID())
			}
			file = path.Join(prefix, file)
			jsonkeyLogs[file] = struct{}{}
		}
	}

	return jsonkeyLogs
}

// removeObjectFiles remove file from oss storage, return error if any log failed to remove.
func (gc *GarbageCollector) RemoveObjectFiles(ctx context.Context, filePaths map[string]struct{}) error {
	futures := make([]*conc.Future[struct{}], 0)
	for filePath := range filePaths {
		filePath := filePath
		future := gc.Option.RemoveObjectPool.Submit(func() (struct{}, error) {
			err := gc.Option.Cli.Remove(ctx, filePath)
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
func (gc *GarbageCollector) recycleUnusedIndexes(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedIndexes"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedIndexes...")
	defer func() {
		log.Info(ctx, "recycleUnusedIndexes done", mlog.Duration("timeCost", time.Since(start)))
	}()

	deletedIndexes := gc.Option.IndexMeta.GetDeletedIndexes()
	for _, idx := range deletedIndexes {
		if ctx.Err() != nil {
			// process canceled.
			return
		}
		if gc.collectionGCPaused(idx.CollectionID) {
			continue
		}
		gc.ackSignal(signal)

		log := mlog.With(mlog.Int64("collectionID", idx.CollectionID), mlog.Int64("fieldID", idx.FieldID), mlog.Int64("indexID", idx.IndexID))
		if err := gc.Option.IndexMeta.RemoveIndex(ctx, idx.CollectionID, idx.IndexID); err != nil {
			log.Warn(ctx, "remove index on collection fail", mlog.Err(err))
			continue
		}
		log.Info(ctx, "remove index on collection done")
	}
}

type manifestIndexIdentity struct {
	indexID int64
	buildID int64
}

type segmentIndexGCItem struct {
	segIdx *model.SegmentIndex
	files  map[string]struct{}
}

// recycleUnusedSegIndexes removes segment indexes whose index definition or
// segment is gone. Candidates are grouped by segment so one manifest read and
// one manifest revision can retire every eligible entry in a transaction-sized
// chunk, while record-only indexes keep the legacy path.
func (gc *GarbageCollector) RecycleUnusedSegIndexes(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedSegIndexes"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedSegIndexes...")
	defer func() {
		log.Info(ctx, "recycleUnusedSegIndexes done", mlog.Duration("timeCost", time.Since(start)))
	}()

	io := packed.NewManifestIOContext(1)
	defer io.Close()
	groups := make(map[int64][]*model.SegmentIndex)
	segmentOrder := make([]int64, 0)
	for _, candidate := range gc.Option.IndexMeta.GetAllSegIndexes() {
		if candidate == nil {
			continue
		}
		if _, ok := groups[candidate.SegmentID]; !ok {
			segmentOrder = append(segmentOrder, candidate.SegmentID)
		}
		groups[candidate.SegmentID] = append(groups[candidate.SegmentID], candidate)
	}
	for _, segmentID := range segmentOrder {
		if ctx.Err() != nil {
			return
		}
		gc.recycleUnusedSegIndexesForSegment(ctx, io, segmentID, groups[segmentID], signal)
	}
}

func (gc *GarbageCollector) recycleUnusedSegIndexesForSegment(ctx context.Context, io *packed.ManifestIOContext, segmentID int64,
	candidates []*model.SegmentIndex, signal <-chan gcCmd,
) {
	segment := gc.Meta.GetSegment(ctx, segmentID)
	items := make([]segmentIndexGCItem, 0, len(candidates))
	for _, candidate := range candidates {
		if ctx.Err() != nil {
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
		if segment != nil && gc.Option.IndexMeta.IsIndexExist(segIdx.CollectionID, segIdx.IndexID) {
			continue
		}
		// Non-terminal tasks may still be writing index files or updating meta.
		// Keep the SegmentIndex until the task reaches a terminal state.
		if segIdx.IndexState == commonpb.IndexState_Unissued ||
			segIdx.IndexState == commonpb.IndexState_InProgress ||
			segIdx.IndexState == commonpb.IndexState_Retry {
			mlog.Info(ctx, "skip GC segment index since index task is not terminal",
				mlog.Int64("collectionID", segIdx.CollectionID),
				mlog.Int64("partitionID", segIdx.PartitionID),
				mlog.Int64("segmentID", segIdx.SegmentID),
				mlog.Int64("indexID", segIdx.IndexID),
				mlog.Int64("buildID", segIdx.BuildID),
				mlog.String("state", segIdx.IndexState.String()))
			continue
		}
		if snapshotMeta := gc.Option.SnapshotMeta; snapshotMeta != nil &&
			snapshotMeta.IsBuildIDGCBlocked(segIdx.CollectionID, segIdx.BuildID) {
			mlog.Info(ctx, "skip GC segment index since buildID is protected by snapshot",
				mlog.Int64("collectionID", segIdx.CollectionID),
				mlog.Int64("buildID", segIdx.BuildID))
			continue
		}
		items = append(items, segmentIndexGCItem{segIdx: segIdx, files: gc.GetAllIndexFilesOfIndex(segIdx)})
	}
	if len(items) == 0 {
		return
	}

	// Task refresh can observe a completion published after the initial segment
	// snapshot. Read placement after admitting terminal tasks, or an old marker
	// or revision could send a manifest-resident result through record-only GC.
	segment = gc.Meta.GetSegment(ctx, segmentID)

	// Only a healthy, marked StorageV3 segment can contain entries to retract.
	// A false marker proves the published section is empty and preserves the
	// legacy path with zero manifest I/O. Dropped/missing segments publish no
	// further revision and are likewise handled record-by-record below.
	manifestEntries := make(map[manifestIndexIdentity]packed.ManifestIndexInfo)
	if segment != nil && meta.IsSegmentHealthy(segment) && segment.GetStorageVersion() == storage.StorageV3 &&
		segment.GetManifestPath() != "" && segment.GetManifestHasIndex() {
		indexes, err := gc.Meta.ReadManifestIndexesWithIO(ctx, io, segment.GetManifestPath(), meta.CreateStorageConfig())
		if err != nil {
			mlog.Warn(ctx, "failed to read segment manifest index metadata, wait to retry",
				mlog.Int64("segmentID", segmentID), mlog.Err(err))
			return
		}
		for _, idx := range indexes {
			key := manifestIndexIdentity{indexID: idx.IndexID, buildID: idx.BuildID}
			if _, exists := manifestEntries[key]; !exists {
				manifestEntries[key] = idx
			}
		}
	}

	manifestItems := make([]segmentIndexGCItem, 0, len(items))
	legacyItems := make([]segmentIndexGCItem, 0, len(items))
	for _, item := range items {
		key := manifestIndexIdentity{indexID: item.segIdx.IndexID, buildID: item.segIdx.BuildID}
		entry, matched := manifestEntries[key]
		if !matched {
			legacyItems = append(legacyItems, item)
			continue
		}
		info, ok := meta.ManifestIndexFilePathInfoForSegment(gc.Option.Cli.RootPath(), segment.SegmentInfo, entry)
		if !ok {
			mlog.Warn(ctx, "invalid segment manifest index metadata, wait to retry",
				mlog.Int64("segmentID", item.segIdx.SegmentID),
				mlog.Int64("indexID", item.segIdx.IndexID),
				mlog.Int64("buildID", item.segIdx.BuildID))
			continue
		}
		for _, file := range info.GetIndexFilePaths() {
			item.files[file] = struct{}{}
		}
		manifestItems = append(manifestItems, item)
	}

	for _, item := range legacyItems {
		gc.recycleRecordOnlySegmentIndex(ctx, item)
	}
	gc.recycleManifestSegmentIndexes(ctx, manifestItems)
}

func (gc *GarbageCollector) recycleRecordOnlySegmentIndex(ctx context.Context, item segmentIndexGCItem) {
	log := segmentIndexGCLog(item)
	log.Info(ctx, "GC Segment Index file start...")
	if err := gc.RemoveObjectFiles(ctx, item.files); err != nil {
		log.Warn(ctx, "fail to remove index files for index", mlog.Err(err))
		return
	}
	if err := gc.Option.IndexMeta.RemoveSegmentIndex(ctx, item.segIdx.BuildID); err != nil {
		log.Warn(ctx, "delete index meta from etcd failed, wait to retry", mlog.Err(err))
		return
	}
	log.Info(ctx, "index meta recycle success")
}

func (gc *GarbageCollector) recycleManifestSegmentIndexes(ctx context.Context, items []segmentIndexGCItem) {
	if len(items) == 0 {
		return
	}
	// One segment pointer PUT shares the atomic transaction with every
	// defensive SegmentIndex DELETE. Keep each chunk within the configured
	// etcd bound; stores with a larger native limit are conservatively split.
	maxBatch := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt() - 1
	if maxBatch < 1 {
		mlog.Error(ctx, "cannot atomically retract manifest indexes with the configured transaction limit",
			mlog.Int("maxEtcdTxnNum", paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()))
		return
	}
	for start := 0; start < len(items); start += maxBatch {
		end := min(start+maxBatch, len(items))
		ready := make([]segmentIndexGCItem, 0, end-start)
		for _, item := range items[start:end] {
			log := segmentIndexGCLog(item)
			log.Info(ctx, "GC Segment Index file start...")
			if err := gc.RemoveObjectFiles(ctx, item.files); err != nil {
				log.Warn(ctx, "fail to remove index files for index", mlog.Err(err))
				continue
			}
			ready = append(ready, item)
		}
		if len(ready) == 0 {
			continue
		}

		retracted, err := gc.commitManifestIndexRetractions(ctx, ready)
		if err != nil {
			mlog.Warn(ctx, "failed to retract segment indexes from manifest, wait to retry",
				mlog.Int64("segmentID", ready[0].segIdx.SegmentID),
				mlog.Int("indexCount", len(ready)), mlog.Err(err))
			continue
		}
		if retracted {
			for _, item := range ready {
				segmentIndexGCLog(item).Info(ctx, "index meta recycle success")
			}
			continue
		}
		// The segment retired between resolution and commit. It will publish no
		// further manifest revision; remove any surviving records via the legacy
		// idempotent path so they do not strand the already-deleted files.
		for _, item := range ready {
			if err := gc.Option.IndexMeta.RemoveSegmentIndex(ctx, item.segIdx.BuildID); err != nil {
				segmentIndexGCLog(item).Warn(ctx, "delete index meta from etcd failed, wait to retry", mlog.Err(err))
			}
		}
	}
}

func segmentIndexGCLog(item segmentIndexGCItem) *mlog.Logger {
	return mlog.With(mlog.Int64("collectionID", item.segIdx.CollectionID),
		mlog.Int64("partitionID", item.segIdx.PartitionID),
		mlog.Int64("segmentID", item.segIdx.SegmentID),
		mlog.Int64("indexID", item.segIdx.IndexID),
		mlog.Int64("buildID", item.segIdx.BuildID),
		mlog.Int64("nodeID", item.segIdx.NodeID),
		mlog.Int("indexFiles", len(item.files)))
}

// commitManifestIndexRetractions publishes a manifest revision without these
// indexes and removes their SegmentIndex records in one catalog transaction. It
// reports whether the records were actually removed; false means the segment
// was retired in the meantime, so the caller still has records to remove on
// its own.
//
// ExpectedBuildID makes the drop reject a manifest that a rebuild has
// meanwhile republished under the same index ID; the commit is resolved
// against the revision it is opened at, not the one the file list was resolved
// from. The SegmentIndex removal rides in the same catalog transaction as the
// resulting pointer, so the manifest and etcd can never disagree about whether
// this index still exists. The etcd side needs no expectation of its own: the
// records are keyed by buildID, and rebuilds allocate new ones.
func (gc *GarbageCollector) commitManifestIndexRetractions(ctx context.Context, items []segmentIndexGCItem) (bool, error) {
	if len(items) == 0 {
		return true, nil
	}
	drops := make([]packed.DropIndexEntry, 0, len(items))
	mutations := make([]meta.SegmentIndexMutation, 0, len(items))
	for _, item := range items {
		drops = append(drops, packed.DropIndexEntry{IndexID: item.segIdx.IndexID, ExpectedBuildID: item.segIdx.BuildID})
		mutations = append(mutations, meta.SegmentIndexMutation{Type: meta.SegmentIndexRemove, BuildID: item.segIdx.BuildID})
	}
	if err := gc.Meta.CommitSegmentManifest(ctx, meta.SegmentManifestCommit{
		SegmentID:     items[0].segIdx.SegmentID,
		StorageConfig: meta.CreateStorageConfig(),
		Mutation: meta.ManifestMutation{
			Type: meta.ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{
				DropIndexes: drops,
			},
		},
		CatalogMutation: meta.SegmentCatalogMutation{
			SegmentIndexes: mutations,
		},
	}); err != nil {
		// Retired between resolution and this commit: the whole manifest goes
		// away with the segment, so there is nothing left to retract.
		if errors.Is(err, merr.ErrSegmentNotFound) {
			return false, nil
		}
		return false, merr.Wrap(err, "failed to publish segment manifest without the recycled index")
	}
	return true, nil
}

// getLatestSegmentIndexForGC takes a SegmentIndex candidate from GC scanning and
// returns the latest SegmentIndex meta for the same buildID. The bool return
// value is false when the candidate is nil or the buildID no longer exists.
func (gc *GarbageCollector) getLatestSegmentIndexForGC(candidate *model.SegmentIndex) (*model.SegmentIndex, bool) {
	if candidate == nil {
		return nil, false
	}
	if gc.Option.IndexMeta == nil || gc.Option.IndexMeta.SegmentBuildInfo == nil {
		return candidate, true
	}
	return gc.Option.IndexMeta.GetIndexJob(candidate.BuildID)
}

// recycleUnusedIndexFilesV0 deletes orphan files under the legacy v0 index_files prefix.
// v0 paths are rooted by buildID, so the first-level directory can be parsed and
// checked against index meta directly.
func (gc *GarbageCollector) RecycleUnusedIndexFilesV0(ctx context.Context) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedIndexFilesV0"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedIndexFilesV0...")

	prefix := path.Join(gc.Option.Cli.RootPath(), common.SegmentIndexV0Path) + "/"

	// Resolve snapshotMeta once. Both IsBuildIDGCBlocked paths below are O(1) so
	// no caching of intermediate state is needed.
	snapshotMeta := gc.Option.SnapshotMeta

	// list dir first
	keyCount := 0
	err := gc.Option.Cli.WalkWithPrefix(ctx, prefix, false, func(indexPathInfo *storage.ChunkObjectInfo) bool {
		key := indexPathInfo.FilePath
		keyCount++
		logger := mlog.With(mlog.String("prefix", prefix), mlog.String("key", key))

		// This recycler only walks index_files/ (v0). Its first path level is buildID;
		// v1 collectionID directories live under index_v1/ and are handled below.
		buildID, err := meta.ParseBuildIDFromFilePath(key)
		if err != nil {
			logger.Warn(ctx, "garbageCollector recycleUnusedIndexFilesV0 parseIndexFileKey", mlog.Err(err))
			return true
		}
		logger = logger.With(mlog.Int64("buildID", buildID))
		logger.Info(ctx, "garbageCollector will recycle index files")
		canRecycle, segIdx := gc.Option.IndexMeta.CheckCleanSegmentIndex(buildID)
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
			err = gc.Option.Cli.RemoveWithPrefix(ctx, key)
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

		filesMap := gc.GetAllIndexFilesOfIndex(segIdx)

		logger.Info(ctx, "recycle index files", mlog.Int("meta files num", len(filesMap)))
		deletedFilesNum := atomic.NewInt32(0)
		fileNum := 0

		futures := make([]*conc.Future[struct{}], 0)
		err = gc.Option.Cli.WalkWithPrefix(ctx, key, true, func(indexFile *storage.ChunkObjectInfo) bool {
			fileNum++
			file := indexFile.FilePath
			if _, ok := filesMap[file]; !ok {
				future := gc.Option.RemoveObjectPool.Submit(func() (struct{}, error) {
					logger := logger.With(mlog.String("file", file))
					logger.Info(ctx, "garbageCollector recycleUnusedIndexFilesV0 remove file...")

					if err := gc.Option.Cli.Remove(ctx, file); err != nil {
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
func (gc *GarbageCollector) GetAllIndexFilesOfIndex(segmentIndex *model.SegmentIndex) map[string]struct{} {
	builder := metautil.NewIndexPathBuilder(gc.Option.Cli.RootPath(),
		segmentIndex.IndexStorePathVersion, segmentIndex.CollectionID,
		segmentIndex.PartitionID, segmentIndex.SegmentID,
		segmentIndex.BuildID, segmentIndex.IndexVersion)
	filesMap := make(map[string]struct{})
	for _, fileID := range segmentIndex.IndexFileKeys {
		filesMap[builder.BuildFilePath(fileID)] = struct{}{}
	}
	return filesMap
}

// recycleUnusedIndexFilesV1 is the orphan scan for the collection-rooted
// index_v1 layout, the counterpart of recycleUnusedIndexFilesV0 for
// index_files. Paths are {root}/index_v1/{coll}/{part}/{seg}/{buildID}/{version}/{key},
// so unlike v0 the buildID is not a first-level directory and the whole prefix
// is walked recursively, like the binlog orphan scans.
//
// A file is reclaimed when meta no longer accounts for it: its buildID has no
// SegmentIndex record (task aborted and recycled, DataCoord crashed before
// FinishTask, worker restarted after uploading, ...), or the record is
// Finished and does not list the file (an earlier attempt's index version).
// Files of records that are not Finished are left alone: the task may still be
// running, or the record is terminal and waiting for recycleUnusedSegIndexes,
// which removes it and lets the next scan reclaim the files. Files younger
// than missingTolerance are skipped so an upload still in flight (the cgo
// upload does not observe cancellation) is never raced.
//
// Files of a segment whose row still exists but is no longer healthy are left
// alone: dropped-segment GC owns them while the row lives, as does the cleanup
// plan of a rejected copy task. The orphan scan takes over once the row is gone.
//
// A build without a record whose segment is still Importing is kept too: a
// snapshot restore pre-registers its target segments as Importing, copies
// index files under freshly allocated build IDs, and only writes the
// SegmentIndex once the copy has finished, so a restore that runs longer than
// missingTolerance would otherwise lose the files it copied first.
func (gc *GarbageCollector) recycleUnusedIndexFilesV1(ctx context.Context) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedIndexFilesV1"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedIndexFilesV1...")

	rootPath := gc.Option.Cli.RootPath()
	prefix := path.Join(rootPath, common.SegmentIndexV1Path) + "/"
	snapshotMeta := gc.Option.SnapshotMeta

	// The decision for the build currently being walked is cached: a build's
	// keys share its prefix and are listed together, so one entry avoids
	// rebuilding the recorded file set per file while keeping memory bounded
	// by a single build rather than every live index file. If a listing ever
	// interleaves builds, the decision is simply recomputed.
	type buildDecision struct {
		buildID    int64
		reclaimAll bool                // no record: everything under the build is garbage
		recorded   map[string]struct{} // Finished record: only unlisted files are garbage
	}
	var current *buildDecision

	decide := func(collectionID, segmentID, buildID int64) *buildDecision {
		if current != nil && current.buildID == buildID {
			return current
		}
		d := &buildDecision{buildID: buildID}
		current = d
		if snapshotMeta != nil && snapshotMeta.IsBuildIDGCBlocked(collectionID, buildID) {
			// Keep everything; nil decision fields mean "reclaim nothing".
			return d
		}
		canRecycle, segIdx := gc.Option.IndexMeta.CheckCleanSegmentIndex(buildID)
		if !canRecycle {
			return d
		}
		if segIdx == nil {
			segment := gc.Meta.GetSegment(ctx, segmentID)
			switch {
			case segment == nil:
				d.reclaimAll = true
			case !meta.IsSegmentHealthy(segment):
				// Dropped-segment GC and rejected-copy cleanup own these files
				// for as long as the segment row exists.
			case segment.GetState() == commonpb.SegmentState_Importing || segment.GetIsImporting():
				// Snapshot restore still copying into this segment.
			default:
				d.reclaimAll = true
			}
			return d
		}
		d.recorded = gc.GetAllIndexFilesOfIndex(segIdx)
		return d
	}

	var (
		total, skipped, valid int
		removed               = atomic.NewInt32(0)
		futures               = make([]*conc.Future[struct{}], 0)
	)
	err := gc.Option.Cli.WalkWithPrefix(ctx, prefix, true, func(obj *storage.ChunkObjectInfo) bool {
		total++
		file := obj.FilePath
		if time.Since(obj.ModifyTime) <= gc.Option.MissingTolerance {
			skipped++
			return true
		}
		collectionID, segmentID, buildID, err := parseIndexV1FilePath(rootPath, file)
		if err != nil {
			skipped++
			log.Warn(ctx, "recycleUnusedIndexFilesV1 skip unparsable index file", mlog.String("file", file), mlog.Err(err))
			return true
		}
		d := decide(collectionID, segmentID, buildID)
		if !d.reclaimAll {
			if _, listed := d.recorded[file]; listed || d.recorded == nil {
				valid++
				return true
			}
		}
		futures = append(futures, gc.Option.RemoveObjectPool.Submit(func() (struct{}, error) {
			if err := gc.Option.Cli.Remove(ctx, file); err != nil {
				log.Warn(ctx, "recycleUnusedIndexFilesV1 remove file failed", mlog.String("file", file), mlog.Err(err))
				return struct{}{}, err
			}
			removed.Inc()
			log.Info(ctx, "recycleUnusedIndexFilesV1 removed orphan index file",
				mlog.String("file", file), mlog.Int64("collectionID", collectionID), mlog.Int64("buildID", buildID))
			return struct{}{}, nil
		}))
		return true
	})
	if walkErr := conc.BlockOnAll(futures...); walkErr != nil {
		log.Warn(ctx, "some task failure in remove object pool", mlog.Err(walkErr))
	}
	log = log.With(mlog.Duration("timeCost", time.Since(start)), mlog.Int("total", total),
		mlog.Int("valid", valid), mlog.Int("skipped", skipped), mlog.Int32("removed", removed.Load()))
	if err != nil {
		log.Warn(ctx, "recycleUnusedIndexFilesV1 walk failed, remaining files wait for the next scan", mlog.Err(err))
		return
	}
	log.Info(ctx, "recycleUnusedIndexFilesV1 done")
}

// parseIndexV1FilePath extracts the collection, segment and build IDs from an
// index_v1 object path: {root}/index_v1/{coll}/{part}/{seg}/{buildID}/{version}/{key}.
func parseIndexV1FilePath(rootPath, filePath string) (collectionID, segmentID, buildID int64, err error) {
	prefix := path.Join(rootPath, common.SegmentIndexV1Path) + "/"
	rel, ok := strings.CutPrefix(filePath, prefix)
	if !ok {
		return 0, 0, 0, merr.WrapErrParameterInvalidMsg("index file %s is not under %s", filePath, prefix)
	}
	parts := strings.Split(rel, "/")
	if len(parts) < 6 {
		return 0, 0, 0, merr.WrapErrParameterInvalidMsg("index file %s does not match {coll}/{part}/{seg}/{buildID}/{version}/{key}", filePath)
	}
	if collectionID, err = strconv.ParseInt(parts[0], 10, 64); err != nil {
		return 0, 0, 0, merr.WrapErrParameterInvalidMsg("index file %s has non-numeric collection id: %v", filePath, err)
	}
	if segmentID, err = strconv.ParseInt(parts[2], 10, 64); err != nil {
		return 0, 0, 0, merr.WrapErrParameterInvalidMsg("index file %s has non-numeric segment id: %v", filePath, err)
	}
	if buildID, err = strconv.ParseInt(parts[3], 10, 64); err != nil {
		return 0, 0, 0, merr.WrapErrParameterInvalidMsg("index file %s has non-numeric build id: %v", filePath, err)
	}
	return collectionID, segmentID, buildID, nil
}

// recycleUnusedAnalyzeFiles is used to delete those analyze stats files that no longer exist in the meta.
func (gc *GarbageCollector) RecycleUnusedAnalyzeFiles(ctx context.Context, signal <-chan gcCmd) {
	mlog.Info(ctx, "start recycleUnusedAnalyzeFiles")
	startTs := time.Now()
	prefix := path.Join(gc.Option.Cli.RootPath(), common.AnalyzeStatsPath) + "/"
	// list dir first
	keys := make([]string, 0)
	err := gc.Option.Cli.WalkWithPrefix(ctx, prefix, false, func(chunkInfo *storage.ChunkObjectInfo) bool {
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
		taskID, err := meta.ParseBuildIDFromFilePath(key)
		if err != nil {
			mlog.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles parseAnalyzeResult failed", mlog.String("key", key), mlog.Err(err))
			continue
		}
		mlog.Info(ctx, "garbageCollector will recycle analyze stats files", mlog.Int64("taskID", taskID))
		canRecycle, task := gc.Option.AnalyzeMeta.CheckCleanAnalyzeTask(taskID)
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
			err = gc.Option.Cli.RemoveWithPrefix(ctx, key)
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
			if err := gc.Option.Cli.RemoveWithPrefix(ctx, removePrefix); err != nil {
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
func (gc *GarbageCollector) RecycleUnusedTextIndexFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedTextIndexFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedTextIndexFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedTextIndexFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	hasTextIndexSegments := gc.Meta.SelectSegments(ctx, meta.SegmentFilterFunc(func(info *meta.SegmentInfo) bool {
		return len(info.GetTextStatsLogs()) != 0
	}))
	fileNum := 0
	deletedFilesNum := atomic.NewInt32(0)

	snapshotMeta := gc.Option.SnapshotMeta

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
				prefix := metautil.BuildTextIndexPrefix(gc.Option.Cli.RootPath(),
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.Option.Cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					fileNum++
					file := files.FilePath

					future := gc.Option.RemoveObjectPool.Submit(func() (struct{}, error) {
						log := mlog.With(mlog.String("file", file))
						log.Info(ctx, "garbageCollector recycleUnusedTextIndexFiles remove file...")

						if err := gc.Option.Cli.Remove(ctx, file); err != nil {
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
func (gc *GarbageCollector) RecycleUnusedJSONStatsFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedJSONStatsFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedJSONStatsFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedJSONStatsFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	hasJSONStatsSegments := gc.Meta.SelectSegments(ctx, meta.SegmentFilterFunc(func(info *meta.SegmentInfo) bool {
		return len(info.GetJsonKeyStats()) != 0
	}))
	fileNum := 0
	deletedFilesNum := atomic.NewInt32(0)
	maxJSONStatsDataFormat := int64(1)

	snapshotMeta := gc.Option.SnapshotMeta

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
			if fieldStats.GetJsonKeyStatsDataFormat() > maxJSONStatsDataFormat {
				maxJSONStatsDataFormat = fieldStats.GetJsonKeyStatsDataFormat()
			}
			// clear low version task
			for i := int64(1); i < fieldStats.GetVersion(); i++ {
				prefix := metautil.BuildJSONKeyStatsPrefix(gc.Option.Cli.RootPath(), fieldStats.GetJsonKeyStatsDataFormat(),
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.Option.Cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					fileNum++
					file := files.FilePath

					future := gc.Option.RemoveObjectPool.Submit(func() (struct{}, error) {
						log := mlog.With(mlog.String("file", file))
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file...")

						if err := gc.Option.Cli.Remove(ctx, file); err != nil {
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
		}
	}

	// Clear files from older data formats once per global format prefix.
	for dataFormat := int64(1); dataFormat < maxJSONStatsDataFormat; dataFormat++ {
		if ctx.Err() != nil {
			return
		}
		gc.ackSignal(signal)
		prefix := fmt.Sprintf("%s/%s/%d", gc.Option.Cli.RootPath(), common.JSONStatsPath, dataFormat)
		futures := make([]*conc.Future[struct{}], 0)

		err := gc.Option.Cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
			fileNum++
			file := files.FilePath

			future := gc.Option.RemoveObjectPool.Submit(func() (struct{}, error) {
				log := mlog.With(mlog.String("file", file))
				log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file...")

				if err := gc.Option.Cli.Remove(ctx, file); err != nil {
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
	log.Info(ctx, "json stats files recycle done",
		mlog.Int("deleteJSONStatsNum", int(deletedFilesNum.Load())),
		mlog.Int("walkFileNum", fileNum))

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleUnusedJSONIndexFiles load meta file info and compares OSS keys
func (gc *GarbageCollector) RecycleUnusedJSONIndexFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedJSONIndexFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedJSONIndexFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedJSONIndexFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	hasJSONIndexSegments := gc.Meta.SelectSegments(ctx, meta.SegmentFilterFunc(func(info *meta.SegmentInfo) bool {
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
		if snapshotMeta := gc.Option.SnapshotMeta; snapshotMeta != nil {
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
				prefix := fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d", gc.Option.Cli.RootPath(), common.JSONIndexPath,
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.Option.Cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					fileNum++
					file := files.FilePath

					future := gc.Option.RemoveObjectPool.Submit(func() (struct{}, error) {
						log := mlog.With(mlog.String("file", file))
						log.Info(ctx, "garbageCollector recycleUnusedJSONIndexFiles remove file...")

						if err := gc.Option.Cli.Remove(ctx, file); err != nil {
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
func (gc *GarbageCollector) recycleSnapshots(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleSnapshots"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleSnapshots...")
	defer func() {
		log.Info(ctx, "recycleSnapshots done", mlog.Duration("timeCost", time.Since(start)))
	}()

	snapshotMeta := gc.Option.SnapshotMeta
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

		for _, snap := range pendingSnapshots {
			snapshotLog := mlog.With(
				mlog.String("snapshotName", snap.GetName()),
				mlog.Int64("snapshotID", snap.GetId()),
				mlog.Int64("collectionID", snap.GetCollectionId()),
			)

			gc.ackSignal(signal)
			// Compute paths from collection_id + snapshot_id
			manifestDir, metadataPath := snapshotstorage.GetSnapshotPaths(
				gc.Option.Cli.RootPath(),
				snap.GetCollectionId(),
				snap.GetId(),
			)

			snapshotLog.Info(ctx, "cleaning up pending snapshot",
				mlog.String("manifestDir", manifestDir),
				mlog.String("metadataPath", metadataPath))

			// Delete manifest directory using RemoveWithPrefix (no list needed)
			// This removes all segment manifest files: manifests/{snapshot_id}/*.avro
			if err := gc.Option.Cli.RemoveWithPrefix(ctx, manifestDir); err != nil {
				snapshotLog.Warn(ctx, "failed to remove pending snapshot manifest directory", mlog.Err(err))
				// Keep catalog record for retry in next GC cycle.
				continue
			}

			// Delete metadata file
			if err := gc.Option.Cli.Remove(ctx, metadataPath); err != nil {
				snapshotLog.Warn(ctx, "failed to remove pending snapshot metadata file", mlog.Err(err))
				// Keep catalog record for retry in next GC cycle.
				continue
			}

			// Delete etcd record
			if err := snapshotMeta.CleanupPendingSnapshot(ctx, snap); err != nil {
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

		for _, snap := range deletingSnapshots {
			snapshotLog := mlog.With(
				mlog.String("snapshotName", snap.GetName()),
				mlog.Int64("snapshotID", snap.GetId()),
				mlog.Int64("collectionID", snap.GetCollectionId()),
			)

			gc.ackSignal(signal)

			// Compute paths from collection_id + snapshot_id
			manifestDir, metadataPath := snapshotstorage.GetSnapshotPaths(
				gc.Option.Cli.RootPath(),
				snap.GetCollectionId(),
				snap.GetId(),
			)

			snapshotLog.Info(ctx, "cleaning up deleting snapshot",
				mlog.String("manifestDir", manifestDir),
				mlog.String("metadataPath", metadataPath))

			// Delete manifest directory
			if err := gc.Option.Cli.RemoveWithPrefix(ctx, manifestDir); err != nil {
				snapshotLog.Warn(ctx, "failed to remove deleting snapshot manifest directory", mlog.Err(err))
				// Continue with metadata and etcd cleanup even if S3 cleanup fails
			}

			// Delete metadata file
			if err := gc.Option.Cli.Remove(ctx, metadataPath); err != nil {
				snapshotLog.Warn(ctx, "failed to remove deleting snapshot metadata file", mlog.Err(err))
				// Continue with etcd cleanup even if S3 cleanup fails
			}

			// Delete etcd record
			if err := snapshotMeta.CleanupDeletingSnapshot(ctx, snap); err != nil {
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
			for _, r := range snapshotMeta.CleanExpiredPinsForCollection(ctx, collectionID) {
				copysegment.SetSnapshotActivePinsGauge(r.CollectionID, r.SnapshotName, r.ActivePins)
			}

			// Step 2: if the collection was dropped, cascade-delete orphan snapshots.
			timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			has, err := gc.Option.Broker.HasCollection(timeoutCtx, collectionID)
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
				copysegment.SetSnapshotActivePinsGauge(collectionID, n, 0)
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
