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

package datanode

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type level2CompactionTask struct {
	compactor
	io        io.BinlogIO
	stageIO   io.BinlogIO
	allocator allocator.Allocator
	metaCache metacache.MetaCache
	syncMgr   syncmgr.SyncManager

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	tr     *timerecord.TimeRecorder

	plan *datapb.CompactionPlan

	//schedule
	totalBufferSize atomic.Int64
	spillChan       chan SpillSignal
	spillCount      atomic.Int64
	pool            *conc.Pool[any]

	// inner field
	collectionID       int64
	partitionID        int64
	collectionMeta     *etcdpb.CollectionMeta
	currentTs          typeutil.Timestamp // for TTL
	clusteringKeyField *schemapb.FieldSchema
	primaryKeyField    *schemapb.FieldSchema

	groupLocks    *lock.KeyLock[interface{}]
	keyToGroupMap map[interface{}]*Group
	groups        []*Group
}

type Group struct {
	id           int
	bufferSize   int64
	buffer       *InsertData
	bufferRowNum int64

	currentSegmentID          int64
	currentSpillSize          int64
	currentSpillRowNum        int64
	currentSpillBinlogs       []*datapb.FieldBinlog
	currentSpillStatslogs     []*datapb.FieldBinlog
	currentPKStats            *storage.PrimaryKeyStats
	currentClusteringKeyStats *storage.FieldStats

	segments     []*datapb.CompactionSegment
	segmentStats map[UniqueID]storage.SegmentStats
}

type SpillSignal struct {
	key interface{}
}

func newLevel2CompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	stagingIO io.BinlogIO,
	alloc allocator.Allocator,
	metaCache metacache.MetaCache,
	syncMgr syncmgr.SyncManager,
	plan *datapb.CompactionPlan,
) *level2CompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	return &level2CompactionTask{
		ctx:             ctx,
		cancel:          cancel,
		io:              binlogIO,
		stageIO:         stagingIO,
		allocator:       alloc,
		metaCache:       metaCache,
		syncMgr:         syncMgr,
		plan:            plan,
		tr:              timerecord.NewTimeRecorder("level2 compaction"),
		done:            make(chan struct{}, 1),
		totalBufferSize: atomic.Int64{},
		spillChan:       make(chan SpillSignal, 100),
		pool:            conc.NewPool[any](hardware.GetCPUNum() * 2),
		groupLocks:      lock.NewKeyLock[interface{}](),
	}
}

func (t *level2CompactionTask) complete() {
	t.done <- struct{}{}
}

func (t *level2CompactionTask) stop() {
	t.cancel()
	<-t.done
}

func (t *level2CompactionTask) getPlanID() UniqueID {
	return t.plan.GetPlanID()
}

func (t *level2CompactionTask) getChannelName() string {
	return t.plan.GetChannel()
}

func (t *level2CompactionTask) getCollection() int64 {
	return t.metaCache.Collection()
}

// injectDone unlock the segments
func (t *level2CompactionTask) injectDone() {
	for _, binlog := range t.plan.SegmentBinlogs {
		t.syncMgr.Unblock(binlog.SegmentID)
	}
}

func (t *level2CompactionTask) init() error {
	segIDs := make([]UniqueID, 0, len(t.plan.GetSegmentBinlogs()))
	for _, s := range t.plan.GetSegmentBinlogs() {
		segIDs = append(segIDs, s.GetSegmentID())
	}
	collectionID, partitionID, meta, err := t.getSegmentMeta(segIDs[0])
	if err != nil {
		return err
	}
	t.collectionID = collectionID
	t.partitionID = partitionID
	t.collectionMeta = meta

	clusteringKeyID := t.plan.GetClusteringKeyId()
	var clusteringKeyField *schemapb.FieldSchema
	var pkField *schemapb.FieldSchema
	for _, field := range meta.Schema.Fields {
		if field.FieldID == clusteringKeyID {
			clusteringKeyField = field
		}
		if field.GetIsPrimaryKey() && field.GetFieldID() >= 100 && typeutil.IsPrimaryFieldType(field.GetDataType()) {
			pkField = field
		}
	}
	t.clusteringKeyField = clusteringKeyField
	t.primaryKeyField = pkField

	t.currentTs = tsoutil.GetCurrentTime()
	return nil
}

func (t *level2CompactionTask) compact() (*datapb.CompactionPlanResult, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("L2Compact-%d", t.getPlanID()))
	defer span.End()
	log := log.With(zap.Int64("planID", t.plan.GetPlanID()), zap.String("type", t.plan.GetType().String()))
	if t.plan.GetType() != datapb.CompactionType_MajorCompaction {
		// this shouldn't be reached
		log.Warn("compact wrong, illegal compaction type")
		return nil, errIllegalCompactionPlan
	}
	log.Info("L2 compaction", zap.Duration("wait in queue elapse", t.tr.RecordSpan()))
	if !funcutil.CheckCtxValid(ctx) {
		log.Warn("compact wrong, task context done or timeout")
		return nil, errContext
	}
	ctxTimeout, cancelAll := context.WithTimeout(ctx, time.Duration(t.plan.GetTimeoutInSeconds())*time.Second)
	defer cancelAll()

	err := t.init()
	if err != nil {
		return nil, err
	}
	log.Info("compact start", zap.Int32("timeout in seconds", t.plan.GetTimeoutInSeconds()))

	err = binlog.DecompressCompactionBinlogs(t.plan.GetSegmentBinlogs())
	if err != nil {
		log.Warn("DecompressCompactionBinlogs fails", zap.Error(err))
		return nil, err
	}
	segIDs := make([]UniqueID, 0, len(t.plan.GetSegmentBinlogs()))
	for _, s := range t.plan.GetSegmentBinlogs() {
		segIDs = append(segIDs, s.GetSegmentID())
	}

	// Inject to stop flush
	injectStart := time.Now()
	for _, segID := range segIDs {
		t.syncMgr.Block(segID)
	}
	log.Info("compact inject elapse", zap.Duration("elapse", time.Since(injectStart)))
	defer func() {
		if err != nil {
			for _, segID := range segIDs {
				t.syncMgr.Unblock(segID)
			}
		}
	}()

	// 1, download delta logs to build deltaMap
	deltaBlobs, _, err := loadDeltaMap(ctxTimeout, t.io, t.plan.GetSegmentBinlogs())
	if err != nil {
		return nil, err
	}
	deltaPk2Ts, err := MergeDeltalogs(deltaBlobs)
	if err != nil {
		return nil, err
	}

	// 2, final clean up
	defer t.cleanUpStep(ctx)

	analyzeDict, err := t.analyzeStep(ctxTimeout, t.collectionMeta)
	if err != nil {
		return nil, err
	}
	plan := t.planStep(analyzeDict)

	// 3, mapStep
	uploadSegments, partitionStats, err := t.mapStep(ctxTimeout, deltaPk2Ts, plan)
	if err != nil {
		return nil, err
	}

	// 4, collect partition stats
	err = t.uploadPartitionStats(ctx, t.collectionID, t.partitionID, partitionStats)
	if err != nil {
		return nil, err
	}

	// 5, assemble CompactionPlanResult
	planResult := &datapb.CompactionPlanResult{
		State:    commonpb.CompactionState_Completed,
		PlanID:   t.getPlanID(),
		Segments: uploadSegments,
		Type:     t.plan.GetType(),
		Channel:  t.plan.GetChannel(),
	}

	metrics.DataNodeCompactionLatency.
		WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), t.plan.GetType().String()).
		Observe(float64(t.tr.ElapseSpan().Milliseconds()))
	log.Info("L2 compaction finished", zap.Duration("elapse", t.tr.ElapseSpan()))

	return planResult, nil
}

// mapStep read and map input segments iteratively, spill data if need
func (t *level2CompactionTask) mapStep(ctx context.Context,
	deltaPk2Ts map[interface{}]Timestamp,
	plan [][]interface{},
) ([]*datapb.CompactionSegment, *storage.PartitionStatsSnapshot, error) {
	flightSegments := t.plan.GetSegmentBinlogs()
	mapStart := time.Now()

	// start spill goroutine
	go t.backgroundSpill(ctx)

	keyToGroupMap := make(map[interface{}]*Group, 0)
	for id, bucket := range plan {
		group := &Group{
			id:           id,
			segments:     make([]*datapb.CompactionSegment, 0),
			segmentStats: make(map[UniqueID]storage.SegmentStats, 0),
		}
		t.groups = append(t.groups, group)
		//t.bufferDatas.Store(bucket, writeBuffer)
		for _, key := range bucket {
			keyToGroupMap[key] = group
		}
	}
	t.keyToGroupMap = keyToGroupMap

	futures := make([]*conc.Future[any], 0, len(flightSegments))
	for _, segment := range flightSegments {
		segmentClone := &datapb.CompactionSegmentBinlogs{
			SegmentID: segment.SegmentID,
			// only FieldBinlogs needed
			FieldBinlogs: segment.FieldBinlogs,
		}
		future := t.pool.Submit(func() (any, error) {
			err := t.split(ctx, segmentClone, deltaPk2Ts)
			return struct{}{}, err
		})
		futures = append(futures, future)
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return nil, nil, err
	}

	// first spill all in memory data to disk, actually we don't need to do this, just for ease
	err := t.forceSpillAll(ctx)
	if err != nil {
		return nil, nil, err
	}

	resultSegments := make([]*datapb.CompactionSegment, 0)
	resultPartitionStats := &storage.PartitionStatsSnapshot{
		SegmentStats: make(map[UniqueID]storage.SegmentStats, 0),
	}
	for _, group := range t.groups {
		for _, seg := range group.segments {
			resultSegments = append(resultSegments, &datapb.CompactionSegment{
				PlanID:              seg.GetPlanID(),
				SegmentID:           seg.GetSegmentID(),
				NumOfRows:           seg.GetNumOfRows(),
				InsertLogs:          seg.GetInsertLogs(),
				Field2StatslogPaths: seg.GetField2StatslogPaths(),
				Deltalogs:           seg.GetDeltalogs(),
				Channel:             seg.GetChannel(),
			})
		}
		for segID, segmentStat := range group.segmentStats {
			resultPartitionStats.SegmentStats[segID] = segmentStat
		}
	}

	log.Info("compact map step end",
		zap.Int64("collectionID", t.getCollection()),
		zap.Int64("partitionID", t.partitionID),
		zap.Int("segmentFrom", len(flightSegments)),
		zap.Int("segmentTo", len(resultSegments)),
		zap.Duration("map elapse", time.Since(mapStart)))

	return resultSegments, resultPartitionStats, nil
}

// read insert log of one segment, split it into buckets according to partitionKey. Spill data to file when necessary
func (t *level2CompactionTask) split(
	ctx context.Context,
	segment *datapb.CompactionSegmentBinlogs,
	delta map[interface{}]Timestamp,
) error {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("Compact-Map-%d", t.getPlanID()))
	defer span.End()
	log := log.With(zap.Int64("planID", t.getPlanID()))

	processStart := time.Now()
	fieldBinlogPaths := make([][]string, 0)
	// initial timestampFrom, timestampTo = -1, -1 is an illegal value, only to mark initial state
	var (
		timestampTo   int64 = -1
		timestampFrom int64 = -1
		expired       int64 = 0
		deleted       int64 = 0
		remained      int64 = 0
	)

	isDeletedValue := func(v *storage.Value) bool {
		ts, ok := delta[v.PK.GetValue()]
		// insert task and delete task has the same ts when upsert
		// here should be < instead of <=
		// to avoid the upsert data to be deleted after compact
		if ok && uint64(v.Timestamp) < ts {
			return true
		}
		return false
	}

	// Get the number of field binlog files from non-empty segment
	var binlogNum int
	for _, b := range segment.GetFieldBinlogs() {
		if b != nil {
			binlogNum = len(b.GetBinlogs())
			break
		}
	}
	// Unable to deal with all empty segments cases, so return error
	if binlogNum == 0 {
		log.Warn("compact wrong, all segments' binlogs are empty")
		return errIllegalCompactionPlan
	}
	for idx := 0; idx < binlogNum; idx++ {
		var ps []string
		for _, f := range segment.GetFieldBinlogs() {
			ps = append(ps, f.GetBinlogs()[idx].GetLogPath())
		}
		fieldBinlogPaths = append(fieldBinlogPaths, ps)
	}

	// todo: concurrent
	for _, path := range fieldBinlogPaths {
		bytesArr, err := t.io.Download(ctx, path)
		blobs := make([]*Blob, len(bytesArr))
		for i := range bytesArr {
			blobs[i] = &Blob{Value: bytesArr[i]}
		}
		if err != nil {
			log.Warn("download insertlogs wrong", zap.Strings("path", path), zap.Error(err))
			return err
		}

		pkIter, err := storage.NewInsertBinlogIterator(blobs, t.primaryKeyField.GetFieldID(), t.primaryKeyField.GetDataType())
		if err != nil {
			log.Warn("new insert binlogs Itr wrong", zap.Strings("path", path), zap.Error(err))
			return err
		}

		for pkIter.HasNext() {
			vInter, _ := pkIter.Next()
			v, ok := vInter.(*storage.Value)
			if !ok {
				log.Warn("transfer interface to Value wrong", zap.Strings("path", path))
				return errors.New("unexpected error")
			}

			// Filtering deleted entity
			if isDeletedValue(v) {
				deleted++
				continue
			}
			// Filtering expired entity
			ts := Timestamp(v.Timestamp)
			if IsExpiredEntity(t.plan.GetCollectionTtl(), ts, t.currentTs) {
				expired++
				continue
			}
			// Update timestampFrom, timestampTo
			if v.Timestamp < timestampFrom || timestampFrom == -1 {
				timestampFrom = v.Timestamp
			}
			if v.Timestamp > timestampTo || timestampFrom == -1 {
				timestampTo = v.Timestamp
			}
			row, ok := v.Value.(map[UniqueID]interface{})
			if !ok {
				log.Warn("transfer interface to map wrong", zap.Strings("path", path))
				return errors.New("unexpected error")
			}

			// log.Debug("size check", zap.Int("size", rowSize), zap.Int("reflectSize", int(reflect.TypeOf(row).Size())))
			err = t.writeToBuffer(ctx, v.PK, row, int(reflect.TypeOf(row).Size()))
			if err != nil {
				return err
			}
			remained++
		}
	}

	log.Debug("split segment end",
		zap.Int64("remained entities", remained),
		zap.Int64("deleted entities", deleted),
		zap.Int64("expired entities", expired),
		zap.Duration("map elapse", time.Since(processStart)))
	return nil
}

func (t *level2CompactionTask) writeToBuffer(ctx context.Context, pk storage.PrimaryKey, row map[int64]interface{}, rowSize int) error {
	clusteringKey := row[t.plan.GetClusteringKeyId()]
	group := t.keyToGroupMap[clusteringKey]

	t.groupLocks.Lock(group)
	defer t.groupLocks.Unlock(group)
	// prepare
	if group.currentSegmentID == 0 {
		segmentID, err := t.allocator.AllocOne()
		if err != nil {
			return err
		}
		group.currentSegmentID = segmentID
		group.currentSpillBinlogs = make([]*datapb.FieldBinlog, 0)
		group.currentSpillStatslogs = make([]*datapb.FieldBinlog, 0)
	}
	if group.currentPKStats == nil {
		stats, err := storage.NewPrimaryKeyStats(t.primaryKeyField.FieldID, int64(t.primaryKeyField.DataType), 1)
		if err != nil {
			return err
		}
		group.currentPKStats = stats
	}
	if group.currentClusteringKeyStats == nil {
		fieldStats, err := storage.NewFieldStats(t.clusteringKeyField.FieldID, t.clusteringKeyField.DataType, 0)
		if err != nil {
			return err
		}
		group.currentClusteringKeyStats = fieldStats
	}
	if group.buffer == nil {
		writeBuffer, err := storage.NewInsertData(t.collectionMeta.GetSchema())
		if err != nil {
			return err
		}
		group.buffer = writeBuffer
	}

	group.buffer.Append(row)
	group.bufferSize = group.bufferSize + int64(rowSize)
	group.bufferRowNum = group.bufferRowNum + 1
	group.currentPKStats.Update(pk)
	group.currentClusteringKeyStats.Update(storage.NewScalarFieldValue(t.clusteringKeyField.DataType, row[t.clusteringKeyField.FieldID]))
	totalBufferSize := t.totalBufferSize.Add(int64(rowSize))

	// trigger spill
	if group.bufferRowNum > t.plan.L2SegmentMaxRows {
		t.spillChan <- SpillSignal{
			key: clusteringKey,
		}
	} else if totalBufferSize >= t.getMemoryBufferSize() {
		t.spillChan <- SpillSignal{}
		// block here, wait for memory release by spill
	loop:
		for {
			select {
			case <-ctx.Done():
				log.Warn("stop waiting for memory buffer release as context done")
				return nil
			case <-t.done:
				log.Warn("stop waiting for memory buffer release as task chan done")
				return nil
			default:
				currentSize := t.totalBufferSize.Load()
				if currentSize < t.getSpillMemorySizeThreshold() {
					break loop
				}
				time.Sleep(time.Millisecond * 50)
			}
		}
	} else if totalBufferSize >= t.getSpillMemorySizeThreshold() {
		// if totalBufferSize reaches memorybufferSize * 0.8, trigger spill without block
		t.spillChan <- SpillSignal{}
	}
	return nil
}

// getMemoryBufferSize return memoryBufferSize
func (t *level2CompactionTask) getMemoryBufferSize() int64 {
	return int64(float64(hardware.GetMemoryCount()) * Params.DataNodeCfg.L2CompactionMemoryBufferRatio.GetAsFloat())
}

func (t *level2CompactionTask) getSpillMemorySizeThreshold() int64 {
	return int64(float64(t.getMemoryBufferSize()) * 0.8)
}

func (t *level2CompactionTask) backgroundSpill(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Info("l2 compaction task context exit")
			return
		case <-t.done:
			log.Info("l2 compaction task done")
			return
		case signal := <-t.spillChan:
			var err error
			if signal.key == nil {
				err = t.spillTopGroups(ctx)
			} else {
				err = t.spill(ctx, t.keyToGroupMap[signal.key])
			}
			if err != nil {
				log.Warn("fail to spill data", zap.Error(err))
				// todo handle error
			}
		}
	}
}

func (t *level2CompactionTask) spillTopGroups(ctx context.Context) error {
	groupIDs := make([]int, 0)
	for _, group := range t.groups {
		groupIDs = append(groupIDs, group.id)
	}
	sort.Slice(groupIDs, func(i, j int) bool {
		return t.groups[i].bufferSize > t.groups[j].bufferSize
	})
	var spilledSize int64 = 0
	for _, id := range groupIDs {
		spillSize := t.groups[id].bufferSize
		err := t.spill(ctx, t.groups[id])
		if err != nil {
			return err
		}
		spilledSize += spillSize
		if spilledSize >= t.getMemoryBufferSize()/2 {
			break
		}
	}
	return nil
}

func (t *level2CompactionTask) forceSpillAll(ctx context.Context) error {
	for _, group := range t.groups {
		err := t.spill(ctx, group)
		if err != nil {
			log.Error("spill fail")
			return err
		}
		err = t.packGroupsToSegments(ctx, group)
		if err != nil {
			log.Error("generate CompactionSegment fail")
			return err
		}
	}
	t.totalBufferSize.Store(0)
	return nil
}

func (t *level2CompactionTask) packGroupsToSegments(ctx context.Context, group *Group) error {
	t.groupLocks.Lock(group)
	defer t.groupLocks.Unlock(group)
	// pack current spill data into a segment
	seg := &datapb.CompactionSegment{
		PlanID:              t.plan.GetPlanID(),
		SegmentID:           group.currentSegmentID,
		NumOfRows:           group.currentSpillRowNum,
		InsertLogs:          group.currentSpillBinlogs,
		Field2StatslogPaths: group.currentSpillStatslogs,
		Channel:             t.plan.GetChannel(),
	}
	group.segments = append(group.segments, seg)
	group.segmentStats[group.currentSegmentID] = storage.SegmentStats{
		FieldStats: []storage.FieldStats{{
			FieldID:   group.currentClusteringKeyStats.FieldID,
			Type:      group.currentClusteringKeyStats.Type,
			Max:       group.currentClusteringKeyStats.Max,
			Min:       group.currentClusteringKeyStats.Min,
			BF:        group.currentClusteringKeyStats.BF,
			Centroids: group.currentClusteringKeyStats.Centroids,
		}},
		NumRows: int(group.currentSpillRowNum),
	}
	// refresh
	group.currentClusteringKeyStats = nil
	group.currentSpillRowNum = 0
	group.currentSpillSize = 0
	group.currentSegmentID = 0

	log.Debug("pack segment", zap.Any("seg", seg.String()))
	return nil
}

func (t *level2CompactionTask) spill(ctx context.Context, group *Group) error {
	t.groupLocks.Lock(group)
	defer t.groupLocks.Unlock(group)

	//if group.currentSpillSize+group.bufferSize > t.plan.GetPrefixSegmentMaxSize() {
	//	t.packGroupsToSegments(ctx, group)
	//}
	if group.currentSpillRowNum+group.bufferRowNum > t.plan.L2SegmentMaxRows {
		t.packGroupsToSegments(ctx, group)
	}

	fieldBinlogs, statsLogs, err := t.uploadSegment(ctx, t.collectionMeta, t.partitionID, group.currentSegmentID, group.buffer, group.currentPKStats, group.currentSpillRowNum)
	if err != nil {
		return err
	}
	group.currentPKStats = nil
	group.currentSpillRowNum = group.currentSpillRowNum + group.bufferRowNum
	group.currentSpillBinlogs = append(group.currentSpillBinlogs, fieldBinlogs...)
	group.currentSpillStatslogs = append(group.currentSpillStatslogs, statsLogs...)

	// clean buffer
	group.buffer = nil
	group.bufferSize = 0
	group.bufferRowNum = 0
	t.totalBufferSize.Add(-group.bufferSize)

	return nil
}

func (t *level2CompactionTask) uploadSegment(ctx context.Context, meta *etcdpb.CollectionMeta, partitionID, segmentID int64, insertData *storage.InsertData, stats *storage.PrimaryKeyStats, totalRows int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	iCodec := storage.NewInsertCodecWithSchema(meta)
	inlogs, err := iCodec.Serialize(t.partitionID, segmentID, insertData)
	if err != nil {
		return nil, nil, err
	}

	uploadFieldBinlogs := make([]*datapb.FieldBinlog, 0)
	notifyGenIdx := make(chan struct{})
	defer close(notifyGenIdx)

	generator, err := t.allocator.GetGenerator(len(inlogs)+1, notifyGenIdx)
	if err != nil {
		return nil, nil, err
	}

	uploadInsertKVs := make(map[string][]byte)
	for _, blob := range inlogs {
		// Blob Key is generated by Serialize from int64 fieldID in collection schema, which won't raise error in ParseInt
		fID, _ := strconv.ParseInt(blob.GetKey(), 10, 64)
		idPath := metautil.JoinIDPath(meta.GetID(), partitionID, segmentID, fID, <-generator)
		key := t.io.JoinFullPath(common.SegmentInsertLogPath, idPath)
		value := blob.GetValue()
		fileLen := len(value)
		uploadInsertKVs[key] = value
		uploadFieldBinlogs = append(uploadFieldBinlogs, &datapb.FieldBinlog{
			FieldID: fID,
			Binlogs: []*datapb.Binlog{{LogSize: int64(fileLen), LogPath: key, EntriesNum: blob.RowNum}},
		})
		log.Debug("upload segment insert log", zap.String("key", key))
	}

	err = t.io.Upload(ctx, uploadInsertKVs)
	if err != nil {
		return nil, nil, err
	}

	uploadStatsKVs := make(map[string][]byte)
	statBlob, err := iCodec.SerializePkStats(stats, totalRows)
	if err != nil {
		return nil, nil, err
	}
	fID, _ := strconv.ParseInt(statBlob.GetKey(), 10, 64)
	idPath := metautil.JoinIDPath(meta.GetID(), partitionID, segmentID, fID, <-generator)
	key := t.io.JoinFullPath(common.SegmentStatslogPath, idPath)
	value := statBlob.GetValue()
	uploadStatsKVs[key] = value
	fileLen := len(value)
	err = t.io.Upload(ctx, uploadStatsKVs)
	log.Debug("upload segment stats log", zap.String("key", key))
	if err != nil {
		return nil, nil, err
	}
	uploadStatslogs := []*datapb.FieldBinlog{
		{
			FieldID: fID,
			Binlogs: []*datapb.Binlog{{LogSize: int64(fileLen), LogPath: key, EntriesNum: totalRows}},
		},
	}

	return uploadFieldBinlogs, uploadStatslogs, nil
}

func (t *level2CompactionTask) getSegmentMeta(segID UniqueID) (UniqueID, UniqueID, *etcdpb.CollectionMeta, error) {
	collID := t.metaCache.Collection()
	seg, ok := t.metaCache.GetSegmentByID(segID)
	if !ok {
		return -1, -1, nil, merr.WrapErrSegmentNotFound(segID)
	}
	partID := seg.PartitionID()
	sch := t.metaCache.Schema()

	meta := &etcdpb.CollectionMeta{
		ID:     collID,
		Schema: sch,
	}
	return collID, partID, meta, nil
}

func (t *level2CompactionTask) uploadPartitionStats(ctx context.Context, collectionID, partitionID UniqueID, partitionStats *storage.PartitionStatsSnapshot) error {
	// use allocID as partitionStats file name
	newVersion, err := t.allocator.AllocOne()
	if err != nil {
		return err
	}
	partitionStats.Version = newVersion
	partitionStatsBytes, err := storage.SerializePartitionStatsSnapshot(partitionStats)
	if err != nil {
		return err
	}
	newStatsPath := t.io.JoinFullPath(common.PartitionStatsPath,
		path.Join(strconv.FormatInt(collectionID, 10), strconv.FormatInt(partitionID, 10), t.plan.GetChannel(), strconv.FormatInt(newVersion, 10)))
	kv := map[string][]byte{
		newStatsPath: partitionStatsBytes,
	}
	err = t.io.Upload(ctx, kv)
	if err != nil {
		return err
	}
	log.Info("Finish upload PartitionStats file", zap.String("key", newStatsPath), zap.Int("length", len(partitionStatsBytes)))
	return nil
}

// cleanUpStep try best to clean all temp datas
func (t *level2CompactionTask) cleanUpStep(ctx context.Context) {
	stagePath := t.stageIO.JoinFullPath(common.CompactionStagePath, metautil.JoinIDPath(t.plan.PlanID))
	err := t.stageIO.Remove(ctx, stagePath)
	if err != nil {
		log.Warn("Fail to remove staging data", zap.String("key", stagePath), zap.Error(err))
	}
}

func (t *level2CompactionTask) analyzeStep(ctx context.Context, meta *etcdpb.CollectionMeta) (map[interface{}]int64, error) {
	flightSegments := t.plan.GetSegmentBinlogs()
	futures := make([]*conc.Future[any], 0, len(flightSegments))
	mapStart := time.Now()
	var mutex sync.Mutex
	analyzeDict := make(map[interface{}]int64, 0)
	for _, segment := range flightSegments {
		segmentClone := &datapb.CompactionSegmentBinlogs{
			SegmentID:           segment.SegmentID,
			FieldBinlogs:        segment.FieldBinlogs,
			Field2StatslogPaths: segment.Field2StatslogPaths,
			Deltalogs:           segment.Deltalogs,
			InsertChannel:       segment.InsertChannel,
			Level:               segment.Level,
			CollectionID:        segment.CollectionID,
			PartitionID:         segment.PartitionID,
		}
		future := t.pool.Submit(func() (any, error) {
			analyzeResult, err := t.analyzeSegment(ctx, segmentClone)
			mutex.Lock()
			defer mutex.Unlock()
			for key, v := range analyzeResult {
				if _, exist := analyzeDict[key]; exist {
					analyzeDict[key] = analyzeDict[key] + v
				} else {
					analyzeDict[key] = v
				}
			}
			return struct{}{}, err
		})
		futures = append(futures, future)
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return nil, err
	}
	log.Info("compact analyze step end",
		zap.Int64("collectionID", t.getCollection()),
		zap.Int64("partitionID", t.partitionID),
		zap.Int("segments", len(flightSegments)),
		zap.Duration("elapse", time.Since(mapStart)))
	return analyzeDict, nil
}

func (t *level2CompactionTask) analyzeSegment(
	ctx context.Context,
	segment *datapb.CompactionSegmentBinlogs,
) (map[interface{}]int64, error) {
	log := log.With(zap.Int64("planID", t.getPlanID()), zap.Int64("segmentID", segment.GetSegmentID()))

	// vars
	processStart := time.Now()
	fieldBinlogPaths := make([][]string, 0)
	// initial timestampFrom, timestampTo = -1, -1 is an illegal value, only to mark initial state
	var (
		timestampTo   int64                 = -1
		timestampFrom int64                 = -1
		expired       int64                 = 0
		deleted       int64                 = 0
		remained      int64                 = 0
		analyzeResult map[interface{}]int64 = make(map[interface{}]int64, 0)
	)

	// Get the number of field binlog files from non-empty segment
	var binlogNum int
	for _, b := range segment.GetFieldBinlogs() {
		if b != nil {
			binlogNum = len(b.GetBinlogs())
			break
		}
	}
	// Unable to deal with all empty segments cases, so return error
	if binlogNum == 0 {
		log.Warn("compact wrong, all segments' binlogs are empty")
		return nil, errIllegalCompactionPlan
	}
	log.Info("binlogNum", zap.Int("binlogNum", binlogNum))
	for idx := 0; idx < binlogNum; idx++ {
		var ps []string
		for _, f := range segment.GetFieldBinlogs() {
			// todo add a new reader only read one column
			if f.FieldID == t.primaryKeyField.GetFieldID() || f.FieldID == t.clusteringKeyField.GetFieldID() || f.FieldID == common.RowIDField || f.FieldID == common.TimeStampField {
				ps = append(ps, f.GetBinlogs()[idx].GetLogPath())
			}
		}
		fieldBinlogPaths = append(fieldBinlogPaths, ps)
	}
	log.Info("fieldBinlogPaths", zap.Int("length", len(fieldBinlogPaths)), zap.Any("fieldBinlogPaths", fieldBinlogPaths))

	for _, path := range fieldBinlogPaths {
		bytesArr, err := t.io.Download(ctx, path)
		blobs := make([]*Blob, len(bytesArr))
		for i := range bytesArr {
			blobs[i] = &Blob{Value: bytesArr[i]}
		}
		if err != nil {
			log.Warn("download insertlogs wrong", zap.Strings("path", path), zap.Error(err))
			return nil, err
		}

		//iter, err := storage.NewBinlogDeserializeReader(blobs)
		//if err != nil {
		//	log.Warn("new insert binlogs reader wrong", zap.Strings("path", path), zap.Error(err))
		//	return nil, err
		//}
		//
		//for {
		//	err := iter.Next()
		//	if err != nil {
		//		if err == sio.EOF {
		//			break
		//		} else {
		//			log.Warn("transfer interface to Value wrong", zap.Strings("path", path))
		//			return nil, errors.New("unexpected error")
		//		}
		//	}
		//	v := iter.Value()
		//
		//	ts := Timestamp(v.Timestamp)
		//	// Filtering expired entity
		//	if IsExpiredEntity(t.plan.GetCollectionTtl(), ts, t.currentTs) {
		//		expired++
		//		continue
		//	}
		//
		//	// Update timestampFrom, timestampTo
		//	if v.Timestamp < timestampFrom || timestampFrom == -1 {
		//		timestampFrom = v.Timestamp
		//	}
		//	if v.Timestamp > timestampTo || timestampFrom == -1 {
		//		timestampTo = v.Timestamp
		//	}
		//
		//	row, ok := v.Value.(map[UniqueID]interface{})
		//	if !ok {
		//		log.Warn("transfer interface to map wrong", zap.Strings("path", path))
		//		return nil, errors.New("unexpected error")
		//	}
		//	key := row[fID]
		//	if _, exist := analyzeResult[key]; exist {
		//		analyzeResult[key] = analyzeResult[key] + 1
		//	} else {
		//		analyzeResult[key] = 1
		//	}
		//	remained++
		//}

		pkIter, err := storage.NewInsertBinlogIterator(blobs, t.primaryKeyField.GetFieldID(), t.primaryKeyField.GetDataType())
		if err != nil {
			log.Warn("new insert binlogs Itr wrong", zap.Strings("path", path), zap.Error(err))
			return nil, err
		}

		//log.Info("pkIter.RowNum()", zap.Int("pkIter.RowNum()", pkIter.RowNum()), zap.Bool("hasNext", pkIter.HasNext()))
		for pkIter.HasNext() {
			vInter, _ := pkIter.Next()
			v, ok := vInter.(*storage.Value)
			if !ok {
				log.Warn("transfer interface to Value wrong", zap.Strings("path", path))
				return nil, errors.New("unexpected error")
			}

			// Filtering expired entity
			ts := Timestamp(v.Timestamp)
			if IsExpiredEntity(t.plan.GetCollectionTtl(), ts, t.currentTs) {
				expired++
				continue
			}

			// Update timestampFrom, timestampTo
			if v.Timestamp < timestampFrom || timestampFrom == -1 {
				timestampFrom = v.Timestamp
			}
			if v.Timestamp > timestampTo || timestampFrom == -1 {
				timestampTo = v.Timestamp
			}
			//rowValue := vIter.GetData().(*iterators.InsertRow).GetValue()
			row, ok := v.Value.(map[UniqueID]interface{})
			if !ok {
				log.Warn("transfer interface to map wrong", zap.Strings("path", path))
				return nil, errors.New("unexpected error")
			}
			key := row[t.clusteringKeyField.GetFieldID()]
			if _, exist := analyzeResult[key]; exist {
				analyzeResult[key] = analyzeResult[key] + 1
			} else {
				analyzeResult[key] = 1
			}
			remained++
		}
	}

	log.Info("analyze segment end",
		zap.Int64("remained entities", remained),
		zap.Int64("deleted entities", deleted),
		zap.Int64("expired entities", expired),
		zap.Duration("map elapse", time.Since(processStart)))
	return analyzeResult, nil
}

// mapStep read and map input segments iteratively, spill data if need
func (t *level2CompactionTask) planStep(dict map[interface{}]int64) [][]interface{} {
	keys := lo.MapToSlice(dict, func(k interface{}, _ int64) interface{} {
		return k
	})
	sort.Slice(keys, func(i, j int) bool {
		return storage.NewScalarFieldValue(t.clusteringKeyField.DataType, keys[i]).LE(storage.NewScalarFieldValue(t.clusteringKeyField.DataType, keys[j]))
	})

	buckets := make([][]interface{}, 0)
	currentBucket := make([]interface{}, 0)
	var currentBucketSize int64 = 0
	var maxTres = t.plan.L2SegmentMaxRows
	var minTres = t.plan.L2SegmentMaxRows / 2
	for _, key := range keys {
		// todo can optimize
		if dict[key] > minTres {
			//if currentBucketSize + dict[key] < maxTres {
			//	currentBucket = append(currentBucket, key)
			//	buckets = append(buckets, currentBucket)
			//} else {
			//	buckets = append(buckets, currentBucket)
			//	buckets = append(buckets, []interface{}{key})
			//}
			buckets = append(buckets, currentBucket)
			buckets = append(buckets, []interface{}{key})
			currentBucket = make([]interface{}, 0)
			currentBucketSize = 0
		} else if currentBucketSize+dict[key] > maxTres {
			buckets = append(buckets, currentBucket)
			currentBucket = []interface{}{key}
			currentBucketSize = dict[key]
		} else if currentBucketSize+dict[key] > minTres {
			currentBucket = append(currentBucket, key)
			buckets = append(buckets, currentBucket)
			currentBucket = make([]interface{}, 0)
			currentBucketSize = 0
		} else {
			currentBucket = append(currentBucket, key)
			currentBucketSize += dict[key]
		}
	}
	buckets = append(buckets, currentBucket)
	return buckets
}
