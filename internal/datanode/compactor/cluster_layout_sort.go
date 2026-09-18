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

package compactor

import (
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	sio "io"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	flushio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	clusterLayoutSortKeySize       = 24 // uint32 centroid + float32 distance + two uint64 source ordinals
	clusterLayoutSortOverhead      = 4
	clusterLayoutMergeMaxAttempts  = 3
	clusterLayoutMaxRunReadBuffer  = 4 << 20
	clusterLayoutMinRunReadBuffer  = 4 << 10
	clusterLayoutMemoryBudgetRatio = 0.7
)

// clusterLayoutRange is the in-memory result of sorted layout materialization.
// The metadata publication layer serializes these ranges with output segments.
type clusterLayoutRange struct {
	SegmentID int64
	Offset    int64
	Size      int64
}

type clusterLayoutResult struct {
	CentroidRanges map[uint32][]clusterLayoutRange
}

func newClusterLayoutResult() *clusterLayoutResult {
	return &clusterLayoutResult{CentroidRanges: make(map[uint32][]clusterLayoutRange)}
}

// Validate checks that the ranges cover every output row exactly once. It does
// not compare with the analyze-time counts because deletes and TTL can remove
// rows while compaction is running.
func (r *clusterLayoutResult) Validate(segments []*datapb.CompactionSegment) error {
	if r == nil {
		return merr.WrapErrServiceInternalMsg("cluster layout result is nil")
	}
	segmentRows := make(map[int64]int64, len(segments))
	for _, segment := range segments {
		if segment == nil || segment.GetSegmentID() <= 0 {
			return merr.WrapErrServiceInternalMsg("cluster layout output contains an invalid segment")
		}
		if segment.GetNumOfRows() < 0 {
			return merr.WrapErrServiceInternalMsg(
				"cluster layout output segment %d has negative row count %d",
				segment.GetSegmentID(), segment.GetNumOfRows())
		}
		if _, ok := segmentRows[segment.GetSegmentID()]; ok {
			return merr.WrapErrServiceInternalMsg("cluster layout output contains duplicate segment %d", segment.GetSegmentID())
		}
		segmentRows[segment.GetSegmentID()] = segment.GetNumOfRows()
	}

	type interval struct {
		offset int64
		size   int64
	}
	bySegment := make(map[int64][]interval, len(segmentRows))
	for centroidID, ranges := range r.CentroidRanges {
		seenSegments := make(map[int64]struct{}, len(ranges))
		for _, current := range ranges {
			rowCount, ok := segmentRows[current.SegmentID]
			if !ok {
				return merr.WrapErrServiceInternalMsg(
					"cluster layout centroid %d references unknown segment %d",
					centroidID, current.SegmentID)
			}
			if current.Offset < 0 || current.Size <= 0 || current.Offset > math.MaxInt64-current.Size {
				return merr.WrapErrServiceInternalMsg(
					"cluster layout centroid %d has invalid range segment=%d offset=%d size=%d",
					centroidID, current.SegmentID, current.Offset, current.Size)
			}
			if current.Offset+current.Size > rowCount {
				return merr.WrapErrServiceInternalMsg(
					"cluster layout centroid %d range exceeds segment %d rows: offset=%d size=%d rows=%d",
					centroidID, current.SegmentID, current.Offset, current.Size, rowCount)
			}
			if _, ok := seenSegments[current.SegmentID]; ok {
				return merr.WrapErrServiceInternalMsg(
					"cluster layout centroid %d has multiple ranges in segment %d",
					centroidID, current.SegmentID)
			}
			seenSegments[current.SegmentID] = struct{}{}
			bySegment[current.SegmentID] = append(bySegment[current.SegmentID], interval{
				offset: current.Offset,
				size:   current.Size,
			})
		}
	}

	for segmentID, rowCount := range segmentRows {
		intervals := bySegment[segmentID]
		sort.Slice(intervals, func(i, j int) bool { return intervals[i].offset < intervals[j].offset })
		var covered int64
		for _, current := range intervals {
			if current.offset != covered {
				return merr.WrapErrServiceInternalMsg(
					"cluster layout ranges do not exactly cover segment %d at offset %d",
					segmentID, covered)
			}
			covered += current.size
		}
		if covered != rowCount {
			return merr.WrapErrServiceInternalMsg(
				"cluster layout ranges cover %d rows in segment %d, expected %d",
				covered, segmentID, rowCount)
		}
	}
	return nil
}

func (t *clusteringCompactionTask) useClusterLayoutSort() bool {
	return t.isVectorClusteringKey && t.layoutPlan != nil
}

// clusterLayoutSortKey orders rows by the public layout key and then by their
// input position. The latter makes equal (centroid, distance) keys stable across
// sub-runs and input segments.
type clusterLayoutSortKey struct {
	centroidID          uint32
	distance            float32
	sourceSegmentOffset uint64
	sourceRowOffset     uint64
}

func (k clusterLayoutSortKey) less(other clusterLayoutSortKey) bool {
	if k.centroidID != other.centroidID {
		return k.centroidID < other.centroidID
	}
	if k.distance != other.distance {
		return k.distance < other.distance
	}
	if k.sourceSegmentOffset != other.sourceSegmentOffset {
		return k.sourceSegmentOffset < other.sourceSegmentOffset
	}
	return k.sourceRowOffset < other.sourceRowOffset
}

func encodeClusterLayoutSortKey(dst []byte, key clusterLayoutSortKey) {
	binary.LittleEndian.PutUint32(dst[0:4], key.centroidID)
	binary.LittleEndian.PutUint32(dst[4:8], math.Float32bits(key.distance))
	binary.LittleEndian.PutUint64(dst[8:16], key.sourceSegmentOffset)
	binary.LittleEndian.PutUint64(dst[16:24], key.sourceRowOffset)
}

func decodeClusterLayoutSortKey(src []byte) clusterLayoutSortKey {
	return clusterLayoutSortKey{
		centroidID:          binary.LittleEndian.Uint32(src[0:4]),
		distance:            math.Float32frombits(binary.LittleEndian.Uint32(src[4:8])),
		sourceSegmentOffset: binary.LittleEndian.Uint64(src[8:16]),
		sourceRowOffset:     binary.LittleEndian.Uint64(src[16:24]),
	}
}

type clusterLayoutSortRow struct {
	value *storage.Value
	key   clusterLayoutSortKey
}

type clusterLayoutSpillRun struct {
	localSegments []*datapb.CompactionSegment
	keysPath      string
	rowCount      int64
}

type clusterLayoutSpiller struct {
	task         *clusteringCompactionTask
	localCM      storage.ChunkManager
	localIO      flushio.BinlogIO
	root         string
	spillParams  compaction.Params
	spillStorage *indexpb.StorageConfig
	segAlloc     allocator.Interface
	logAlloc     allocator.Interface

	subRunRowBudget int64

	mu   sync.Mutex
	runs map[int][]*clusterLayoutSpillRun
}

func calculateClusterLayoutSubRunRows(memoryLimit int64, poolSize int, serializedRowBytes int) int64 {
	if poolSize < 1 {
		poolSize = 1
	}
	if serializedRowBytes < 1 {
		serializedRowBytes = 1024
	}
	effectiveRowBytes := int64(serializedRowBytes)
	if effectiveRowBytes > math.MaxInt64/clusterLayoutSortOverhead {
		return 1
	}
	effectiveRowBytes *= clusterLayoutSortOverhead
	workerBudget := int64(float64(memoryLimit)*clusterLayoutMemoryBudgetRatio) / int64(poolSize)
	if workerBudget <= effectiveRowBytes {
		return 1
	}
	return workerBudget / effectiveRowBytes
}

func newClusterLayoutSpiller(t *clusteringCompactionTask) (*clusterLayoutSpiller, error) {
	if t.compactionParams.StorageConfig == nil {
		return nil, merr.WrapErrServiceInternalMsg("cluster layout spill requires storage config")
	}
	localRoot := strings.TrimSpace(localClusterLayoutStorageRoot())
	if localRoot == "" || path.Clean(localRoot) == "/" {
		return nil, merr.WrapErrServiceInternalMsg("cluster layout spill requires a dedicated local storage root")
	}
	root := path.Join(localRoot, "cluster_layout_compaction", fmt.Sprintf("%d", t.GetPlanID()))
	if err := os.RemoveAll(root); err != nil {
		return nil, merr.Wrapf(err, "remove stale cluster layout spill directory %q", root)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, merr.Wrapf(err, "create cluster layout spill directory %q", root)
	}

	localCM := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	spillStorage := proto.Clone(t.compactionParams.StorageConfig).(*indexpb.StorageConfig)
	spillStorage.RootPath = root
	spillParams := t.compactionParams
	spillParams.StorageVersion = storage.StorageV1
	spillParams.StorageConfig = spillStorage

	rowBytes, err := typeutil.EstimateSizePerRecord(t.plan.GetSchema())
	if err != nil || rowBytes <= 0 {
		rowBytes = 1024
	}

	return &clusterLayoutSpiller{
		task:            t,
		localCM:         localCM,
		localIO:         flushio.NewBinlogIO(localCM),
		root:            root,
		spillParams:     spillParams,
		spillStorage:    spillStorage,
		segAlloc:        allocator.NewLocalAllocator(1, math.MaxInt64),
		logAlloc:        allocator.NewLocalAllocator(1, math.MaxInt64),
		subRunRowBudget: calculateClusterLayoutSubRunRows(t.memoryLimit, t.getSpillPoolSize(), rowBytes),
		runs:            make(map[int][]*clusterLayoutSpillRun),
	}, nil
}

func (s *clusterLayoutSpiller) addRun(groupID int, run *clusterLayoutSpillRun) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[groupID] = append(s.runs[groupID], run)
}

func (s *clusterLayoutSpiller) runReadBufferSize(runCount int) int64 {
	if runCount < 1 {
		runCount = 1
	}
	mergeWorkers := s.task.getWorkerPoolSize()
	budget := int64(float64(s.task.memoryLimit)*(1-clusterLayoutMemoryBudgetRatio)) /
		int64(mergeWorkers) / int64(runCount)
	if budget < clusterLayoutMinRunReadBuffer {
		return clusterLayoutMinRunReadBuffer
	}
	if budget > clusterLayoutMaxRunReadBuffer {
		return clusterLayoutMaxRunReadBuffer
	}
	return budget
}

func (s *clusterLayoutSpiller) cleanup(ctx context.Context) {
	if err := os.RemoveAll(s.root); err != nil {
		mlog.Warn(ctx, "failed to clean up cluster layout spill directory",
			mlog.String("root", s.root), mlog.Err(err))
	}
}

// mappingClusterLayoutSorted performs the two-phase sorted rewrite. Its range
// result remains task-local until the metadata publication layer consumes it.
func (t *clusteringCompactionTask) mappingClusterLayoutSorted(ctx context.Context,
) ([]*datapb.CompactionSegment, *storage.PartitionStatsSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	spiller, err := newClusterLayoutSpiller(t)
	if err != nil {
		return nil, nil, err
	}
	defer spiller.cleanup(ctx)

	inputSegments := t.plan.GetSegmentBinlogs()
	spillFutures := make([]*conc.Future[any], 0, len(inputSegments))
	for segmentOffset, segment := range inputSegments {
		segmentOffset := segmentOffset
		segment := proto.Clone(segment).(*datapb.CompactionSegmentBinlogs)
		spillFutures = append(spillFutures, t.spillPool.Submit(func() (any, error) {
			return struct{}{}, t.spillClusterLayoutSegment(ctx, spiller, segmentOffset, segment)
		}))
	}
	if err := conc.AwaitAll(spillFutures...); err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	groupResults := make([]*clusterLayoutResult, len(t.clusterBuffers))
	mergeFutures := make([]*conc.Future[any], 0, len(t.clusterBuffers))
	for groupID, buffer := range t.clusterBuffers {
		groupID, buffer := groupID, buffer
		groupResult := newClusterLayoutResult()
		groupResults[groupID] = groupResult
		mergeFutures = append(mergeFutures, t.mappingPool.Submit(func() (any, error) {
			return struct{}{}, t.mergeClusterLayoutGroupWithRetry(ctx, spiller, groupID, buffer, groupResult)
		}))
	}
	if err := conc.AwaitAll(mergeFutures...); err != nil {
		return nil, nil, err
	}

	result := newClusterLayoutResult()
	for _, groupResult := range groupResults {
		for centroidID, ranges := range groupResult.CentroidRanges {
			if _, ok := result.CentroidRanges[centroidID]; ok {
				return nil, nil, merr.WrapErrServiceInternalMsg(
					"cluster layout merge produced centroid %d in multiple groups", centroidID)
			}
			result.CentroidRanges[centroidID] = ranges
		}
	}

	segments, partitionStats := t.collectClusterLayoutBufferResults()
	if err := result.Validate(segments); err != nil {
		return nil, nil, err
	}
	t.layoutResult = result
	mlog.Info(ctx, "cluster layout sorted mapping finished",
		mlog.Int("segmentFrom", len(inputSegments)),
		mlog.Int("segmentTo", len(segments)),
		mlog.Int("centroidCount", len(result.CentroidRanges)))
	return segments, partitionStats, nil
}

func (t *clusteringCompactionTask) spillClusterLayoutSegment(
	ctx context.Context,
	spiller *clusterLayoutSpiller,
	segmentOffset int,
	segment *datapb.CompactionSegmentBinlogs,
) error {
	logger := mlog.With(mlog.Int64("planID", t.GetPlanID()), mlog.Int64("segmentID", segment.GetSegmentID()))
	if err := ctx.Err(); err != nil {
		return err
	}

	delta, err := compaction.ComposeDeleteFromDeltalogs(ctx, t.primaryKeyField.DataType, segment,
		storage.WithDownloader(t.binlogIO.Download),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
	)
	if err != nil {
		return err
	}
	entityFilter := compaction.NewEntityFilter(delta, t.plan.GetCollectionTtl(), t.currentTime, segment.GetCommitTimestamp())

	mappingPath, ok := t.segmentIDOffsetMapping[segment.GetSegmentID()]
	if !ok {
		return merr.WrapErrServiceInternalMsg("missing clustering assignment artifact for segment %d", segment.GetSegmentID())
	}
	mappingBytes, err := t.binlogIO.Download(ctx, []string{mappingPath})
	if err != nil {
		return err
	}
	if len(mappingBytes) != 1 {
		return merr.WrapErrServiceInternalMsg(
			"expected one clustering assignment artifact for segment %d, got %d",
			segment.GetSegmentID(), len(mappingBytes))
	}
	mappingStats := &clusteringpb.ClusteringCentroidIdMappingStats{}
	if err := proto.Unmarshal(mappingBytes[0], mappingStats); err != nil {
		return merr.WrapErrServiceInternalErr(err, "failed to decode clustering assignment artifact for segment %d", segment.GetSegmentID())
	}
	if err := clustercompaction.ValidateCentroidMappingStats(
		mappingStats,
		int64(len(mappingStats.GetCentroidIdMapping())),
		t.layoutPlan.CentroidCount,
	); err != nil {
		return err
	}
	idMapping := mappingStats.GetCentroidIdMapping()
	distances := mappingStats.GetDistanceToCentroid()

	rr, existingFields, err := newCompactionSegmentRecordReader(ctx, segment, t.plan.Schema, t.compactionParams.StorageConfig,
		storage.WithDownloader(t.binlogIO.Download),
		storage.WithCollectionID(t.GetCollection()),
		storage.WithVersion(segment.StorageVersion),
		storage.WithBufferSize(t.bufferSize),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
	)
	if err != nil {
		return err
	}
	materializer, err := NewRecordMaterializer(t.plan.Schema, t.plan.Schema.GetFunctions(), existingFields)
	if err != nil {
		rr.Close()
		return err
	}
	rr = newMaterializedRecordReader(rr, materializer)
	rr = wrapReaderWithTimestampOverwrite(rr, segment.GetCommitTimestamp())
	defer rr.Close()

	hasTTLField := t.ttlFieldID >= common.StartOfUserFieldID
	partitions := make(map[int][]clusterLayoutSortRow)
	var accumulatedRows int64
	subRunOffset := 0
	flushSubRuns := func() error {
		groupIDs := make([]int, 0, len(partitions))
		for groupID := range partitions {
			groupIDs = append(groupIDs, groupID)
		}
		sort.Ints(groupIDs)
		for _, groupID := range groupIDs {
			run, err := spiller.writeRun(ctx, segment.GetSegmentID(), groupID, subRunOffset, partitions[groupID])
			if err != nil {
				return err
			}
			spiller.addRun(groupID, run)
		}
		partitions = make(map[int][]clusterLayoutSortRow)
		accumulatedRows = 0
		subRunOffset++
		return nil
	}

	rowOffset := int64(-1)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, err := rr.Next()
		if err != nil {
			if err == sio.EOF {
				break
			}
			return err
		}
		values := make([]*storage.Value, record.Len())
		if err := storage.ValueDeserializerWithSchema(record, values, t.plan.Schema, true); err != nil {
			return err
		}
		for _, value := range values {
			rowOffset++
			if rowOffset >= int64(len(idMapping)) {
				return merr.WrapErrServiceInternalMsg(
					"row offset exceeds clustering assignment artifact, segment=%d offset=%d assignments=%d",
					segment.GetSegmentID(), rowOffset, len(idMapping))
			}
			row, ok := value.Value.(map[typeutil.UniqueID]interface{})
			if !ok {
				return merr.WrapErrServiceInternalMsg("unexpected cluster layout row type")
			}
			expireTs := int64(-1)
			if hasTTLField {
				if raw, exists := row[t.ttlFieldID]; exists {
					if ts, ok := raw.(int64); ok {
						expireTs = ts
					}
				}
			}
			if entityFilter.Filtered(value.PK.GetValue(), uint64(value.Timestamp), expireTs) {
				continue
			}

			centroidID := idMapping[rowOffset]
			groupID, ok := t.centroidGroupIndex[centroidID]
			if !ok {
				return merr.WrapErrServiceInternalMsg(
					"centroid %d is not covered by cluster layout plan", centroidID)
			}
			partitions[groupID] = append(partitions[groupID], clusterLayoutSortRow{
				value: value,
				key: clusterLayoutSortKey{
					centroidID:          centroidID,
					distance:            distances[rowOffset],
					sourceSegmentOffset: uint64(segmentOffset),
					sourceRowOffset:     uint64(rowOffset),
				},
			})
			accumulatedRows++
			if accumulatedRows >= spiller.subRunRowBudget {
				if err := flushSubRuns(); err != nil {
					return err
				}
			}
		}
	}
	if rowOffset+1 != int64(len(idMapping)) {
		return merr.WrapErrServiceInternalMsg(
			"clustering assignment row count mismatch, segment=%d rows=%d assignments=%d",
			segment.GetSegmentID(), rowOffset+1, len(idMapping))
	}
	if len(partitions) > 0 {
		if err := flushSubRuns(); err != nil {
			return err
		}
	}
	logger.Info(ctx, "cluster layout spill finished",
		mlog.Int("subRuns", subRunOffset),
		mlog.Int64("rowBudgetPerSubRun", spiller.subRunRowBudget))
	return nil
}

func (s *clusterLayoutSpiller) writeRun(
	ctx context.Context,
	inputSegmentID int64,
	groupID int,
	subRunOffset int,
	rows []clusterLayoutSortRow,
) (*clusterLayoutSpillRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].key.less(rows[j].key) })

	task := s.task
	writer, err := NewMultiSegmentWriter(
		ctx,
		s.localIO,
		NewCompactionAllocator(s.segAlloc, s.logAlloc),
		math.MaxInt64,
		task.plan.GetSchema(),
		s.spillParams,
		task.plan.GetMaxSegmentRows(),
		task.partitionID,
		task.collectionID,
		task.plan.GetChannel(),
		100,
		storage.WithBufferSize(task.bufferSize),
		storage.WithStorageConfig(s.spillStorage),
	)
	if err != nil {
		return nil, err
	}

	keyBytes := make([]byte, len(rows)*clusterLayoutSortKeySize)
	for offset, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := writer.WriteValue(row.value); err != nil {
			return nil, err
		}
		encodeClusterLayoutSortKey(keyBytes[offset*clusterLayoutSortKeySize:], row.key)
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	localSegments := writer.GetCompactionSegments()
	if len(localSegments) != 1 {
		return nil, merr.WrapErrServiceInternalMsg(
			"cluster layout spill run produced %d local segments, expected 1", len(localSegments))
	}

	keysPath := path.Join(s.root, "keys", fmt.Sprintf(
		"segment_%d_group_%d_subrun_%d.keys", inputSegmentID, groupID, subRunOffset))
	if err := s.localCM.Write(ctx, keysPath, keyBytes); err != nil {
		return nil, merr.Wrapf(err, "write cluster layout spill keys %q", keysPath)
	}
	task.writtenRowNum.Add(int64(len(rows)))
	return &clusterLayoutSpillRun{
		localSegments: localSegments,
		keysPath:      keysPath,
		rowCount:      int64(len(rows)),
	}, nil
}

func (t *clusteringCompactionTask) mergeClusterLayoutGroup(
	ctx context.Context,
	spiller *clusterLayoutSpiller,
	groupID int,
	buffer *ClusterBuffer,
	result *clusterLayoutResult,
) error {
	runs := spiller.runs[groupID]
	if len(runs) == 0 {
		return nil
	}

	readBufferSize := spiller.runReadBufferSize(len(runs))
	mergeHeap := &clusterLayoutMergeHeap{}
	heap.Init(mergeHeap)
	readers := make([]*clusterLayoutRunReader, 0, len(runs))
	defer func() {
		for _, reader := range readers {
			reader.close()
		}
	}()
	for _, run := range runs {
		reader, err := newClusterLayoutRunReader(ctx, spiller, t, run, readBufferSize)
		if err != nil {
			return err
		}
		readers = append(readers, reader)
		hasValue, err := reader.advance()
		if err != nil {
			return err
		}
		if hasValue {
			heap.Push(mergeHeap, reader)
		}
	}

	tracker := newClusterLayoutRangeTracker(buffer.writer, result.CentroidRanges)
	for mergeHeap.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		reader := heap.Pop(mergeHeap).(*clusterLayoutRunReader)
		if err := tracker.write(reader.currentValue, reader.currentKey.centroidID); err != nil {
			return err
		}
		hasValue, err := reader.advance()
		if err != nil {
			return err
		}
		if hasValue {
			heap.Push(mergeHeap, reader)
		}
	}
	tracker.finish()
	return nil
}

// Only final-writer close/upload failures are retried. Read or merge failures
// leave a live writer whose safe abort semantics are unknown, so retrying them
// could leak resources or publish ambiguous partial output.
func (t *clusteringCompactionTask) mergeClusterLayoutGroupWithRetry(
	ctx context.Context,
	spiller *clusterLayoutSpiller,
	groupID int,
	buffer *ClusterBuffer,
	result *clusterLayoutResult,
) error {
	for attempt := 0; attempt < clusterLayoutMergeMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 0 {
			writer, err := NewMultiSegmentWriter(
				ctx,
				t.binlogIO,
				NewCompactionAllocator(t.segIDAlloc, t.logIDAlloc),
				t.plan.GetMaxSize(),
				t.plan.GetSchema(),
				t.compactionParams,
				t.plan.GetMaxSegmentRows(),
				t.partitionID,
				t.collectionID,
				t.plan.GetChannel(),
				100,
				t.getWriterOpts()...,
			)
			if err != nil {
				return err
			}
			buffer.resetWriter(writer)
			clear(result.CentroidRanges)
		}

		if err := t.mergeClusterLayoutGroup(ctx, spiller, groupID, buffer, result); err != nil {
			return err
		}
		if err := buffer.Close(); err != nil {
			if attempt+1 == clusterLayoutMergeMaxAttempts {
				return err
			}
			mlog.Warn(ctx, "retrying cluster layout group finalization",
				mlog.Int64("planID", t.GetPlanID()),
				mlog.Int("groupID", groupID),
				mlog.Int("attempt", attempt+1),
				mlog.Err(err))
			continue
		}
		return nil
	}
	return merr.WrapErrServiceInternalMsg("cluster layout group %d merge attempts exhausted", groupID)
}

type clusterLayoutRangeWriter interface {
	WriteValue(*storage.Value) error
	CurrentSegmentID() typeutil.UniqueID
}

type clusterLayoutRangeTracker struct {
	writer clusterLayoutRangeWriter
	ranges map[uint32][]clusterLayoutRange

	segmentOffsets map[int64]int64
	segmentID      int64
	centroidID     uint32
	start          int64
	end            int64
	open           bool
}

func newClusterLayoutRangeTracker(
	writer clusterLayoutRangeWriter,
	ranges map[uint32][]clusterLayoutRange,
) *clusterLayoutRangeTracker {
	return &clusterLayoutRangeTracker{
		writer:         writer,
		ranges:         ranges,
		segmentOffsets: make(map[int64]int64),
		segmentID:      -1,
	}
}

func (t *clusterLayoutRangeTracker) write(value *storage.Value, centroidID uint32) error {
	if err := t.writer.WriteValue(value); err != nil {
		return err
	}
	segmentID := t.writer.CurrentSegmentID()
	offset := t.segmentOffsets[segmentID]
	t.segmentOffsets[segmentID] = offset + 1

	if t.open && (t.segmentID != segmentID || t.centroidID != centroidID) {
		t.flush()
	}
	if !t.open {
		t.segmentID = segmentID
		t.centroidID = centroidID
		t.start = offset
		t.open = true
	}
	t.end = offset
	return nil
}

func (t *clusterLayoutRangeTracker) flush() {
	if !t.open {
		return
	}
	t.ranges[t.centroidID] = append(t.ranges[t.centroidID], clusterLayoutRange{
		SegmentID: t.segmentID,
		Offset:    t.start,
		Size:      t.end - t.start + 1,
	})
	t.open = false
}

func (t *clusterLayoutRangeTracker) finish() {
	t.flush()
}

type clusterLayoutRunReader struct {
	ctx            context.Context
	spiller        *clusterLayoutSpiller
	task           *clusteringCompactionTask
	run            *clusterLayoutSpillRun
	readBufferSize int64

	keyReader storage.FileReader
	keyOffset int64
	segment   int
	record    storage.RecordReader
	batch     []*storage.Value
	batchPos  int

	currentValue *storage.Value
	currentKey   clusterLayoutSortKey
}

func newClusterLayoutRunReader(
	ctx context.Context,
	spiller *clusterLayoutSpiller,
	task *clusteringCompactionTask,
	run *clusterLayoutSpillRun,
	readBufferSize int64,
) (*clusterLayoutRunReader, error) {
	if run.rowCount < 0 || run.rowCount > math.MaxInt64/clusterLayoutSortKeySize {
		return nil, merr.WrapErrServiceInternalMsg("invalid cluster layout spill row count %d", run.rowCount)
	}
	keySize, err := spiller.localCM.Size(ctx, run.keysPath)
	if err != nil {
		return nil, err
	}
	expectedKeySize := run.rowCount * clusterLayoutSortKeySize
	if keySize != expectedKeySize {
		return nil, merr.WrapErrServiceInternalMsg(
			"cluster layout spill key size mismatch, path=%s expected=%d actual=%d",
			run.keysPath, expectedKeySize, keySize)
	}
	keyReader, err := spiller.localCM.Reader(ctx, run.keysPath)
	if err != nil {
		return nil, err
	}
	return &clusterLayoutRunReader{
		ctx:            ctx,
		spiller:        spiller,
		task:           task,
		run:            run,
		readBufferSize: readBufferSize,
		keyReader:      keyReader,
	}, nil
}

func (r *clusterLayoutRunReader) advance() (bool, error) {
	for r.batchPos >= len(r.batch) {
		hasBatch, err := r.nextBatch()
		if err != nil {
			return false, err
		}
		if !hasBatch {
			if r.keyOffset != r.run.rowCount {
				return false, merr.WrapErrServiceInternalMsg(
					"cluster layout spill produced fewer rows than keys, path=%s rows=%d keys=%d",
					r.run.keysPath, r.keyOffset, r.run.rowCount)
			}
			return false, nil
		}
	}
	if r.keyOffset >= r.run.rowCount {
		return false, merr.WrapErrServiceInternalMsg(
			"cluster layout spill produced more rows than keys, path=%s", r.run.keysPath)
	}
	var keyBytes [clusterLayoutSortKeySize]byte
	if _, err := sio.ReadFull(r.keyReader, keyBytes[:]); err != nil {
		return false, merr.Wrapf(err, "read cluster layout spill key %q", r.run.keysPath)
	}
	r.currentValue = r.batch[r.batchPos]
	r.batchPos++
	r.currentKey = decodeClusterLayoutSortKey(keyBytes[:])
	r.keyOffset++
	return true, nil
}

func (r *clusterLayoutRunReader) nextBatch() (bool, error) {
	for {
		if r.record == nil {
			if r.segment >= len(r.run.localSegments) {
				return false, nil
			}
			segment := r.run.localSegments[r.segment]
			r.segment++
			segmentBinlogs := &datapb.CompactionSegmentBinlogs{
				SegmentID:      segment.GetSegmentID(),
				FieldBinlogs:   segment.GetInsertLogs(),
				StorageVersion: segment.GetStorageVersion(),
				Manifest:       segment.GetManifest(),
			}
			record, _, err := newCompactionSegmentRecordReader(
				r.ctx,
				segmentBinlogs,
				r.task.plan.GetSchema(),
				r.spiller.spillStorage,
				storage.WithDownloader(r.spiller.localIO.Download),
				storage.WithCollectionID(r.task.collectionID),
				storage.WithVersion(segment.GetStorageVersion()),
				storage.WithBufferSize(r.readBufferSize),
				storage.WithStorageConfig(r.spiller.spillStorage),
			)
			if err != nil {
				return false, err
			}
			r.record = record
		}

		record, err := r.record.Next()
		if err != nil {
			if err == sio.EOF {
				r.record.Close()
				r.record = nil
				continue
			}
			return false, err
		}
		values := make([]*storage.Value, record.Len())
		if err := storage.ValueDeserializerWithSchema(record, values, r.task.plan.GetSchema(), true); err != nil {
			return false, err
		}
		if len(values) == 0 {
			continue
		}
		r.batch = values
		r.batchPos = 0
		return true, nil
	}
}

func (r *clusterLayoutRunReader) close() {
	if r.record != nil {
		r.record.Close()
		r.record = nil
	}
	if r.keyReader != nil {
		_ = r.keyReader.Close()
		r.keyReader = nil
	}
}

type clusterLayoutMergeHeap []*clusterLayoutRunReader

func (h clusterLayoutMergeHeap) Len() int           { return len(h) }
func (h clusterLayoutMergeHeap) Less(i, j int) bool { return h[i].currentKey.less(h[j].currentKey) }
func (h clusterLayoutMergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *clusterLayoutMergeHeap) Push(value interface{}) {
	*h = append(*h, value.(*clusterLayoutRunReader))
}

func (h *clusterLayoutMergeHeap) Pop() interface{} {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = nil
	*h = old[:last]
	return value
}

func (t *clusteringCompactionTask) collectClusterLayoutBufferResults() ([]*datapb.CompactionSegment, *storage.PartitionStatsSnapshot) {
	segments := make([]*datapb.CompactionSegment, 0)
	partitionStats := &storage.PartitionStatsSnapshot{
		SegmentStats: make(map[typeutil.UniqueID]storage.SegmentStats),
	}
	for _, buffer := range t.clusterBuffers {
		bufferSegments := buffer.GetCompactionSegments()
		segments = append(segments, bufferSegments...)
		for _, segment := range bufferSegments {
			partitionStats.SegmentStats[segment.GetSegmentID()] = storage.SegmentStats{
				FieldStats: []storage.FieldStats{buffer.clusteringKeyFieldStats.Clone()},
				NumRows:    int(segment.GetNumOfRows()),
			}
		}
	}
	return segments, partitionStats
}

func localClusterLayoutStorageRoot() string {
	return paramtable.Get().LocalStorageCfg.Path.GetValue()
}
