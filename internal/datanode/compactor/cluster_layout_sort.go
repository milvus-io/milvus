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
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	sio "io"
	"math"
	"path"
	"sort"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	flushio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const clusterLayoutSortKeySize = 24 // uint32 centroid + float32 distance + two uint64 source ordinals

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

// clusterLayoutSortKey orders rows by centroid and ascending squared-L2
// distance, matching the metric fixed by cluster Analyze. Input position makes
// equal keys stable across sub-runs and input segments.
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
	segments []*datapb.CompactionSegment
	keysPath string
	rowCount int64
}

type clusterLayoutSpiller struct {
	task         *clusteringCompactionTask
	cm           storage.ChunkManager
	binlogIO     flushio.BinlogIO
	root         string
	spillParams  compaction.Params
	spillStorage *indexpb.StorageConfig
	segAlloc     allocator.Interface
	logAlloc     allocator.Interface
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
		s.binlogIO,
		NewCompactionAllocator(s.segAlloc, s.logAlloc),
		math.MaxInt64,
		task.plan.GetSchema(),
		s.spillParams,
		int64(len(rows)), // temporary run bloom filter is sized to this run only
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
	segments := writer.GetCompactionSegments()
	if len(segments) != 1 {
		return nil, merr.WrapErrServiceInternalMsg(
			"cluster layout spill run produced %d run segments, expected 1", len(segments))
	}

	keysPath := path.Join(s.root, "keys", fmt.Sprintf(
		"segment_%d_group_%d_subrun_%d.keys", inputSegmentID, groupID, subRunOffset))
	if err := s.cm.Write(ctx, keysPath, keyBytes); err != nil {
		return nil, merr.Wrapf(err, "write cluster layout spill keys %q", keysPath)
	}
	task.writtenRowNum.Add(int64(len(rows)))
	return &clusterLayoutSpillRun{
		segments: segments,
		keysPath: keysPath,
		rowCount: int64(len(rows)),
	}, nil
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
	keyBuffer *bufio.Reader
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
	keySize, err := spiller.cm.Size(ctx, run.keysPath)
	if err != nil {
		return nil, err
	}
	expectedKeySize := run.rowCount * clusterLayoutSortKeySize
	if keySize != expectedKeySize {
		return nil, merr.WrapErrServiceInternalMsg(
			"cluster layout spill key size mismatch, path=%s expected=%d actual=%d",
			run.keysPath, expectedKeySize, keySize)
	}
	keyReader, err := spiller.cm.Reader(ctx, run.keysPath)
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
		keyBuffer:      bufio.NewReaderSize(keyReader, 64<<10),
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
	if _, err := sio.ReadFull(r.keyBuffer, keyBytes[:]); err != nil {
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
			if r.segment >= len(r.run.segments) {
				return false, nil
			}
			segment := r.run.segments[r.segment]
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
				storage.WithDownloader(r.spiller.binlogIO.Download),
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
