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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"sort"
	"strconv"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const clusterStatsVersion = 1

// A block has a 16-byte header followed by fixed-width keys. Blocks are
// independently bounded; a segment never requires buffering all of its keys.
const clusterStatsBlockRows = 4096

func clusterStatsKey(fieldID int64) string {
	return fmt.Sprintf("%s.%d", common.ClusterStats, fieldID)
}

func encodeClusterStatsBlock(keys []clusterLayoutSortKey) []byte {
	buf := make([]byte, 16+len(keys)*clusterLayoutSortKeySize)
	copy(buf, "CLST")
	binary.LittleEndian.PutUint32(buf[4:], clusterStatsVersion)
	binary.LittleEndian.PutUint64(buf[8:], uint64(len(keys)))
	for i, key := range keys {
		encodeClusterLayoutSortKey(buf[16+i*clusterLayoutSortKeySize:], key)
	}
	return buf
}

func decodeClusterStatsBlock(buf []byte) ([]clusterLayoutSortKey, error) {
	if len(buf) < 16 || string(buf[:4]) != "CLST" || binary.LittleEndian.Uint32(buf[4:]) != clusterStatsVersion {
		return nil, merr.WrapErrServiceInternalMsg("invalid cluster_stats header")
	}
	n := binary.LittleEndian.Uint64(buf[8:])
	if n > clusterStatsBlockRows || uint64(len(buf)-16) != n*clusterLayoutSortKeySize {
		return nil, merr.WrapErrServiceInternalMsg("invalid cluster_stats block length")
	}
	keys := make([]clusterLayoutSortKey, int(n))
	for i := range keys {
		keys[i] = decodeClusterLayoutSortKey(buf[16+i*clusterLayoutSortKeySize:])
		if math.IsNaN(float64(keys[i].distance)) || math.IsInf(float64(keys[i].distance), 0) || keys[i].distance < 0 {
			return nil, merr.WrapErrServiceInternalMsg("invalid squared L2 distance in cluster_stats")
		}
	}
	return keys, nil
}

// clusterStatsWriter serializes data, sidecar writes and segment rotation under
// ONE lock per batch. An error poisons the writer: callers must fail the task,
// never replay a partially appended batch. Flush/Close must use the same owner.
type clusterStatsWriter struct {
	mu        sync.Mutex
	writer    *MultiSegmentWriter
	template  *datapb.ClusterStats
	attempt   string
	refs      map[int64]*datapb.ClusterStats
	keys      []clusterLayoutSortKey
	segmentID int64
	tracker   *clusterLayoutRangeTracker
	err       error
	closed    bool
	keyLimit  int
	centroids map[uint32]struct{}
}

func newClusterStatsWriter(writer *MultiSegmentWriter, ref *datapb.ClusterStats) *clusterStatsWriter {
	w := &clusterStatsWriter{writer: writer, template: proto.Clone(ref).(*datapb.ClusterStats), attempt: uuid.NewString(), refs: make(map[int64]*datapb.ClusterStats), keyLimit: clusterStatsBlockRows, centroids: make(map[uint32]struct{}, len(ref.CentroidIds))}
	for _, id := range ref.CentroidIds {
		w.centroids[id] = struct{}{}
	}
	if ref.GetSorted() {
		w.tracker = newClusterLayoutRangeTracker(writer, make(map[uint32][]clusterLayoutRange))
	}
	return w
}

func (w *clusterStatsWriter) blockPath(segmentID int64, name string) string {
	if w.writer.params.StorageVersion == storage.StorageV3 {
		return path.Join(w.writer.params.StorageConfig.GetRootPath(), common.SegmentInsertLogPath,
			strconv.FormatInt(w.writer.collectionID, 10), strconv.FormatInt(w.writer.partitionID, 10), strconv.FormatInt(segmentID, 10),
			"_stats", clusterStatsKey(w.template.FieldId), w.attempt, name)
	}
	return path.Join(w.writer.params.StorageConfig.GetRootPath(), common.ClusterStats,
		strconv.FormatInt(w.writer.collectionID, 10), strconv.FormatInt(w.writer.partitionID, 10),
		strconv.FormatInt(segmentID, 10), w.attempt, name)
}

func (w *clusterStatsWriter) flushKeys(ctx context.Context) error {
	if len(w.keys) == 0 {
		return nil
	}
	ref := w.refs[w.segmentID]
	file := w.blockPath(w.segmentID, fmt.Sprintf("%d.keys", len(ref.Files)))
	if err := w.writer.binlogIO.Upload(ctx, map[string][]byte{file: encodeClusterStatsBlock(w.keys)}); err != nil {
		return err
	}
	ref.Files = append(ref.Files, file)
	w.keys = w.keys[:0]
	return nil
}

func (w *clusterStatsWriter) WriteBatch(ctx context.Context, rows []clusterLayoutSortRow) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return merr.WrapErrServiceInternalMsg("cluster writer is closed")
	}
	defer func() {
		if err != nil {
			w.err = err
		}
	}()
	for _, row := range rows {
		if err = ctx.Err(); err != nil {
			return err
		}
		if _, ok := w.centroids[row.key.centroidID]; !ok {
			return merr.WrapErrServiceInternalMsg("centroid %d is outside centroid group %d", row.key.centroidID, w.template.GroupId)
		}
		if w.tracker != nil {
			err = w.tracker.write(row.value, row.key.centroidID)
		} else {
			err = w.writer.WriteValue(row.value)
		}
		if err != nil {
			return err
		}
		segmentID := w.writer.CurrentSegmentID()
		if segmentID != w.segmentID {
			if err = w.flushKeys(ctx); err != nil {
				return err
			}
			w.segmentID = segmentID
			ref := proto.Clone(w.template).(*datapb.ClusterStats)
			ref.Files, ref.NumRows, ref.RangesPath = nil, 0, ""
			w.refs[segmentID] = ref
		}
		w.refs[segmentID].NumRows++
		w.keys = append(w.keys, row.key)
		if len(w.keys) == w.keyLimit {
			if err = w.flushKeys(ctx); err != nil {
				return err
			}
		}
	}
	// Release serializer row references at the batch boundary. This does not
	// seal a segment; the native/binlog writer still uses its IO buffer limit.
	return w.writer.Flush()
}

func (w *clusterStatsWriter) Close(ctx context.Context) (segments []*datapb.CompactionSegment, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return nil, w.err
	}
	if w.closed {
		return w.writer.GetCompactionSegments(), nil
	}
	defer func() {
		if err != nil {
			w.err = err
		}
	}()
	if err = w.flushKeys(ctx); err != nil {
		return nil, err
	}
	if err = w.writer.Close(); err != nil {
		return nil, err
	}
	segments = w.writer.GetCompactionSegments()
	if w.tracker != nil {
		w.tracker.finish()
		if err = (&clusterLayoutResult{CentroidRanges: w.tracker.ranges}).Validate(segments); err != nil {
			return nil, err
		}
	}
	bySegment := make(map[int64]map[uint32][]clusterLayoutRange)
	if w.tracker != nil {
		for centroid, ranges := range w.tracker.ranges {
			for _, r := range ranges {
				if bySegment[r.SegmentID] == nil {
					bySegment[r.SegmentID] = make(map[uint32][]clusterLayoutRange)
				}
				bySegment[r.SegmentID][centroid] = append(bySegment[r.SegmentID][centroid], r)
			}
		}
	}
	for _, segment := range segments {
		ref := w.refs[segment.GetSegmentID()]
		if ref == nil || ref.NumRows != segment.NumOfRows {
			return nil, merr.WrapErrServiceInternalMsg("cluster_stats/data row count mismatch for segment %d", segment.GetSegmentID())
		}
		extraBytes := 16*int64(len(ref.Files)) + ref.NumRows*clusterLayoutSortKeySize
		files := append([]string(nil), ref.Files...)
		if w.tracker != nil {
			ranges := bySegment[segment.GetSegmentID()]
			ref.CentroidIds = nil
			for centroid := range ranges {
				ref.CentroidIds = append(ref.CentroidIds, centroid)
			}
			sort.Slice(ref.CentroidIds, func(i, j int) bool { return ref.CentroidIds[i] < ref.CentroidIds[j] })
			payload, e := json.Marshal(ranges)
			if e != nil {
				return nil, e
			}
			ref.RangesPath = w.blockPath(segment.GetSegmentID(), "ranges.json")
			if err = w.writer.binlogIO.Upload(ctx, map[string][]byte{ref.RangesPath: payload}); err != nil {
				return nil, err
			}
			extraBytes += int64(len(payload))
			files = append(files, ref.RangesPath)
		}
		if segment.GetManifest() != "" {
			base, version, e := packed.UnmarshalManifestPath(segment.GetManifest())
			if e != nil {
				return nil, e
			}
			segment.Manifest, err = packed.CommitManifestUpdates(base, version, w.writer.params.StorageConfig,
				&packed.ManifestUpdates{Stats: []packed.StatEntry{{
					Key: clusterStatsKey(ref.FieldId), Files: files,
					Metadata: map[string]string{"version": "1", "sorted": strconv.FormatBool(ref.Sorted), "num_rows": strconv.FormatInt(ref.NumRows, 10), "memory_size": strconv.FormatInt(extraBytes, 10)},
				}}})
			if err != nil {
				return nil, err
			}
		}
		segment.ClusterStats = ref
		if segment.Stats != nil {
			segment.Stats.StatsBinlogSize += extraBytes
		}
	}
	w.closed = true
	return segments, nil
}

// Payload accounting for worker-owned rows. Reader/native/serializer memory is
// separate; this budget is not a process-RSS guarantee.
func clusterRowBytes(v *storage.Value) int64 {
	n := int64(128)
	if fields, ok := v.Value.(map[int64]interface{}); ok {
		for _, value := range fields {
			n += 64
			switch x := value.(type) {
			case []byte:
				n += int64(cap(x))
			case []float32:
				n += int64(cap(x)) * 4
			case []int8:
				n += int64(cap(x))
			case string:
				n += int64(len(x))
			case proto.Message:
				n += int64(proto.Size(x)) * 2
			}
		}
	}
	return n + clusterLayoutSortKeySize
}
