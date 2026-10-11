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

package storage

import (
	"bytes"
	"math"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const PartitionClusteringStatsFormatVersion uint32 = 1

// JSON can return a typed error from a nested FieldStats or range decoder.
// Preserve that classification rather than replacing it with a parse error.
func wrapPartitionStatsDecodeError(err error, context string) error {
	if merr.IsMilvusError(err) {
		return merr.Wrap(err, context)
	}
	return merr.WrapErrDataIntegrity(err, "%s", context)
}

// The existing JSON envelope stays unchanged. New protobuf values use protojson
// so VectorField's oneof and int64 ranges survive a round trip.
type partitionStatsJSON struct {
	SegmentStats    map[UniqueID]SegmentStats `json:"segmentStats"`
	Version         int64
	ClusteringStats json.RawMessage `json:"clusteringStats,omitempty"`
}

func (ps PartitionStatsSnapshot) MarshalJSON() ([]byte, error) {
	wire := partitionStatsJSON{SegmentStats: ps.SegmentStats, Version: ps.Version}
	if ps.ClusteringStats != nil {
		encoded, err := protojson.Marshal(ps.ClusteringStats)
		if err != nil {
			return nil, merr.WrapErrSerializationFailed(err, "encode partition clustering stats")
		}
		wire.ClusteringStats = encoded
	}
	return json.Marshal(wire)
}

func (ps *PartitionStatsSnapshot) UnmarshalJSON(data []byte) error {
	var wire partitionStatsJSON
	if err := json.Unmarshal(data, &wire); err != nil {
		return wrapPartitionStatsDecodeError(err, "decode partition stats snapshot")
	}
	// A missing segmentStats field used to dereference a nil RawMessage. Reject
	// it explicitly, but preserve legacy snapshots with a null segment map.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return merr.WrapErrDataIntegrity(err, "decode partition stats fields")
	}
	if _, ok := fields["segmentStats"]; !ok {
		return merr.WrapErrDataIntegrityMsg("partition stats snapshot has no segmentStats")
	}
	next := PartitionStatsSnapshot{SegmentStats: wire.SegmentStats, Version: wire.Version}
	if len(wire.ClusteringStats) != 0 && !bytes.Equal(bytes.TrimSpace(wire.ClusteringStats), []byte("null")) {
		next.ClusteringStats = &clusteringpb.PartitionClusteringStats{}
		if err := protojson.Unmarshal(wire.ClusteringStats, next.ClusteringStats); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode partition clustering stats")
		}
	}
	*ps = next
	return nil
}

type segmentStatsJSON struct {
	FieldStats     []FieldStats `json:"fieldStats"`
	NumRows        int
	CentroidRanges []json.RawMessage `json:"centroidRanges,omitempty"`
}

func (stats SegmentStats) MarshalJSON() ([]byte, error) {
	wire := segmentStatsJSON{FieldStats: stats.FieldStats, NumRows: stats.NumRows}
	for _, entry := range stats.CentroidRanges {
		if entry == nil {
			return nil, merr.WrapErrDataIntegrityMsg("nil centroid range")
		}
		encoded, err := protojson.Marshal(entry)
		if err != nil {
			return nil, merr.WrapErrSerializationFailed(err, "encode centroid range")
		}
		wire.CentroidRanges = append(wire.CentroidRanges, encoded)
	}
	return json.Marshal(wire)
}

func (stats *SegmentStats) UnmarshalJSON(data []byte) error {
	var wire segmentStatsJSON
	if err := json.Unmarshal(data, &wire); err != nil {
		return wrapPartitionStatsDecodeError(err, "decode segment stats")
	}
	next := SegmentStats{FieldStats: wire.FieldStats, NumRows: wire.NumRows}
	for _, encoded := range wire.CentroidRanges {
		if bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
			return merr.WrapErrDataIntegrityMsg("null centroid range")
		}
		entry := &clusteringpb.ClusteringCentroidRange{}
		if err := protojson.Unmarshal(encoded, entry); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode centroid range")
		}
		next.CentroidRanges = append(next.CentroidRanges, entry)
	}
	*stats = next
	return nil
}

// ValidateClusteringStats checks a self-contained vector layout. It does not
// compare this snapshot to a query target or change legacy scalar/vector stats.
// A valid layout must still be bound to its matching segment view before pruning.
func (ps *PartitionStatsSnapshot) ValidateClusteringStats() error {
	if ps == nil {
		return merr.WrapErrDataIntegrityMsg("nil partition stats snapshot")
	}
	clustering := ps.ClusteringStats
	if clustering == nil {
		for _, stats := range ps.SegmentStats {
			if len(stats.CentroidRanges) != 0 {
				return merr.WrapErrDataIntegrityMsg("centroid ranges have no enclosing centroid table")
			}
		}
		return nil
	}
	if clustering.FormatVersion != PartitionClusteringStatsFormatVersion {
		return merr.Wrapf(merr.ErrServiceUnimplemented, "unsupported partition clustering stats format %d", clustering.FormatVersion)
	}
	if ps.Version <= 0 || clustering.FieldId <= 0 || clustering.Dim <= 0 {
		return merr.WrapErrDataIntegrityMsg("invalid clustering snapshot version, field or dimension")
	}
	// Half-precision layouts are admitted when their complete typed path lands.
	if clustering.DataType != schemapb.DataType_FloatVector {
		return merr.Wrapf(merr.ErrServiceUnimplemented, "unsupported clustering stats vector type %s", clustering.DataType)
	}
	if uint64(len(clustering.Centroids)) > math.MaxUint32 {
		return merr.WrapErrDataIntegrityMsg("centroid table exceeds uint32 offset capacity")
	}
	for offset, centroid := range clustering.Centroids {
		if centroid == nil || centroid.Dim != clustering.Dim || centroid.GetFloatVector() == nil || int64(len(centroid.GetFloatVector().Data)) != clustering.Dim {
			return merr.WrapErrDataIntegrityMsg("invalid centroid vector at offset %d", offset)
		}
		for _, v := range centroid.GetFloatVector().Data {
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return merr.WrapErrDataIntegrityMsg("non-finite centroid at offset %d", offset)
			}
		}
	}
	referenced := make([]bool, len(clustering.Centroids))
	for segmentID, stats := range ps.SegmentStats {
		if segmentID <= 0 || stats.NumRows <= 0 {
			return merr.WrapErrDataIntegrityMsg("invalid covered segment %d or row count", segmentID)
		}
		seen := make(map[uint32]struct{}, len(stats.CentroidRanges))
		var end int64
		for _, entry := range stats.CentroidRanges {
			if entry == nil || uint64(entry.CentroidOffset) >= uint64(len(referenced)) {
				return merr.WrapErrDataIntegrityMsg("invalid centroid offset in segment %d", segmentID)
			}
			if _, exists := seen[entry.CentroidOffset]; exists {
				return merr.WrapErrDataIntegrityMsg("non-contiguous centroid in segment %d", segmentID)
			}
			// Subtraction avoids overflow in row_offset + row_count.
			if entry.RowOffset != end || entry.RowCount <= 0 || entry.RowCount > int64(stats.NumRows)-end {
				return merr.WrapErrDataIntegrityMsg("centroid ranges overlap, have a gap or exceed segment %d", segmentID)
			}
			end += entry.RowCount
			seen[entry.CentroidOffset] = struct{}{}
			referenced[entry.CentroidOffset] = true
		}
		if end != int64(stats.NumRows) {
			return merr.WrapErrDataIntegrityMsg("centroid ranges do not cover segment %d", segmentID)
		}
	}
	for offset, used := range referenced {
		if !used {
			return merr.WrapErrDataIntegrityMsg("centroid at offset %d has no physical rows", offset)
		}
	}
	return nil
}
