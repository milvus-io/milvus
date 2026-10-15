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
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
// so int64 range offsets, counts and group segment IDs survive a round trip.
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
	FieldStats       []FieldStats `json:"fieldStats"`
	NumRows          int
	ClusteringRanges []json.RawMessage `json:"clusteringRanges,omitempty"`
}

func (stats SegmentStats) MarshalJSON() ([]byte, error) {
	wire := segmentStatsJSON{FieldStats: stats.FieldStats, NumRows: stats.NumRows}
	for _, entry := range stats.ClusteringRanges {
		if entry == nil {
			return nil, merr.WrapErrDataIntegrityMsg("nil clustering range")
		}
		encoded, err := protojson.Marshal(entry)
		if err != nil {
			return nil, merr.WrapErrSerializationFailed(err, "encode clustering range")
		}
		wire.ClusteringRanges = append(wire.ClusteringRanges, encoded)
	}
	return json.Marshal(wire)
}

func (stats *SegmentStats) UnmarshalJSON(data []byte) error {
	var wire segmentStatsJSON
	if err := json.Unmarshal(data, &wire); err != nil {
		return wrapPartitionStatsDecodeError(err, "decode segment stats")
	}
	next := SegmentStats{FieldStats: wire.FieldStats, NumRows: wire.NumRows}
	for _, encoded := range wire.ClusteringRanges {
		if bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
			return merr.WrapErrDataIntegrityMsg("null clustering range")
		}
		entry := &clusteringpb.ClusteringRange{}
		if err := protojson.Unmarshal(encoded, entry); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode clustering range")
		}
		next.ClusteringRanges = append(next.ClusteringRanges, entry)
	}
	*stats = next
	return nil
}

// ValidateClusteringStats checks metadata without loading cluster/index files.
// Readers must validate typed cluster descriptions and matching index labels.
// A valid layout must still be bound to its matching segment view before pruning.
// Selection and publication must separately enforce complete group replacement
// against the pinned base; this validator only checks one snapshot's structure.
func (ps *PartitionStatsSnapshot) ValidateClusteringStats() error {
	if ps == nil {
		return merr.WrapErrDataIntegrityMsg("nil partition stats snapshot")
	}
	clustering := ps.ClusteringStats
	if clustering == nil {
		for _, stats := range ps.SegmentStats {
			if len(stats.ClusteringRanges) != 0 {
				return merr.WrapErrDataIntegrityMsg("clustering ranges have no enclosing cluster table")
			}
		}
		return nil
	}
	if clustering.FormatVersion != PartitionClusteringStatsFormatVersion {
		return merr.Wrapf(merr.ErrServiceUnimplemented, "unsupported partition clustering stats format %d", clustering.FormatVersion)
	}
	if ps.Version <= 0 || clustering.FieldId <= 0 {
		return merr.WrapErrDataIntegrityMsg("invalid clustering snapshot version or field")
	}
	if err := validateClusteringField(clustering); err != nil {
		return err
	}
	if (clustering.NumClusters == 0) != (clustering.ClustersFile == "") {
		return merr.WrapErrDataIntegrityMsg("cluster count and file reference disagree")
	}
	if err := validateClusteringIndex(clustering); err != nil {
		return err
	}
	return ps.validateClusteringLayout()
}

// Also used by an in-memory draft before its descriptions/index are uploaded.
// Published serialization still requires ValidateClusteringStats in full.
func (ps *PartitionStatsSnapshot) validateClusteringLayout() error {
	clustering := ps.ClusteringStats
	segmentGroups := make(map[UniqueID]int)
	for groupOffset, group := range clustering.Groups {
		if group == nil || len(group.SegmentIds) == 0 {
			return merr.WrapErrDataIntegrityMsg("empty clustering group %d", groupOffset)
		}
		for _, segmentID := range group.SegmentIds {
			if _, exists := ps.SegmentStats[segmentID]; !exists {
				return merr.WrapErrDataIntegrityMsg("clustering group %d references unknown segment %d", groupOffset, segmentID)
			}
			if _, exists := segmentGroups[segmentID]; exists {
				return merr.WrapErrDataIntegrityMsg("duplicate clustering group membership for segment %d", segmentID)
			}
			segmentGroups[segmentID] = groupOffset
		}
	}
	if len(segmentGroups) != len(ps.SegmentStats) {
		return merr.WrapErrDataIntegrityMsg("clustering groups do not cover every segment")
	}
	// Allocate only for offsets actually present in the decoded ranges, not an
	// untrusted count from metadata that could otherwise allocate gigabytes.
	// A cluster's group is derived from its ranges' segment ownership, so no
	// second membership list needs to be persisted or kept in sync.
	clusterGroups := make(map[uint32]int)
	for segmentID, stats := range ps.SegmentStats {
		if segmentID <= 0 || stats.NumRows <= 0 {
			return merr.WrapErrDataIntegrityMsg("invalid covered segment %d or row count", segmentID)
		}
		seen := make(map[uint32]struct{}, len(stats.ClusteringRanges))
		var end int64
		for _, entry := range stats.ClusteringRanges {
			if entry == nil || entry.ClusterOffset >= clustering.NumClusters {
				return merr.WrapErrDataIntegrityMsg("invalid cluster offset in segment %d", segmentID)
			}
			if _, exists := seen[entry.ClusterOffset]; exists {
				return merr.WrapErrDataIntegrityMsg("non-contiguous cluster in segment %d", segmentID)
			}
			// Subtraction avoids overflow in row_offset + row_count.
			if entry.RowOffset != end || entry.RowCount <= 0 || entry.RowCount > int64(stats.NumRows)-end {
				return merr.WrapErrDataIntegrityMsg("clustering ranges overlap, have a gap or exceed segment %d", segmentID)
			}
			end += entry.RowCount
			seen[entry.ClusterOffset] = struct{}{}
			groupOffset := segmentGroups[segmentID]
			if previous, exists := clusterGroups[entry.ClusterOffset]; exists && previous != groupOffset {
				return merr.WrapErrDataIntegrityMsg("cluster %d spans clustering groups %d and %d", entry.ClusterOffset, previous, groupOffset)
			}
			clusterGroups[entry.ClusterOffset] = groupOffset
		}
		if end != int64(stats.NumRows) {
			return merr.WrapErrDataIntegrityMsg("clustering ranges do not cover segment %d", segmentID)
		}
	}
	if uint64(len(clusterGroups)) != uint64(clustering.NumClusters) {
		return merr.WrapErrDataIntegrityMsg("cluster count does not match referenced offsets")
	}
	return nil
}

// Keep type admission separate from the common index and physical-range rules.
// Scalar mapping must supply its typed cluster descriptions and sorted ranges
// before scalar snapshots can be admitted. Existing scalar FieldStats are
// unaffected. Dense floating vectors share the snapshot layout contract.
func validateClusteringField(clustering *clusteringpb.PartitionClusteringStats) error {
	switch clustering.DataType {
	case schemapb.DataType_FloatVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		if clustering.Dim <= 0 {
			return merr.WrapErrDataIntegrityMsg("invalid clustering vector dimension")
		}
		return nil
	default:
		return merr.Wrapf(merr.ErrServiceUnimplemented, "unsupported clustering stats field type %s", clustering.DataType)
	}
}

func validateClusteringIndex(clustering *clusteringpb.PartitionClusteringStats) error {
	index := clustering.ClusteringIndex
	if clustering.NumClusters == 0 {
		if index != nil {
			return merr.WrapErrDataIntegrityMsg("empty cluster table has an index")
		}
		return nil
	}
	if index == nil {
		return merr.WrapErrDataIntegrityMsg("nonempty cluster table has no index")
	}
	if index.IndexEngineVersion <= 0 || index.IndexSize <= 0 {
		return merr.WrapErrDataIntegrityMsg("invalid clustering index engine version or size")
	}
	params := make(map[string]string, len(index.IndexParams))
	for _, param := range index.IndexParams {
		if param == nil || strings.TrimSpace(param.Key) == "" {
			return merr.WrapErrDataIntegrityMsg("invalid clustering index parameter")
		}
		if _, exists := params[param.Key]; exists {
			return merr.WrapErrDataIntegrityMsg("duplicate clustering index parameter %s", param.Key)
		}
		params[param.Key] = param.Value
	}
	if strings.TrimSpace(params[common.IndexTypeKey]) == "" {
		return merr.WrapErrDataIntegrityMsg("clustering index requires index_type")
	}
	if typeutil.IsVectorType(clustering.DataType) && strings.TrimSpace(params[common.MetricTypeKey]) == "" {
		return merr.WrapErrDataIntegrityMsg("vector clustering index requires metric_type")
	}
	if len(index.IndexFilePaths) == 0 {
		return merr.WrapErrDataIntegrityMsg("clustering index has no files")
	}
	files := make(map[string]struct{}, len(index.IndexFilePaths))
	for _, file := range index.IndexFilePaths {
		// Complete object keys are opaque; do not normalize or prepend a root.
		if file == "" || file == clustering.ClustersFile {
			return merr.WrapErrDataIntegrityMsg("invalid clustering index file reference")
		}
		if _, exists := files[file]; exists {
			return merr.WrapErrDataIntegrityMsg("duplicate clustering index file reference")
		}
		files[file] = struct{}{}
	}
	return nil
}
