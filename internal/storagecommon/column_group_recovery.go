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

package storagecommon

import (
	"cmp"
	"math"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RestoreColumnGroupLayout matches manifest field sets to persisted logical
// IDs, coalescing equivalent historical aliases while retaining manifest group
// order, field order and physical format. The returned groups own their Fields
// slices and need schema column indexes filled by the caller.
func RestoreColumnGroupLayout(manifestGroups []ColumnGroup, logicalGroups []*datapb.ColumnGroupStatistics) ([]ColumnGroup, error) {
	groups, err := indexColumnGroupStatistics(logicalGroups)
	if err != nil {
		return nil, err
	}

	var result []ColumnGroup
	for _, group := range manifestGroups {
		fields := slices.Clone(group.Fields)
		key, err := columnGroupFieldKey(fields)
		if err != nil {
			return nil, err
		}
		statistics, exists := groups[key]
		if !exists {
			return nil, merr.WrapErrDataIntegrityMsg("manifest column group %v has no matching logical group in Statistics", fields)
		}
		if strings.TrimSpace(group.Format) == "" {
			return nil, merr.WrapErrDataIntegrityMsg("manifest column group %v has no physical format", fields)
		}
		result = append(result, ColumnGroup{GroupID: statistics.GetGroupId(), Fields: fields, Format: group.Format})
		delete(groups, key)
	}
	if len(groups) != 0 {
		return nil, merr.WrapErrDataIntegrityMsg("%d persisted column groups are absent from the manifest", len(groups))
	}
	return result, nil
}

// NormalizeColumnGroupStatistics coalesces historical aliases onto the IDs of
// the layout that will actually be used to write. It owns the returned proto
// and retains every aggregate outside LoadResource unchanged. An absent load
// resource is retained for new segments and legacy snapshots.
func NormalizeColumnGroupStatistics(layout []ColumnGroup, stats *datapb.Statistics) (*datapb.Statistics, error) {
	if stats == nil {
		return nil, nil
	}
	result := proto.Clone(stats).(*datapb.Statistics)
	if len(stats.GetLoadResource().GetColumnGroups()) == 0 {
		return result, nil
	}
	groups, err := indexColumnGroupStatistics(stats.GetLoadResource().GetColumnGroups())
	if err != nil {
		return nil, err
	}
	normalized := make([]*datapb.ColumnGroupStatistics, 0, len(layout))
	seenIDs := make(map[int64]struct{}, len(layout))
	for _, group := range layout {
		if group.GroupID < 0 {
			return nil, merr.WrapErrDataIntegrityMsg("invalid recovered column-group ID %d", group.GroupID)
		}
		if _, exists := seenIDs[group.GroupID]; exists {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate recovered column-group ID %d", group.GroupID)
		}
		seenIDs[group.GroupID] = struct{}{}
		key, err := columnGroupFieldKey(group.Fields)
		if err != nil {
			return nil, err
		}
		statistics, exists := groups[key]
		if !exists {
			return nil, merr.WrapErrDataIntegrityMsg("recovered column group %v has no matching statistics", group.Fields)
		}
		statistics.GroupId = group.GroupID
		normalized = append(normalized, statistics)
		delete(groups, key)
	}
	if len(groups) != 0 {
		return nil, merr.WrapErrDataIntegrityMsg("%d persisted column groups are absent from the recovered layout", len(groups))
	}
	slices.SortFunc(normalized, func(a, b *datapb.ColumnGroupStatistics) int {
		return cmp.Compare(a.GetGroupId(), b.GetGroupId())
	})
	result.LoadResource.ColumnGroups = normalized
	return result, nil
}

// Equal field sets may have acquired different IDs when an older flusher
// sampled a fresh split after recovery. Those entries describe disjoint sync
// contributions to the same physical group. Choose the smallest ID independent
// of input order and sum their sizes. Partial overlap is not such an alias.
func indexColumnGroupStatistics(logicalGroups []*datapb.ColumnGroupStatistics) (map[string]*datapb.ColumnGroupStatistics, error) {
	groups := make(map[string]*datapb.ColumnGroupStatistics, len(logicalGroups))
	seenIDs := make(map[int64]struct{}, len(logicalGroups))
	fieldGroups := make(map[int64]string)
	for _, group := range logicalGroups {
		if group == nil || group.GetGroupId() < 0 || group.GetMemorySize() < 0 {
			return nil, merr.WrapErrDataIntegrityMsg("invalid persisted column-group statistics")
		}
		id := group.GetGroupId()
		if _, exists := seenIDs[id]; exists {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate persisted column-group ID %d", id)
		}
		seenIDs[id] = struct{}{}
		key, err := columnGroupFieldKey(group.GetFieldIds())
		if err != nil {
			return nil, err
		}
		for _, field := range group.GetFieldIds() {
			if previous, exists := fieldGroups[field]; exists && previous != key {
				return nil, merr.WrapErrDataIntegrityMsg("field %d appears in non-equivalent persisted column groups", field)
			}
			fieldGroups[field] = key
		}
		if merged, exists := groups[key]; exists {
			if merged.GetMemorySize() > math.MaxInt64-group.GetMemorySize() {
				return nil, merr.WrapErrDataIntegrityMsg("column-group memory size overflows for fields %v", group.GetFieldIds())
			}
			merged.MemorySize += group.GetMemorySize()
			merged.GroupId = min(merged.GetGroupId(), id)
		} else {
			merged := proto.Clone(group).(*datapb.ColumnGroupStatistics)
			slices.Sort(merged.FieldIds)
			groups[key] = merged
		}
	}
	return groups, nil
}

func columnGroupFieldKey(fields []int64) (string, error) {
	if len(fields) == 0 {
		return "", merr.WrapErrDataIntegrityMsg("persisted column group has no fields")
	}
	sorted := slices.Clone(fields)
	slices.Sort(sorted)
	parts := make([]string, len(sorted))
	for i, field := range sorted {
		if field < 0 || (i > 0 && field == sorted[i-1]) {
			return "", merr.WrapErrDataIntegrityMsg("invalid or duplicate field in column group %v", fields)
		}
		parts[i] = strconv.FormatInt(field, 10)
	}
	return strings.Join(parts, ","), nil
}
