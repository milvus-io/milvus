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
	"slices"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RestoreColumnGroupLayout matches manifest field sets to persisted logical
// IDs, retaining manifest group order, field order and physical format. The
// returned groups own their Fields slices and need schema column indexes filled
// by the caller.
func RestoreColumnGroupLayout(manifestGroups []ColumnGroup, logicalGroups []*datapb.ColumnGroupStatistics) ([]ColumnGroup, error) {
	groupIDs := make(map[string]int64, len(logicalGroups))
	seenIDs := make(map[int64]struct{}, len(logicalGroups))
	seenFields := make(map[int64]struct{})
	for _, group := range logicalGroups {
		if group == nil || group.GetGroupId() < 0 {
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
			if _, exists := seenFields[field]; exists {
				return nil, merr.WrapErrDataIntegrityMsg("field %d appears in multiple persisted column groups", field)
			}
			seenFields[field] = struct{}{}
		}
		groupIDs[key] = id
	}

	var result []ColumnGroup
	for _, group := range manifestGroups {
		fields := slices.Clone(group.Fields)
		key, err := columnGroupFieldKey(fields)
		if err != nil {
			return nil, err
		}
		id, exists := groupIDs[key]
		if !exists {
			return nil, merr.WrapErrDataIntegrityMsg("manifest column group %v has no matching logical group in Statistics", fields)
		}
		if strings.TrimSpace(group.Format) == "" {
			return nil, merr.WrapErrDataIntegrityMsg("manifest column group %v has no physical format", fields)
		}
		result = append(result, ColumnGroup{GroupID: id, Fields: fields, Format: group.Format})
		delete(groupIDs, key)
	}
	if len(groupIDs) != 0 {
		return nil, merr.WrapErrDataIntegrityMsg("%d persisted column groups are absent from the manifest", len(groupIDs))
	}
	return result, nil
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
