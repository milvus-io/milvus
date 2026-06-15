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

package externalmaterialization

import (
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type FieldRef struct {
	FieldID   int64
	FieldName string
}

func SegmentCoversField(segment *datapb.SegmentInfo, fieldID int64) bool {
	if segment.GetManifestPath() == "" {
		return false
	}

	for _, fieldBinlog := range segment.GetBinlogs() {
		if fieldBinlog == nil {
			continue
		}
		if fieldBinlog.GetFieldID() == fieldID {
			return true
		}
		for _, childFieldID := range fieldBinlog.GetChildFields() {
			if childFieldID == fieldID {
				return true
			}
		}
	}

	return false
}

func SegmentCoversBM25Stats(segment *datapb.SegmentInfo, fieldID int64) bool {
	if segment.GetManifestPath() == "" {
		return false
	}

	for _, fieldBinlog := range segment.GetBm25Statslogs() {
		if fieldBinlog == nil {
			continue
		}
		if fieldBinlog.GetFieldID() == fieldID && len(fieldBinlog.GetBinlogs()) > 0 {
			return true
		}
	}

	return false
}

func MissingFieldsForSegment(fields []*schemapb.FieldSchema, segment *datapb.SegmentInfo, fieldIDs []int64) []FieldRef {
	fieldSchemas := make(map[int64]*schemapb.FieldSchema, len(fields))
	for _, field := range fields {
		if field == nil {
			continue
		}
		fieldSchemas[field.GetFieldID()] = field
	}

	uniqueFieldIDs := make(map[int64]struct{}, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		uniqueFieldIDs[fieldID] = struct{}{}
	}

	sortedFieldIDs := make([]int64, 0, len(uniqueFieldIDs))
	for fieldID := range uniqueFieldIDs {
		sortedFieldIDs = append(sortedFieldIDs, fieldID)
	}
	sort.Slice(sortedFieldIDs, func(i, j int) bool {
		return sortedFieldIDs[i] < sortedFieldIDs[j]
	})

	missingFields := make([]FieldRef, 0, len(sortedFieldIDs))
	for _, fieldID := range sortedFieldIDs {
		field := fieldSchemas[fieldID]
		if !SegmentCoversSchemaField(segment, field, fieldID) {
			fieldName := ""
			if field != nil {
				fieldName = field.GetName()
			}
			missingFields = append(missingFields, FieldRef{
				FieldID:   fieldID,
				FieldName: fieldName,
			})
		}
	}

	return missingFields
}

// SegmentCoversSchemaField reports whether the segment materializes the schema
// field using the correct backing metadata for that field kind.
func SegmentCoversSchemaField(segment *datapb.SegmentInfo, field *schemapb.FieldSchema, fieldID int64) bool {
	if field != nil &&
		field.GetDataType() == schemapb.DataType_SparseFloatVector &&
		(field.GetIsFunctionOutput() || field.GetExternalField() == "") {
		return SegmentCoversBM25Stats(segment, fieldID)
	}

	return SegmentCoversField(segment, fieldID)
}
