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

package segments

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func fuzzyBM25FieldIDs(schema *schemapb.CollectionSchema) map[int64]struct{} {
	fields := make(map[int64]struct{})
	for _, field := range schema.GetFields() {
		if typeutil.IsFuzzyEnabledBM25InputField(schema, field) {
			fields[field.GetFieldID()] = struct{}{}
		}
	}
	return fields
}

func textLogV2Sizes(schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo) (int64, int64) {
	enabled := fuzzyBM25FieldIDs(schema)
	var logSize, memorySize int64
	for _, fieldLog := range loadInfo.GetTextLogV2() {
		if _, ok := enabled[fieldLog.GetFieldID()]; !ok {
			continue
		}
		logSize += getBinlogDataDiskSize(fieldLog)
		memorySize += getBinlogDataMemorySize(fieldLog)
	}
	return logSize, memorySize
}

// textTermHeapTransientBytes returns the additional Go-side byte buffer held
// while one FST fragment is copied into its native heap-owned representation.
// V3 manifests expose only aggregate field sizes, so use the full field total
// as the safe upper bound for the largest fragment.
func textTermHeapTransientBytes(schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo) int64 {
	enabled := fuzzyBM25FieldIDs(schema)
	var largest int64
	for _, fieldLog := range loadInfo.GetTextLogV2() {
		if _, ok := enabled[fieldLog.GetFieldID()]; !ok {
			continue
		}
		if hasAggregateTextTermSize(fieldLog.GetBinlogs()) {
			if size := getBinlogDataMemorySize(fieldLog); size > largest {
				largest = size
			}
			continue
		}
		for _, binlog := range fieldLog.GetBinlogs() {
			if size := binlog.GetMemorySize(); size > largest {
				largest = size
			}
		}
	}
	return largest
}
