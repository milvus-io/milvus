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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// LogicalSearchResultBytes measures returned values. Search control metadata,
// protobuf framing, identifiers for columns and metric aliases are excluded.
func LogicalSearchResultBytes(result *schemapb.SearchResultData, schema *schemapb.CollectionSchema) (uint64, error) {
	fields := append([]*schemapb.FieldData(nil), result.GetFieldsData()...)
	fields = append(fields, result.GetGroupByFieldValues()...)
	if group := result.GetGroupByFieldValue(); group != nil {
		fields = append(fields, group)
	}
	bytes, err := LogicalQueryResultBytes(fields, schema)
	if err != nil {
		return 0, err
	}
	ids := result.GetIds()
	if !addLogicalProduct(&bytes, uint64(len(ids.GetIntId().GetData())), 8) ||
		!addResultByteValues(&bytes, ids.GetStrId().GetData(), nil) ||
		!addLogicalProduct(&bytes, uint64(len(result.GetScores())), 4) ||
		!addLogicalProduct(&bytes, uint64(len(result.GetDistances())), 4) ||
		!addLogicalProduct(&bytes, uint64(len(result.GetRecalls())), 4) ||
		!addLogicalProduct(&bytes, uint64(len(result.GetElementIndices().GetData())), 8) {
		return 0, merr.WrapErrServiceInternalMsg("search response exceeds the byte counter")
	}
	for _, highlight := range result.GetHighlightResults() {
		for _, data := range highlight.GetDatas() {
			if !addResultByteValues(&bytes, data.GetFragments(), nil) ||
				!addLogicalProduct(&bytes, uint64(len(data.GetScores())), 4) {
				return 0, merr.WrapErrServiceInternalMsg("search highlights exceed the byte counter")
			}
		}
	}
	if !addSearchBuckets(&bytes, result.GetAggBuckets()) {
		return 0, merr.WrapErrServiceInternalMsg("search aggregation response exceeds the byte counter")
	}
	return bytes, nil
}

func addSearchBuckets(bytes *uint64, buckets []*schemapb.AggBucket) bool {
	for _, bucket := range buckets {
		if bucket == nil {
			continue
		}
		if !addLogicalCount(bytes, 8) { // Bucket count.
			return false
		}
		for _, key := range bucket.GetKey() {
			if !addLogicalCount(bytes, searchScalarBytes(key)) {
				return false
			}
		}
		for _, value := range bucket.GetMetrics() {
			if !addLogicalCount(bytes, searchScalarBytes(value)) {
				return false
			}
		}
		for _, hit := range bucket.GetHits() {
			if hit == nil {
				continue
			}
			if !addLogicalCount(bytes, searchScalarBytes(hit)+4) { // PK and score.
				return false
			}
			for _, field := range hit.GetFields() {
				if !addLogicalCount(bytes, searchScalarBytes(field)) {
					return false
				}
			}
		}
		if !addSearchBuckets(bytes, bucket.GetSubGroups()) {
			return false
		}
	}
	return true
}

// A bucket key, metric, hit ID or hit field stores its value in a scalar oneof.
func searchScalarBytes(value proto.Message) uint64 {
	message := value.ProtoReflect()
	field := message.WhichOneof(message.Descriptor().Oneofs().Get(0))
	if field == nil {
		return 0
	}
	switch field.Kind() {
	case protoreflect.BoolKind:
		return 1
	case protoreflect.Int32Kind, protoreflect.FloatKind:
		return 4
	case protoreflect.Int64Kind, protoreflect.DoubleKind:
		return 8
	case protoreflect.StringKind:
		return uint64(len(message.Get(field).String()))
	case protoreflect.BytesKind:
		return uint64(len(message.Get(field).Bytes()))
	}
	return 0
}
