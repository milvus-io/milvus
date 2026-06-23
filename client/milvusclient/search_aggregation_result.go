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

package milvusclient

import (
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// AggregationBucket is one bucket in a search aggregation result tree.
type AggregationBucket struct {
	Key       []BucketKeyEntry
	Count     int64
	Metrics   map[string]any
	Hits      []AggregationHit
	SubGroups []AggregationBucket
}

// BucketKeyEntry is one field value in a bucket key.
type BucketKeyEntry struct {
	FieldName string
	FieldID   int64
	Value     any
}

// AggregationHit is one top hit inside an aggregation bucket.
type AggregationHit struct {
	PK       any
	Score    float32
	Fields   map[string]any
	FieldIDs map[string]int64
}

func parseAggregationBuckets(results *schemapb.SearchResultData) ([][]AggregationBucket, error) {
	if results == nil {
		return nil, nil
	}
	pbBuckets := results.GetAggBuckets()
	aggTopks := results.GetAggTopks()
	if len(pbBuckets) == 0 && len(aggTopks) == 0 {
		return nil, nil
	}
	if len(aggTopks) == 0 {
		return nil, errors.New("search_aggregation response missing agg_topks")
	}
	if len(aggTopks) != int(results.GetNumQueries()) {
		return nil, errors.Newf("search_aggregation agg_topks length %d does not match nq %d", len(aggTopks), results.GetNumQueries())
	}

	total := int64(0)
	for _, topk := range aggTopks {
		if topk < 0 {
			return nil, errors.New("search_aggregation agg_topks cannot contain negative values")
		}
		total += topk
	}
	if total != int64(len(pbBuckets)) {
		return nil, errors.Newf("search_aggregation agg_topks sum %d does not match bucket count %d", total, len(pbBuckets))
	}

	parsed := make([][]AggregationBucket, len(aggTopks))
	offset := 0
	for i, topk := range aggTopks {
		buckets := make([]AggregationBucket, 0, int(topk))
		for j := int64(0); j < topk; j++ {
			bucket, err := parseAggregationBucket(pbBuckets[offset])
			if err != nil {
				return nil, err
			}
			buckets = append(buckets, bucket)
			offset++
		}
		parsed[i] = buckets
	}
	return parsed, nil
}

func parseAggregationBucket(pb *schemapb.AggBucket) (AggregationBucket, error) {
	if pb == nil {
		return AggregationBucket{}, errors.New("search_aggregation bucket is nil")
	}
	bucket := AggregationBucket{
		Key:       make([]BucketKeyEntry, 0, len(pb.GetKey())),
		Metrics:   make(map[string]any, len(pb.GetMetrics())),
		Hits:      make([]AggregationHit, 0, len(pb.GetHits())),
		SubGroups: make([]AggregationBucket, 0, len(pb.GetSubGroups())),
		Count:     pb.GetCount(),
	}
	for _, entry := range pb.GetKey() {
		bucket.Key = append(bucket.Key, parseBucketKeyEntry(entry))
	}
	for alias, metric := range pb.GetMetrics() {
		bucket.Metrics[alias] = metricValueToAny(metric)
	}
	for _, hit := range pb.GetHits() {
		bucket.Hits = append(bucket.Hits, parseAggregationHit(hit))
	}
	for _, sub := range pb.GetSubGroups() {
		subBucket, err := parseAggregationBucket(sub)
		if err != nil {
			return AggregationBucket{}, err
		}
		bucket.SubGroups = append(bucket.SubGroups, subBucket)
	}
	return bucket, nil
}

func parseBucketKeyEntry(pb *schemapb.BucketKeyEntry) BucketKeyEntry {
	if pb == nil {
		return BucketKeyEntry{}
	}
	fieldName := pb.GetFieldName()
	if fieldName == "" {
		fieldName = strconv.FormatInt(pb.GetFieldId(), 10)
	}
	return BucketKeyEntry{
		FieldName: fieldName,
		FieldID:   pb.GetFieldId(),
		Value:     bucketKeyEntryValueToAny(pb),
	}
}

func parseAggregationHit(pb *schemapb.AggHit) AggregationHit {
	if pb == nil {
		return AggregationHit{}
	}
	hit := AggregationHit{
		PK:       aggHitPKToAny(pb),
		Score:    pb.GetScore(),
		Fields:   make(map[string]any, len(pb.GetFields())),
		FieldIDs: make(map[string]int64, len(pb.GetFields())),
	}
	for _, field := range pb.GetFields() {
		fieldName := field.GetFieldName()
		if fieldName == "" {
			fieldName = strconv.FormatInt(field.GetFieldId(), 10)
		}
		hit.Fields[fieldName] = aggHitFieldValueToAny(field)
		hit.FieldIDs[fieldName] = field.GetFieldId()
	}
	return hit
}

func metricValueToAny(pb *schemapb.MetricValue) any {
	if pb == nil {
		return nil
	}
	switch v := pb.GetValue().(type) {
	case *schemapb.MetricValue_IntVal:
		return v.IntVal
	case *schemapb.MetricValue_DoubleVal:
		return v.DoubleVal
	case *schemapb.MetricValue_StringVal:
		return v.StringVal
	case *schemapb.MetricValue_BoolVal:
		return v.BoolVal
	default:
		return nil
	}
}

func aggHitFieldValueToAny(pb *schemapb.AggHitField) any {
	if pb == nil {
		return nil
	}
	switch v := pb.GetValue().(type) {
	case *schemapb.AggHitField_IntVal:
		return v.IntVal
	case *schemapb.AggHitField_BoolVal:
		return v.BoolVal
	case *schemapb.AggHitField_FloatVal:
		return v.FloatVal
	case *schemapb.AggHitField_DoubleVal:
		return v.DoubleVal
	case *schemapb.AggHitField_StringVal:
		return v.StringVal
	case *schemapb.AggHitField_BytesVal:
		return v.BytesVal
	default:
		return nil
	}
}

func bucketKeyEntryValueToAny(pb *schemapb.BucketKeyEntry) any {
	if pb == nil {
		return nil
	}
	switch v := pb.GetValue().(type) {
	case *schemapb.BucketKeyEntry_IntVal:
		return v.IntVal
	case *schemapb.BucketKeyEntry_StringVal:
		return v.StringVal
	case *schemapb.BucketKeyEntry_BoolVal:
		return v.BoolVal
	default:
		return nil
	}
}

func aggHitPKToAny(pb *schemapb.AggHit) any {
	if pb == nil {
		return nil
	}
	switch v := pb.GetPk().(type) {
	case *schemapb.AggHit_IntPk:
		return v.IntPk
	case *schemapb.AggHit_StrPk:
		return v.StrPk
	default:
		return nil
	}
}
