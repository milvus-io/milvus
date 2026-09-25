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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/featureusage"
)

func featureCounts() map[string]int64 {
	out := make(map[string]int64)
	for _, e := range featureusage.Snapshot() {
		name := e.Name
		if e.Bucket != "" {
			name += "|" + e.Bucket
		}
		out[name] = e.Value
	}
	return out
}

func featureDelta(before, after map[string]int64) map[string]int64 {
	out := make(map[string]int64)
	for name, v := range after {
		if d := v - before[name]; d != 0 {
			out[name] = d
		}
	}
	return out
}

func fieldsNamed(names ...string) []*schemapb.FieldData {
	out := make([]*schemapb.FieldData, 0, len(names))
	for _, n := range names {
		out = append(out, &schemapb.FieldData{FieldName: n})
	}
	return out
}

func TestRecordUpsertFeatures(t *testing.T) {
	featureusage.SetEnabled(true)
	t.Cleanup(func() { featureusage.SetEnabled(true) })

	before := featureCounts()
	recordUpsertFeatures(&milvuspb.UpsertRequest{FieldsData: fieldsNamed("pk", "vec")})
	assert.Equal(t, map[string]int64{"upsert_mode=override": 1, "upsert_fields|2-4": 1}, featureDelta(before, featureCounts()))

	// Two ops of one kind count once; the mode read is the normalized one.
	before = featureCounts()
	recordUpsertFeatures(&milvuspb.UpsertRequest{
		PartialUpdate: true,
		FieldsData:    fieldsNamed("pk", "tags", "labels", "meta", "a", "b"),
		FieldOps: []*schemapb.FieldPartialUpdateOp{
			{FieldName: "tags", Op: schemapb.FieldPartialUpdateOp_ARRAY_APPEND},
			{FieldName: "labels", Op: schemapb.FieldPartialUpdateOp_ARRAY_APPEND},
			{FieldName: "meta", Op: schemapb.FieldPartialUpdateOp_PATH_REPLACE},
		},
	})
	assert.Equal(t, map[string]int64{
		"upsert_mode=merge":      1,
		"field_ops=ARRAY_APPEND": 1,
		"field_ops=PATH_REPLACE": 1,
		"upsert_fields|5-16":     1,
	}, featureDelta(before, featureCounts()))

	featureusage.SetEnabled(false)
	before = featureCounts()
	recordUpsertFeatures(&milvuspb.UpsertRequest{FieldsData: fieldsNamed("pk")})
	assert.Empty(t, featureDelta(before, featureCounts()))
}

func TestRecordDeleteMode(t *testing.T) {
	featureusage.SetEnabled(true)
	t.Cleanup(func() { featureusage.SetEnabled(true) })

	before := featureCounts()
	recordDeleteMode(true)
	recordDeleteMode(false)
	recordDeleteMode(false)
	assert.Equal(t, map[string]int64{"delete_mode=ids": 1, "delete_mode=filter": 2}, featureDelta(before, featureCounts()))

	featureusage.SetEnabled(false)
	before = featureCounts()
	recordDeleteMode(true)
	assert.Empty(t, featureDelta(before, featureCounts()))
}
