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

package featureusage

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestTallyCoversOnlyPerSubrequestCounters(t *testing.T) {
	var tally Tally
	assert.Equal(t, 27, numTally)
	assert.EqualValues(t, 54, unsafe.Sizeof(tally))

	tally.Add(FeatureEf64)
	tally.Add(FeatureEf64)
	tally.Add(FeatureRetrievalElementLevel)
	tally.Add(FeatureGroupByField) // request-level: ignored
	tally.Add(Feature(-1))
	tally.Add(numFeatures)
	assert.EqualValues(t, 2, tally.Count(FeatureEf64))
	assert.EqualValues(t, 1, tally.Count(FeatureRetrievalElementLevel))
	assert.EqualValues(t, 0, tally.Count(FeatureGroupByField))

	var nilTally *Tally
	nilTally.Add(FeatureEf64)
	assert.EqualValues(t, 0, nilTally.Count(FeatureEf64))
	nilTally.HitAll()

	// Every counter of the range carries a bucket or a retrieval kind, and
	// nothing outside the range is a per-subrequest counter.
	for f := firstTallyFeature; f <= lastTallyFeature; f++ {
		assert.True(t, f.Bucket() != "" || RetrievalFamily(f) != 0, f.Name())
	}
}

func TestTallySaturates(t *testing.T) {
	var tally Tally
	for i := 0; i < 70000; i++ {
		tally.Add(FeatureNq1)
	}
	assert.EqualValues(t, 65535, tally.Count(FeatureNq1))
}

func TestTallyHitAll(t *testing.T) {
	before := index(Snapshot())
	var tally Tally
	tally.Add(FeatureLimit100)
	tally.Add(FeatureLimit100)
	tally.Add(FeatureRetrievalSparse)

	SetEnabled(false)
	tally.HitAll()
	SetEnabled(true)
	mid := index(Snapshot())
	assert.Equal(t, before["limit|11-100"].Value, mid["limit|11-100"].Value, "disabled: nothing moves")

	tally.HitAll()
	after := index(Snapshot())
	assert.EqualValues(t, 2, after["limit|11-100"].Value-before["limit|11-100"].Value)
	assert.EqualValues(t, 1, after["retrieval=sparse_vector"].Value-before["retrieval=sparse_vector"].Value)
	assert.NotZero(t, after["limit|11-100"].LastUsedAt)
}

func TestHitN(t *testing.T) {
	c := newCountersWithClock(func() int64 { return 7 })
	c.HitN(FeatureNq10, 3)
	c.HitN(FeatureNq10, 0)
	c.HitN(FeatureNq10, -1)
	c.HitN(numFeatures, 1)
	e := index(c.Snapshot())["nq|2-10"]
	require.NotNil(t, e)
	assert.EqualValues(t, 3, e.Value)
	assert.EqualValues(t, 7, e.LastUsedAt)
}

func TestDistributionBuckets(t *testing.T) {
	bucket := func(f Feature) string { return f.Name() + "|" + f.Bucket() }

	assert.Equal(t, "ef|omitted", bucket(EfFeature(64, false)))
	assert.Equal(t, "ef|<=16", bucket(EfFeature(0, true)))
	assert.Equal(t, "ef|<=16", bucket(EfFeature(16, true)))
	assert.Equal(t, "ef|17-64", bucket(EfFeature(17, true)))
	assert.Equal(t, "ef|257-1024", bucket(EfFeature(1024, true)))
	assert.Equal(t, "ef|>1024", bucket(EfFeature(1025, true)))

	assert.Equal(t, "nprobe|omitted", bucket(NprobeFeature(0, false)))
	assert.Equal(t, "nprobe|<=8", bucket(NprobeFeature(8, true)))
	assert.Equal(t, "nprobe|9-32", bucket(NprobeFeature(9, true)))
	assert.Equal(t, "nprobe|>1024", bucket(NprobeFeature(4096, true)))

	assert.Equal(t, "limit|<=10", bucket(LimitFeature(10)))
	assert.Equal(t, "limit|11-100", bucket(LimitFeature(11)))
	assert.Equal(t, "limit|1001-16384", bucket(LimitFeature(16384)))
	assert.Equal(t, "limit|>16384", bucket(LimitFeature(16385)))

	assert.Equal(t, "nq|1", bucket(NqFeature(1)))
	assert.Equal(t, "nq|2-10", bucket(NqFeature(2)))
	assert.Equal(t, "nq|>1000", bucket(NqFeature(1001)))

	assert.Equal(t, "hybrid_search_reqs|<=2", bucket(HybridReqsFeature(2)))
	assert.Equal(t, "hybrid_search_reqs|3", bucket(HybridReqsFeature(3)))
	assert.Equal(t, "hybrid_search_reqs|4-5", bucket(HybridReqsFeature(5)))
	assert.Equal(t, "hybrid_search_reqs|6-10", bucket(HybridReqsFeature(10)))
	assert.Equal(t, "hybrid_search_reqs|>10", bucket(HybridReqsFeature(11)))

	assert.Equal(t, "upsert_fields|1", bucket(UpsertFieldsFeature(1)))
	assert.Equal(t, "upsert_fields|2-4", bucket(UpsertFieldsFeature(4)))
	assert.Equal(t, "upsert_fields|5-16", bucket(UpsertFieldsFeature(16)))
	assert.Equal(t, "upsert_fields|>16", bucket(UpsertFieldsFeature(17)))
}

func TestHybridCombination(t *testing.T) {
	_, ok := HybridComboFeature(0)
	assert.False(t, ok)
	cases := map[int]string{
		HybridFamilyDense:                                             "hybrid_search=dense_vector",
		HybridFamilySparse:                                            "hybrid_search=sparse_vector",
		HybridFamilyFullText:                                          "hybrid_search=full_text_search",
		HybridFamilyDense | HybridFamilySparse:                        "hybrid_search=dense_vector+sparse_vector",
		HybridFamilyDense | HybridFamilyFullText:                      "hybrid_search=dense_vector+full_text_search",
		HybridFamilySparse | HybridFamilyFullText:                     "hybrid_search=sparse_vector+full_text_search",
		HybridFamilyDense | HybridFamilySparse | HybridFamilyFullText: "hybrid_search=dense_vector+sparse_vector+full_text_search",
	}
	for families, name := range cases {
		f, ok := HybridComboFeature(families)
		require.True(t, ok)
		assert.Equal(t, name, f.Name())
	}
	assert.Equal(t, HybridFamilyDense, RetrievalFamily(FeatureRetrievalEmbeddingList))
	assert.Equal(t, HybridFamilyDense, RetrievalFamily(FeatureRetrievalElementLevel))
	assert.Equal(t, HybridFamilyFullText, RetrievalFamily(FeatureRetrievalFullText))
	assert.Equal(t, 0, RetrievalFamily(FeatureGroupByField))
}

func TestQueryAggregationAndFieldOps(t *testing.T) {
	for _, op := range []string{"count", "sum", "min", "max", "avg"} {
		f, ok := QueryAggregationFeature(op)
		require.True(t, ok, op)
		assert.Equal(t, "query_aggregation="+op, f.Name())
	}
	_, ok := QueryAggregationFeature("median")
	assert.False(t, ok)

	assert.Equal(t, FeatureFieldOpReplace, FieldOpFeature(schemapb.FieldPartialUpdateOp_REPLACE))
	assert.Equal(t, FeatureFieldOpArrayAppend, FieldOpFeature(schemapb.FieldPartialUpdateOp_ARRAY_APPEND))
	assert.Equal(t, FeatureFieldOpArrayRemove, FieldOpFeature(schemapb.FieldPartialUpdateOp_ARRAY_REMOVE))
	assert.Equal(t, FeatureFieldOpPathReplace, FieldOpFeature(schemapb.FieldPartialUpdateOp_PATH_REPLACE))
	assert.Equal(t, FeatureFieldOpOther, FieldOpFeature(schemapb.FieldPartialUpdateOp_OpType(99)))
}
