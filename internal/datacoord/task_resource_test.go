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

package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	testMiB = int64(1) << 20
	testGiB = int64(1) << 30
)

func testResourceSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
			{FieldID: 102, Name: "str", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}}},
		},
	}
}

func TestTaskResource_Formulas(t *testing.T) {
	paramtable.Init()
	defaultCPU := Params.DataCoordCfg.TaskResourceDefaultCPU.GetAsInt64()
	minMem := Params.DataCoordCfg.TaskResourceMinTaskMemory.GetAsSize()

	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 2 * testGiB}, indexTaskResource(testGiB, true))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, indexTaskResource(testGiB, false))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, statsTaskResource(testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * testMiB}, mixCompactionTaskResource())
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, l0CompactionTaskResource(testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 32 * testGiB}, clusteringCompactionTaskResource())
	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 2 * testGiB}, analyzeTaskResource(testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: testGiB}, importTaskResource(testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: minMem}, lightweightTaskResource())
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: minMem}, defaultTaskResource())

	// Nothing is ever priced below the floor: a 0-byte input still costs minTaskMemory.
	assert.Equal(t, minMem, indexTaskResource(0, true).Memory)
	assert.Equal(t, minMem, statsTaskResource(0).Memory)
	assert.Equal(t, minMem, l0CompactionTaskResource(0).Memory)
	assert.Equal(t, minMem, analyzeTaskResource(0).Memory)
	assert.Equal(t, minMem, importTaskResource(0).Memory)
}

func TestTaskResource_ConfigOverride(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(Params.DataCoordCfg.TaskResourceVectorIndexCPU.Key, "4")
	pt.Save(Params.DataCoordCfg.TaskResourceIndexMemoryFactor.Key, "3")
	defer pt.Reset(Params.DataCoordCfg.TaskResourceVectorIndexCPU.Key)
	defer pt.Reset(Params.DataCoordCfg.TaskResourceIndexMemoryFactor.Key)

	assert.Equal(t, taskcommon.Resource{CPU: 4, Memory: 3 * testGiB}, indexTaskResource(testGiB, true))
}

// TestTaskResource_ConfigFloors pins the "never zero" invariant against a
// configuration that asks for zero: CPU is floored at one whole core.
func TestTaskResource_ConfigFloors(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	for _, key := range []string{
		Params.DataCoordCfg.TaskResourceDefaultCPU.Key,
		Params.DataCoordCfg.TaskResourceVectorIndexCPU.Key,
		Params.DataCoordCfg.TaskResourceAnalyzeCPU.Key,
		Params.DataCoordCfg.TaskResourceClusteringCompactionCPU.Key,
	} {
		pt.Save(key, "0")
		defer pt.Reset(key)
	}

	assert.Equal(t, int64(1), defaultTaskResource().CPU)
	assert.Equal(t, int64(1), indexTaskResource(testGiB, true).CPU)
	assert.Equal(t, int64(1), analyzeTaskResource(testGiB).CPU)
	assert.Equal(t, int64(1), clusteringCompactionTaskResource().CPU)
}

func TestEstimateSegmentSize(t *testing.T) {
	paramtable.Init()
	schema := testResourceSchema()

	// Stats present (every storage version persists it): use it verbatim.
	withStats := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: 1000, StorageVersion: 3,
		Stats: &datapb.Statistics{InsertBinlogSize: 700, StatsBinlogSize: 200, DeltaBinlogSize: 100},
	}}
	assert.Equal(t, int64(1000), estimateSegmentSize(withStats, schema))

	// V1 without Stats but with binlogs: EnsureStats rebuilds from the arrays.
	fromBinlogs := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2, NumOfRows: 1000,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 512000, EntriesNum: 1000}}}},
	}}
	assert.Equal(t, int64(512000), estimateSegmentSize(fromBinlogs, schema))

	// External-collection shape: no Stats, no binlogs, rows known -> rows x per-record estimate.
	external := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 1000, ManifestPath: "m"}}
	perRecord, err := typeutilEstimateSizePerRecord(schema)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000)*perRecord, estimateSegmentSize(external, schema))

	// Nothing to go on at all.
	assert.Equal(t, int64(0), estimateSegmentSize(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 4}}, schema))
	assert.Equal(t, int64(0), estimateSegmentSize(external, nil))
	assert.Equal(t, int64(0), estimateSegmentSize(nil, schema))
	assert.Equal(t, int64(0), estimateSegmentSize(&SegmentInfo{}, schema))

	// A schema the estimator rejects (varchar without max_length) is not guessed at.
	badSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 102, Name: "str", DataType: schemapb.DataType_VarChar},
	}}
	assert.Equal(t, int64(0), estimateSegmentSize(external, badSchema))
}

func TestEstimateFieldSize(t *testing.T) {
	paramtable.Init()
	schema := testResourceSchema()

	// Binlog bytes for the field exist: use them.
	v1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: 1000,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 512000}}},
			{FieldID: 102, Binlogs: []*datapb.Binlog{{MemorySize: 64000}}},
		},
	}}
	assert.Equal(t, int64(512000), estimateFieldSize(v1, schema, 101))
	assert.Equal(t, int64(64000), estimateFieldSize(v1, schema, 102))

	// A struct-array parent binlog carries its children's bytes.
	withChildren := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 5, NumOfRows: 1000,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 200, ChildFields: []int64{101}, Binlogs: []*datapb.Binlog{{MemorySize: 4096}}},
		},
	}}
	assert.Equal(t, int64(4096), estimateFieldSize(withChildren, schema, 101))

	// V3 after a DataCoord restart: Binlogs empty, Stats present. Vector field is
	// closed-form rows x dim x 4; scalar field is its share of the segment size.
	// Expected scalar bytes come from the same estimator the code apportions with,
	// so the test does not hard-code the varchar length policy.
	perRecord, err := typeutilEstimateSizePerRecord(schema)
	assert.NoError(t, err)
	strBytes := fieldBytesPerRow(typeutil.GetFieldByID(schema, 102))
	assert.Greater(t, strBytes, int64(0))

	total := int64(1000) * perRecord
	v3 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2, NumOfRows: 1000, StorageVersion: 3, ManifestPath: "m",
		Stats: &datapb.Statistics{InsertBinlogSize: total},
	}}
	assert.Equal(t, int64(1000*128*4), estimateFieldSize(v3, schema, 101))
	assert.Equal(t, total*strBytes/perRecord, estimateFieldSize(v3, schema, 102))

	// External collection: no Stats either -> rows x per-field bytes.
	external := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 1000, ManifestPath: "m"}}
	assert.Equal(t, int64(1000*128*4), estimateFieldSize(external, schema, 101))
	assert.Equal(t, int64(1000)*strBytes, estimateFieldSize(external, schema, 102))

	// Unknown field / nil schema: fall back to the whole segment size (conservative).
	assert.Equal(t, total, estimateFieldSize(v3, schema, 999))
	assert.Equal(t, int64(0), estimateFieldSize(external, nil, 101))
	assert.Equal(t, int64(0), estimateFieldSize(nil, schema, 101))
	assert.Equal(t, int64(0), estimateFieldSize(&SegmentInfo{}, schema, 101))

	// A vector field whose dim cannot be resolved falls through to apportioning,
	// and with no sizeable schema left there is nothing to apportion.
	dimless := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
	}}
	assert.Equal(t, int64(0), estimateFieldSize(external, dimless, 101))

	// A field the estimator rejects outright (varchar without max_length) has no
	// per-row size, so apportioning is abandoned for the whole-segment fallback.
	unsizable := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 102, Name: "str", DataType: schemapb.DataType_VarChar},
	}}
	assert.Equal(t, int64(0), fieldBytesPerRow(unsizable.Fields[0]))
	assert.Equal(t, int64(0), estimateFieldSize(external, unsizable, 102))
}

func TestResourceCache(t *testing.T) {
	var c resourceCache
	calls := 0
	compute := func(ok bool) func() (taskcommon.Resource, bool) {
		return func() (taskcommon.Resource, bool) {
			calls++
			return taskcommon.Resource{CPU: int64(calls), Memory: 1}, ok
		}
	}
	// Not ok: value is returned but not cached, so the next call recomputes.
	assert.Equal(t, int64(1), c.get(compute(false)).CPU)
	assert.Equal(t, int64(2), c.get(compute(false)).CPU)
	// Ok: cached; subsequent calls do not recompute.
	assert.Equal(t, int64(3), c.get(compute(true)).CPU)
	assert.Equal(t, int64(3), c.get(compute(true)).CPU)
	assert.Equal(t, 3, calls)
}

func typeutilEstimateSizePerRecord(s *schemapb.CollectionSchema) (int64, error) {
	n, err := typeutil.EstimateSizePerRecord(s)
	return int64(n), err
}
