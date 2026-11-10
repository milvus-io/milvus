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

package analyze

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestTaskResource_Analyze(t *testing.T) {
	paramtable.Init()
	mt := taskresource.NewFixtureMeta(false)
	at := NewAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 9, FieldID: 101, FieldType: schemapb.DataType_FloatVector, SegmentIDs: []int64{3}}, mt, nil)
	assert.Equal(t, taskresource.Analyze(1000*128*4), taskresource.Price(at.GetTaskResource()))
	assert.Equal(t, taskresource.Analyze(1000*128*4), taskresource.Price(at.GetTaskResource())) // cached

	missing := NewAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 10, FieldID: 101, FieldType: schemapb.DataType_FloatVector, SegmentIDs: []int64{999}}, mt, nil)
	assert.Equal(t, taskresource.Default(), taskresource.Price(missing.GetTaskResource()))
	assert.Equal(t, taskresource.Default(), taskresource.Price(missing.GetTaskResource()))
}

// TestTaskResource_AnalyzeSchemaCacheMiss is the analyze counterpart of
// the index family's schema-cache-miss test. NewAnalyzeTask snapshots the schema at
// construction, so a task built before the collection is cached would keep a nil
// schema forever: priced at the floor on every round, and re-walking every input
// segment each time because the floor is never cached. Resolving the schema
// lazily lets it repair itself.
func TestTaskResource_AnalyzeSchemaCacheMiss(t *testing.T) {
	paramtable.Init()
	mt := taskresource.NewFixtureMeta(true)
	// Exactly the state before the collection cache is warmed.
	taskresource.DropFixtureSchema(mt)

	priced := taskresource.Analyze(taskresource.BigFixtureRows * 128 * 4)
	assert.NotEqual(t, taskresource.Default(), priced, "the bug this test guards must be observable")
	// Without the schema the field cannot be told apart, so the task is bounded
	// by its whole input instead of by the floor.
	bound := taskresource.Analyze(taskresource.BigFixtureSegmentSize)

	at := NewAnalyzeTask(&indexpb.AnalyzeTask{
		CollectionID: 1, TaskID: 13, FieldID: 101,
		FieldType: schemapb.DataType_FloatVector, SegmentIDs: []int64{3},
	}, mt, nil)
	assert.Nil(t, at.schema, "the snapshot must be empty, or the test proves nothing")
	assert.Equal(t, bound, taskresource.Price(at.GetTaskResource()))
	assert.False(t, taskresource.Resolved(at.GetTaskResource()), "the scheduler must be told the price is not exact")
	assert.Equal(t, bound, taskresource.Price(at.GetTaskResource()))
	assert.Greater(t, bound.Memory, priced.Memory, "the fallback must err high, never low")

	// The schema arriving repairs the price on the very next call, without the
	// task being rebuilt.
	taskresource.PutFixtureSchema(mt, taskresource.FixtureSchema())
	assert.Equal(t, priced, taskresource.Price(at.GetTaskResource()))

	// And that answer is cached: dropping the input segment does not change it.
	mt.Segments.DropSegment(3)
	assert.Equal(t, priced, taskresource.Price(at.GetTaskResource()))
}

// TestTaskResource_AnalyzeUnknownField covers a clustering key that is not in
// the cached schema, and a field the vector estimator cannot size. Neither can
// be sized exactly, so both are bounded by the whole input.
func TestTaskResource_AnalyzeUnknownField(t *testing.T) {
	paramtable.Init()
	mt := taskresource.NewFixtureMeta(false)
	bound := taskresource.Analyze(taskresource.FixtureSegmentSize)

	unknown := NewAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 11, FieldID: 999, SegmentIDs: []int64{3}}, mt, nil)
	assert.Equal(t, bound, taskresource.Price(unknown.GetTaskResource()))

	assert.False(t, taskresource.Resolved(unknown.GetTaskResource()), "a field the schema does not know may be a schema not warmed yet")

	// A field the schema knows is priced by the field-size rule like every other
	// family: here no binlogs in meta, so the int64 schema bound.
	scalar := NewAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 12, FieldID: 100, SegmentIDs: []int64{3}}, mt, nil)
	assert.Equal(t, taskresource.Analyze(1000*8), taskresource.Price(scalar.GetTaskResource()))
}

// TestTaskResource_AnalyzeSparseKeyIsCached: a sparse (or array-of-vector)
// clustering key has no dim to multiply. The closed form answered 0 for it and
// that answer was never cached, so every scheduling round re-walked every input
// segment for as long as the task was pending. The field-size rule answers from
// meta's binlog size or the segment, and the answer is cached.
func TestTaskResource_AnalyzeSparseKeyIsCached(t *testing.T) {
	paramtable.Init()
	mt := taskresource.NewFixtureMeta(false)
	schema := taskresource.FixtureSchema()
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 103, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector})
	taskresource.PutFixtureSchema(mt, schema)

	at := NewAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 15, FieldID: 103, SegmentIDs: []int64{3}}, mt, nil)
	assert.Equal(t, taskresource.Analyze(taskresource.FixtureSegmentSize), taskresource.Price(at.GetTaskResource()))
	assert.True(t, taskresource.Resolved(at.GetTaskResource()), "a known field's price must be cached")

	// Cached: the input disappearing no longer changes the answer, i.e. meta is
	// not walked again.
	mt.Segments.DropSegment(3)
	assert.Equal(t, taskresource.Analyze(taskresource.FixtureSegmentSize), taskresource.Price(at.GetTaskResource()))

	// With the field's binlog in meta, that size answers instead of the segment.
	mt2 := taskresource.NewFixtureMeta(false)
	taskresource.PutFixtureSchema(mt2, schema)
	taskresource.UpdateFixtureSegment(mt2, func(seg *datapb.SegmentInfo) {
		seg.Binlogs = []*datapb.FieldBinlog{
			{FieldID: 103, ChildFields: []int64{103}, Binlogs: []*datapb.Binlog{{MemorySize: 200 * taskresource.MiB}}},
		}
	})
	at2 := NewAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 16, FieldID: 103, SegmentIDs: []int64{3}}, mt2, nil)
	assert.Equal(t, taskresource.Analyze(200*taskresource.MiB), taskresource.Price(at2.GetTaskResource()))
}

// TestTaskResource_AnalyzeInputGone is the other side: an input that is gone for
// good cannot be bounded at all, so it answers the floor. That price only has to
// carry the task to CreateTaskOnWorker, which retires it.
func TestTaskResource_AnalyzeInputGone(t *testing.T) {
	paramtable.Init()
	mt := taskresource.NewFixtureMeta(false)

	orphan := NewAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 14, FieldID: 101, SegmentIDs: []int64{999}}, mt, nil)
	assert.Equal(t, taskresource.Default(), taskresource.Price(orphan.GetTaskResource()))
	assert.False(t, taskresource.Resolved(orphan.GetTaskResource()))
}
