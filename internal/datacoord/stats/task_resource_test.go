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

package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/taskresource"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestTaskResource_Stats(t *testing.T) {
	paramtable.Init()
	mt := taskresource.NewFixtureMeta(false)
	// No field in the fixture schema has text match enabled, so the sub job
	// targets nothing specific and the whole segment is charged.
	st := newStatsTask(&indexpb.StatsTask{CollectionID: 1, SegmentID: 3, TaskID: 7, SubJobType: indexpb.StatsSubJob_TextIndexJob}, 1, mt, nil, nil, nil, nil)
	assert.Equal(t, taskresource.Stats(taskresource.FixtureSegmentSize), st.GetTaskResource())
	assert.Equal(t, taskresource.Stats(taskresource.FixtureSegmentSize), st.GetTaskResource()) // cached

	orphan := newStatsTask(&indexpb.StatsTask{CollectionID: 1, SegmentID: 999, TaskID: 8}, 1, mt, nil, nil, nil, nil)
	assert.Equal(t, taskresource.Default(), orphan.GetTaskResource())
	assert.Equal(t, taskresource.Default(), orphan.GetTaskResource())
}

// TestTaskResource_StatsTargetsFields: a text-match stats task reads only the
// fields with enable_match, so it is priced on those, not on the vector that
// dominates the segment.
func TestTaskResource_StatsTargetsFields(t *testing.T) {
	paramtable.Init()
	// bigFamilyMeta, so that the field price clears the floor and is
	// distinguishable from the whole-segment price.
	mt := taskresource.NewFixtureMeta(true)
	schema := taskresource.FixtureSchema()
	str := typeutil.GetFieldByID(schema, 102)
	str.TypeParams = append(str.TypeParams,
		&commonpb.KeyValuePair{Key: "enable_match", Value: "true"},
		&commonpb.KeyValuePair{Key: "enable_analyzer", Value: "true"})
	taskresource.PutFixtureSchema(mt, schema)

	st := newStatsTask(&indexpb.StatsTask{CollectionID: 1, SegmentID: 3, TaskID: 7, SubJobType: indexpb.StatsSubJob_TextIndexJob}, 1, mt, nil, nil, nil, nil)
	// No binlogs in meta: the varchar's schema bound, max_length plus the
	// Arrow offset, 68 bytes a row.
	assert.Equal(t, taskresource.Stats(taskresource.BigFixtureRows*68), st.GetTaskResource())
	assert.Equal(t, taskresource.Stats(taskresource.BigFixtureRows*68), st.GetTaskResource()) // cached
	assert.Less(t, st.GetTaskResource().Memory, taskresource.Stats(taskresource.BigFixtureRows*(taskresource.SystemFieldsBytesPerRow+8+512+64)).Memory)
}

func TestTaskResource_StatsUnknownText(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataCoordCfg.TaskResourceStatsMemoryFactor.Key, "2"))
	require.NoError(t, params.Save(params.DataCoordCfg.TaskResourceMinTaskMemory.Key, "1MB"))
	require.NoError(t, params.Save(params.DataCoordCfg.TaskResourceDefaultCPU.Key, "1"))
	t.Cleanup(func() {
		params.Reset(params.DataCoordCfg.TaskResourceStatsMemoryFactor.Key)
		params.Reset(params.DataCoordCfg.TaskResourceMinTaskMemory.Key)
		params.Reset(params.DataCoordCfg.TaskResourceDefaultCPU.Key)
	})

	for _, tc := range []struct {
		name       string
		recorded   int64
		wantMemory int64
	}{
		// Unknown TEXT uses 1000 rows at 64 KiB before the 2x stats factor.
		// The group's reference bytes must not cap that input estimate.
		{"reference group below fallback", 8 << 20, 131072000},
		{"recorded input above fallback", 96 << 20, 192 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mt := taskresource.NewFixtureMeta(false)
			schema := taskresource.FixtureSchema()
			field := typeutil.GetFieldByID(schema, 102)
			field.DataType = schemapb.DataType_Text
			field.TypeParams = []*commonpb.KeyValuePair{
				{Key: "enable_match", Value: "true"}, {Key: "enable_analyzer", Value: "true"},
			}
			taskresource.PutFixtureSchema(mt, schema)
			taskresource.UpdateFixtureSegment(mt, func(seg *datapb.SegmentInfo) {
				seg.Level = datapb.SegmentLevel_L1
				seg.ManifestPath = packed.MarshalManifestPath("files/insert_log/1/2/3", 1)
				seg.Stats.InsertBinlogSize = tc.recorded + 536000
				seg.Stats.LoadResource = &datapb.LoadResourceStatistics{
					ColumnGroups: []*datapb.ColumnGroupStatistics{{GroupId: 102, FieldIds: []int64{102}, MemorySize: tc.recorded}},
				}
			})
			st := newStatsTask(&indexpb.StatsTask{
				CollectionID: 1, SegmentID: 3, TaskID: 7, SubJobType: indexpb.StatsSubJob_TextIndexJob,
			}, 1, mt, nil, nil, nil, nil)
			assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: tc.wantMemory}, st.GetTaskResource())
		})
	}
}

// TestTaskResource_StatsSchemaCacheMiss: without a schema the targeted fields
// cannot be told apart, so the whole segment is charged -- and that answer is
// not cached, so the schema arriving later shrinks the price.
func TestTaskResource_StatsSchemaCacheMiss(t *testing.T) {
	paramtable.Init()
	mt := taskresource.NewFixtureMeta(true)
	segmentSize := taskresource.BigFixtureRows * (taskresource.SystemFieldsBytesPerRow + 8 + 512 + 64)
	schema := taskresource.FixtureSchema()
	str := typeutil.GetFieldByID(schema, 102)
	str.TypeParams = append(str.TypeParams,
		&commonpb.KeyValuePair{Key: "enable_match", Value: "true"},
		&commonpb.KeyValuePair{Key: "enable_analyzer", Value: "true"})
	taskresource.DropFixtureSchema(mt)

	st := newStatsTask(&indexpb.StatsTask{CollectionID: 1, SegmentID: 3, TaskID: 7, SubJobType: indexpb.StatsSubJob_TextIndexJob}, 1, mt, nil, nil, nil, nil)
	assert.Equal(t, taskresource.Stats(segmentSize), st.GetTaskResource())
	assert.Equal(t, taskresource.Stats(segmentSize), st.GetTaskResource())

	taskresource.PutFixtureSchema(mt, schema)
	assert.Equal(t, taskresource.Stats(taskresource.BigFixtureRows*68), st.GetTaskResource())
}

// TestTaskResource_StatsUnsizedSegment covers a segment with neither Stats nor
// a cached schema: the floor, and not cached.
func TestTaskResource_StatsUnsizedSegment(t *testing.T) {
	paramtable.Init()
	mt := taskresource.NewFixtureMeta(false)
	taskresource.UpdateFixtureSegment(mt, func(seg *datapb.SegmentInfo) { seg.Stats = nil })
	taskresource.DropFixtureSchema(mt)

	st := newStatsTask(&indexpb.StatsTask{CollectionID: 1, SegmentID: 3, TaskID: 7}, 1, mt, nil, nil, nil, nil)
	assert.Equal(t, taskresource.Default(), st.GetTaskResource())
	assert.Equal(t, taskresource.Default(), st.GetTaskResource())
}
