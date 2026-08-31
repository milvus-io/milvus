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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Every task the global scheduler can place must be able to state a
// requirement, or it silently falls back to the scalar tier and is placed on a
// number that stands in for its cost rather than being it.
//
// This is a compile-time assertion list on purpose: a new task family that
// forgets TaskResources fails to build here rather than quietly losing
// dimensioned placement in production, where the only symptom is a node
// occasionally chosen for work it cannot hold.
var _ = []globalTask.ResourceAwareTask{
	(*mixCompactionTask)(nil),
	(*l0CompactionTask)(nil),
	(*clusteringCompactionTask)(nil),
	(*bumpSchemaVersionTask)(nil),
	(*indexBuildTask)(nil),
	(*statsTask)(nil),
	(*analyzeTask)(nil),
	(*importTask)(nil),
	(*preImportTask)(nil),
	(*copySegmentTask)(nil),
	(*refreshExternalCollectionTask)(nil),
}

// A task with nothing resolvable behind it must still state a positive
// requirement. Reporting zero would make the picker treat it as free and place
// it anywhere, which is how a multi-GiB task lands on a node with no room.
func TestEveryFamilyStatesAPositiveRequirement(t *testing.T) {
	paramtable.Init()

	assertPositive := func(t *testing.T, name string, p *datapb.TaskResources) {
		t.Helper()
		req, ok := taskresource.RequirementFromProto(p)
		require.True(t, ok, "%s must state a requirement the scheduler can read", name)
		assert.Positive(t, req.Memory, "%s must never price itself as free", name)
	}

	copySeg := &copySegmentTask{}
	copySeg.task.Store(&datapb.CopySegmentTask{TaskId: 1})
	assertPositive(t, "copySegment", copySeg.TaskResources())

	assertPositive(t, "refreshExternalCollection",
		(&refreshExternalCollectionTask{}).TaskResources())
}

// The estimate that decided WHERE a task goes and the estimate the worker is
// handed have to be the same object. If placement used one figure and the
// request carried another, the worker would be charged something the
// coordinator never reserved for -- and the drift would be invisible, because
// both numbers are individually plausible.
func TestPlacedAndShippedPricesAreTheSameObject(t *testing.T) {
	paramtable.Init()

	const (
		mem   = int64(4) << 30
		segID = int64(200)
	)

	meta := NewMockCompactionMeta(t)
	meta.EXPECT().GetHealthySegment(mock.Anything, segID).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             segID,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: 3,
		NumOfRows:      1_000_000,
		Stats:          &datapb.Statistics{InsertBinlogSize: mem},
	}}).Once()

	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_MixCompaction,
		InputSegments: []int64{segID},
	}, nil, meta, newMockVersionManager())

	// What the scheduler places on...
	placed, ok := taskresource.RequirementFromProto(task.TaskResources())
	require.True(t, ok)

	// ...and what a second read gives, which is what the plan builder uses.
	// The mock allows exactly one meta walk, so a second walk here would fail
	// the test outright -- the cache is what makes the two agree cheaply.
	shipped, ok := taskresource.RequirementFromProto(task.TaskResources())
	require.True(t, ok)

	assert.Equal(t, placed, shipped)
	assert.Greater(t, placed.Memory, int64(64)<<20, "setup: not the estimator floor")
}

// Stats prices from the sub-job's own fields, so two sub-jobs over the same
// segment must not cost the same unless they touch the same fields.
func TestStatsSubJobsArePricedApart(t *testing.T) {
	paramtable.Init()

	schema := vectorSchema()
	schema.Fields[2].TypeParams = append(schema.Fields[2].TypeParams,
		&commonpb.KeyValuePair{Key: "enable_match", Value: "true"})

	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, NumOfRows: 1_000_000, StorageVersion: 3, ManifestPath: "loon://b/m@1",
		Stats: &datapb.Statistics{InsertBinlogSize: 5 * resourceGiB},
	})

	text := statsRequirement(seg, schema, indexpb.StatsSubJob_TextIndexJob)
	unknown := statsRequirement(seg, schema, indexpb.StatsSubJob(9999))

	assert.Less(t, text.Memory, unknown.Memory,
		"a sub-job that reads one field must not be charged the whole segment")
}
