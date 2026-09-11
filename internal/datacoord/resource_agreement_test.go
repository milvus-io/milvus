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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The two sides of this design compute a task's requirement INDEPENDENTLY from
// one shared package: DataCoord to size the task for dispatch, the DataNode to
// admit it. That independence is deliberate -- it is what protects a node
// driven by a coordinator that has not been upgraded yet -- but it means every
// quantity both sides derive is two implementations of one contract, and
// nothing forces them to agree.
//
// Three disagreements of exactly this shape have already shipped on this branch
// and been found by review rather than by a test: row counts multiplied by the
// field count, per-field binlog lookups that never match on storage v2/v3, and
// segment sizes that summed different sets of logs. The tests below feed ONE
// fixture through BOTH paths and assert the answers are the same, so the next
// one fails here instead.

const (
	agreementMiB = int64(1) << 20
	agreementGiB = int64(1) << 30
)

// columnGroupBinlogs is the storage v2/v3 layout: FieldBinlog entries are keyed
// by COLUMN GROUP id -- a counter from 0, see storagecommon.SplitBySchema --
// and the real field ids live in ChildFields. This is the layout the whole
// effort targets, and the one a FieldID-only lookup never matches.
func columnGroupBinlogs() []*datapb.FieldBinlog {
	return []*datapb.FieldBinlog{
		{
			FieldID:     0,
			ChildFields: []int64{100, 101},
			Binlogs:     []*datapb.Binlog{{MemorySize: 3 * agreementGiB, EntriesNum: 1_000_000}},
		},
		{
			FieldID:     1,
			ChildFields: []int64{102},
			Binlogs:     []*datapb.Binlog{{MemorySize: 512 * agreementMiB, EntriesNum: 1_000_000}},
		},
	}
}

// TestFieldBinlogSizeAgreesAcrossSides pins the C2 contract: the per-field
// lookup the DataNode uses to size a scalar index build must return the same
// bytes DataCoord's SegmentInfo.getFieldBinlogSize returns for the same field
// on the same segment.
//
// Before the fix the DataNode side matched only the top-level FieldID, so on
// the column-group layout below it returned 0 for every real field id and the
// estimate floored at 64MiB -- admission control effectively off for every
// scalar index build and every stats sub-job on storage v2/v3.
func TestFieldBinlogSizeAgreesAcrossSides(t *testing.T) {
	paramtable.Init()

	binlogs := columnGroupBinlogs()
	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID:             1,
		Binlogs:        binlogs,
		StorageVersion: 3,
	})

	const targetField = int64(100)
	coordSize := seg.getFieldBinlogSize(targetField)
	require.Equal(t, 3*agreementGiB, coordSize,
		"setup: DataCoord must find the field through ChildFields")

	req := &workerpb.CreateJobRequest{
		BuildID:        9001,
		FieldID:        targetField,
		Field:          &schemapb.FieldSchema{FieldID: targetField, DataType: schemapb.DataType_VarChar},
		StorageVersion: 3,
		InsertLogs:     binlogs,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "INVERTED"},
		},
	}

	nodeSide := taskresource.RequirementForIndex(req)
	coordSide := taskresource.EstimateIndexBuild(taskresource.IndexInput{
		IndexType:       "INVERTED",
		FieldMemorySize: coordSize,
		StorageVersion:  3,
	})

	require.Greater(t, coordSide.Memory, 64*agreementMiB,
		"setup: the expected value must not be the 64MiB estimator floor")
	assert.Equal(t, coordSide, nodeSide,
		"the two sides must price the same field on the same segment identically")
}

// The stats family is the other consumer of the per-field lookup, and the one
// DataCoord actually submits three sub-jobs of. Same fixture, same contract.
func TestStatsFieldSelectionAgreesAcrossSides(t *testing.T) {
	paramtable.Init()

	binlogs := columnGroupBinlogs()
	seg := NewSegmentInfo(&datapb.SegmentInfo{ID: 1, Binlogs: binlogs, StorageVersion: 3})

	// Field 102 is a JSON column sharing group 1.
	const jsonField = int64(102)
	coordSize := seg.getFieldBinlogSize(jsonField)
	require.Equal(t, 512*agreementMiB, coordSize, "setup: found through ChildFields")

	req := &workerpb.CreateStatsRequest{
		TaskID:         9002,
		SubJobType:     indexpb.StatsSubJob_JsonKeyIndexJob,
		StorageVersion: 3,
		InsertLogs:     binlogs,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, DataType: schemapb.DataType_VarChar},
				{FieldID: 101, DataType: schemapb.DataType_Int64},
				{FieldID: jsonField, DataType: schemapb.DataType_JSON},
			},
		},
	}

	nodeSide := taskresource.RequirementForStats(req)
	factor := paramtable.Get().DataCoordCfg.ResourceJSONKeyIndexFactor.GetAsFloat()
	want := int64(float64(coordSize) * factor)

	require.Greater(t, want, 64*agreementMiB,
		"setup: the expected value must not be the 64MiB estimator floor")
	assert.Equal(t, want, nodeSide.Memory)
}

// TestCompactionRequirementAgreesWithDataCoord pins the F5 contract:
// CompactionInput.TotalMemorySize must mean the same thing on both sides.
//
// DataCoord feeds SegmentInfo.getSegmentSize(), which sums insert + delta +
// stats logs. The DataNode summed only the insert logs, so the enforcing side
// under-counted -- the unsafe direction. The fixture below gives each of the
// three log kinds a distinct, large size, so any term dropped on either side
// shows up as a different number rather than as rounding.
func TestCompactionRequirementAgreesWithDataCoord(t *testing.T) {
	paramtable.Init()

	insert := []*datapb.FieldBinlog{
		{FieldID: 0, ChildFields: []int64{100, 101}, Binlogs: []*datapb.Binlog{{MemorySize: 4 * agreementGiB, EntriesNum: 2_000_000}}},
	}
	deltas := []*datapb.FieldBinlog{
		{Binlogs: []*datapb.Binlog{{MemorySize: 700 * agreementMiB}}},
	}
	stats := []*datapb.FieldBinlog{
		{Binlogs: []*datapb.Binlog{{MemorySize: 300 * agreementMiB}}},
	}

	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID:             1,
		NumOfRows:      2_000_000,
		Binlogs:        insert,
		Deltalogs:      deltas,
		Statslogs:      stats,
		StorageVersion: 3,
		State:          commonpb.SegmentState_Flushed,
	})

	// --- DataCoord's path, as mixCompactionTask.computeAndCacheTaskSlot runs it.
	coordSide := taskresource.EstimateCompaction(taskresource.CompactionInput{
		Type:                  datapb.CompactionType_MixCompaction,
		StorageVersion:        seg.GetStorageVersion(),
		TotalMemorySize:       seg.getSegmentSize(),
		TotalRows:             seg.GetNumOfRows(),
		MaxSegmentDeleteBytes: seg.getDeltaLogSize(),
	})

	// --- The DataNode's path, from the plan it is actually sent.
	plan := &datapb.CompactionPlan{
		PlanID: 4242,
		Type:   datapb.CompactionType_MixCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID:           seg.GetID(),
				StorageVersion:      seg.GetStorageVersion(),
				FieldBinlogs:        insert,
				Deltalogs:           deltas,
				Field2StatslogPaths: stats,
			},
		},
	}
	nodeSide := taskresource.RequirementForCompaction(plan)

	require.Greater(t, coordSide.Memory, 4*agreementGiB,
		"setup: the expected value must be well clear of any estimator floor")
	assert.Equal(t, coordSide, nodeSide,
		"the enforcing side must not price the same compaction lower than the dispatching side")
}

// The row count is the third quantity both sides derive, and the first one this
// class of bug was caught in (it was being multiplied by the field count).
// Sort compaction is where it feeds the estimate, so assert it there.
func TestCompactionRowCountAgreesWithDataCoord(t *testing.T) {
	paramtable.Init()

	// Several column groups, each covering the same rows: summing EntriesNum
	// across them all would treble the row count.
	insert := []*datapb.FieldBinlog{
		{FieldID: 0, ChildFields: []int64{100}, Binlogs: []*datapb.Binlog{{MemorySize: agreementGiB, EntriesNum: 3_000_000}}},
		{FieldID: 1, ChildFields: []int64{101}, Binlogs: []*datapb.Binlog{{MemorySize: agreementGiB, EntriesNum: 3_000_000}}},
		{FieldID: 2, ChildFields: []int64{102}, Binlogs: []*datapb.Binlog{{MemorySize: agreementGiB, EntriesNum: 3_000_000}}},
	}

	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID:             2,
		NumOfRows:      3_000_000,
		Binlogs:        insert,
		StorageVersion: 3,
		State:          commonpb.SegmentState_Flushed,
	})

	coordSide := taskresource.EstimateCompaction(taskresource.CompactionInput{
		Type:            datapb.CompactionType_SortCompaction,
		StorageVersion:  seg.GetStorageVersion(),
		TotalMemorySize: seg.getSegmentSize(),
		TotalRows:       seg.GetNumOfRows(),
	})

	plan := &datapb.CompactionPlan{
		PlanID: 4243,
		Type:   datapb.CompactionType_SortCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{SegmentID: seg.GetID(), StorageVersion: seg.GetStorageVersion(), FieldBinlogs: insert},
		},
	}

	assert.Equal(t, coordSide, taskresource.RequirementForCompaction(plan))

	// Anti-vacuity: the row term must actually be visible in the answer, or
	// this test would pass with the row count dropped entirely.
	withoutRows := taskresource.EstimateCompaction(taskresource.CompactionInput{
		Type:            datapb.CompactionType_SortCompaction,
		StorageVersion:  seg.GetStorageVersion(),
		TotalMemorySize: seg.getSegmentSize(),
	})
	require.Greater(t, coordSide.Memory, withoutRows.Memory)
}
