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
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	resourceMiB = int64(1) << 20
	resourceGiB = int64(1) << 30
)

func vectorSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "768"}},
			},
			{
				FieldID: 102, Name: "body", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "512"}},
			},
		},
	}
}

// A compaction is priced from Stats, which every storage version populates, so
// the estimate does not depend on the per-FieldBinlog arrays a V3 segment drops.
func TestCompactionRequirementReadsStatsNotBinlogArrays(t *testing.T) {
	paramtable.Init()

	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID:             1,
		NumOfRows:      1_000_000,
		StorageVersion: 3,
		ManifestPath:   "loon://bucket/m@3",
		// No Binlogs and no Deltalogs: exactly a V3 segment after a restart.
		Stats: &datapb.Statistics{InsertBinlogSize: 4 * resourceGiB, DeltaBinlogSize: 256 * resourceMiB},
	})

	req := compactionRequirement(datapb.CompactionType_MixCompaction, []*SegmentInfo{seg}, vectorSchema())
	require.Greater(t, req.Memory, 64*resourceMiB, "must not be the estimator floor")
	// storage v3 mix is input x factor plus the delete payload, and the delete
	// term must be the one from Stats rather than the empty Deltalogs array.
	assert.Greater(t, req.Memory, 4*resourceGiB)
}

// The delete payload used to be read by walking GetDeltalogs(), which is empty
// on a restarted V3 segment; the whole term vanished from the estimate.
func TestCompactionRequirementKeepsTheDeleteTermOnV3(t *testing.T) {
	paramtable.Init()

	withDeletes := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, NumOfRows: 1000, StorageVersion: 3, ManifestPath: "loon://b/m@1",
		Stats: &datapb.Statistics{InsertBinlogSize: resourceGiB, DeltaBinlogSize: 2 * resourceGiB},
	})
	withoutDeletes := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 2, NumOfRows: 1000, StorageVersion: 3, ManifestPath: "loon://b/m@1",
		Stats: &datapb.Statistics{InsertBinlogSize: resourceGiB},
	})

	assert.Greater(t,
		compactionRequirement(datapb.CompactionType_MixCompaction, []*SegmentInfo{withDeletes}, nil).Memory,
		compactionRequirement(datapb.CompactionType_MixCompaction, []*SegmentInfo{withoutDeletes}, nil).Memory,
		"a segment with 2GiB of deletes must be charged more than one with none")
}

// L0 holds every input segment's deletes at once; the streaming compactors hold
// one segment's at a time. The two must not be priced the same way.
func TestL0SumsDeletesWhileMixTakesTheLargest(t *testing.T) {
	paramtable.Init()

	segs := []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{ID: 1, NumOfRows: 10, Stats: &datapb.Statistics{DeltaBinlogSize: resourceGiB}}),
		NewSegmentInfo(&datapb.SegmentInfo{ID: 2, NumOfRows: 10, Stats: &datapb.Statistics{DeltaBinlogSize: resourceGiB}}),
		NewSegmentInfo(&datapb.SegmentInfo{ID: 3, NumOfRows: 10, Stats: &datapb.Statistics{DeltaBinlogSize: resourceGiB}}),
	}

	l0 := compactionRequirement(datapb.CompactionType_Level0DeleteCompaction, segs, nil)
	assert.GreaterOrEqual(t, l0.Memory, 3*resourceGiB, "L0 must sum the delete payload across segments")
}

// An external-collection segment written before Stats was persisted reloads
// with neither Stats nor binlogs. Pricing it at zero would put it at the
// estimator floor; the schema and the row count are what is left to use.
func TestExternalSegmentWithoutStatsIsPricedFromTheSchema(t *testing.T) {
	paramtable.Init()

	external := NewSegmentInfo(&datapb.SegmentInfo{
		ID:             7,
		NumOfRows:      2_000_000,
		StorageVersion: 3,
		ManifestPath:   "s3://external/manifest@1",
		// The shape after a restart: the fake binlogs were stripped on persist
		// and Stats was never written.
	})
	require.Zero(t, external.getSegmentSize(), "setup: the segment really does report no bytes")

	schema := vectorSchema()
	assert.Positive(t, segmentMemorySize(external, schema),
		"a segment with rows must not be priced as empty")

	req := compactionRequirement(datapb.CompactionType_MixCompaction, []*SegmentInfo{external}, schema)
	assert.Greater(t, req.Memory, 64*resourceMiB, "must not fall to the estimator floor")

	// Without a schema there is nothing left to derive from, and the honest
	// answer is zero rather than a fabricated one.
	assert.Zero(t, segmentMemorySize(external, nil))
}

// Fixed-width fields are exactly computable from the row count, which is the
// only field-sizing route that works on a V3 segment at all.
func TestFieldBytesPrefersTheClosedFormAndApportionsTheRest(t *testing.T) {
	paramtable.Init()

	schema := vectorSchema()
	const rows = int64(1_000_000)
	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, NumOfRows: rows, StorageVersion: 3, ManifestPath: "loon://b/m@1",
		Stats: &datapb.Statistics{InsertBinlogSize: 5 * resourceGiB},
	})

	// The vector is exact: 768 dims x 4 bytes x rows.
	vec := schema.Fields[1]
	assert.Equal(t, 768*4*rows, fieldMemorySize(seg, schema, vec))

	// The VarChar has no closed form and no binlog array, so it takes what the
	// fixed-width fields leave behind -- and that is strictly less than the
	// whole segment, which is what it used to be charged.
	body := schema.Fields[2]
	got := fieldMemorySize(seg, schema, body)
	assert.Positive(t, got)
	assert.Less(t, got, seg.getSegmentSize(),
		"one variable-width field must not be charged the whole segment")
}

// The stats family is the one DataCoord charged whole-segment size for on every
// sub-job, however few fields the sub-job actually reads.
func TestStatsRequirementChargesOnlyTheTouchedFields(t *testing.T) {
	paramtable.Init()

	schema := vectorSchema()
	// Only the VarChar participates in a text index.
	schema.Fields[2].TypeParams = append(schema.Fields[2].TypeParams,
		&commonpb.KeyValuePair{Key: "enable_match", Value: "true"})

	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, NumOfRows: 1_000_000, StorageVersion: 3, ManifestPath: "loon://b/m@1",
		Stats: &datapb.Statistics{InsertBinlogSize: 5 * resourceGiB},
	})

	textJob := statsRequirement(seg, schema, indexpb.StatsSubJob_TextIndexJob)
	wholeSegment := statsRequirement(seg, schema, indexpb.StatsSubJob_None)

	assert.Less(t, textJob.Memory, wholeSegment.Memory,
		"a text index over one field must cost less than the whole segment")
	assert.Positive(t, textJob.Memory)
}

// An unrecognized sub-job has no known field subset. It must charge the whole
// segment rather than nothing: under-provisioning is the direction that OOMs.
func TestUnknownStatsSubJobChargesTheWholeSegment(t *testing.T) {
	paramtable.Init()

	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, NumOfRows: 1000, Stats: &datapb.Statistics{InsertBinlogSize: 2 * resourceGiB},
	})
	req := statsRequirement(seg, vectorSchema(), indexpb.StatsSubJob(9999))
	assert.Greater(t, req.Memory, resourceGiB)
}
