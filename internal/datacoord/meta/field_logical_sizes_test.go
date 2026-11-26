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

package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSaveBinlogFieldLogicalSizes(t *testing.T) {
	for _, mode := range []struct {
		name string
		full bool
	}{{"incremental", false}, {"full", true}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			mt, err := NewMemoryMeta(t)
			require.NoError(t, err)
			const segmentID, collectionID = 101, 2
			require.NoError(t, mt.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
				ID: segmentID, CollectionID: collectionID, PartitionID: 3,
				State: commonpb.SegmentState_Growing, StorageVersion: storage.StorageV3, Level: datapb.SegmentLevel_L1,
			})))
			manifest := func(ver int64) string { return packed.MarshalManifestPath("files/insert_log/2/3/101", ver) }
			request := func(revision, rows int64, tick uint64, sizes *datapb.FieldLogicalSizes) *datapb.SaveBinlogPathsRequest {
				return &datapb.SaveBinlogPathsRequest{
					SegmentID: segmentID, WithFullBinlogs: mode.full, ManifestPath: manifest(revision),
					CheckPoints: []*datapb.CheckPoint{{SegmentID: segmentID, NumOfRows: rows, Position: &msgpb.MsgPosition{Timestamp: tick}}},
					Stats:       &datapb.Statistics{InsertBinlogSize: 90, FieldLogicalSizes: sizes},
				}
			}
			save := func(req *datapb.SaveBinlogPathsRequest) error {
				op := AddBinlogsOperator(segmentID, req.GetField2BinlogPaths(), nil, nil, nil)
				if mode.full {
					op = UpdateBinlogsFromSaveBinlogPathsOperator(segmentID, req.GetField2BinlogPaths(), nil, nil, nil)
				}
				return mt.UpdateSegmentsInfo(ctx, op, UpdateCheckPointOperator(segmentID, req.GetCheckPoints(), mode.full), UpdateManifest(segmentID, req.GetManifestPath()), UpdateSaveBinlogStats(req))
			}
			current := func() *datapb.SegmentInfo { return mt.GetSegment(ctx, segmentID).SegmentInfo }
			initial := &datapb.FieldLogicalSizes{Version: 1, RowCount: 10, Fields: map[int64]*datapb.FieldLogicalSize{101: {}, 102: {PayloadBytes: 19, ValidityBits: 10}}}
			first := request(1, 10, 10, initial)
			require.NoError(t, save(first))
			require.True(t, proto.Equal(initial, SegmentFieldLogicalSizes(current())))
			mt = BootMetaWithLoader(t, mt.Catalog, collectionID, withoutDomainMetas)
			require.Empty(t, current().GetBinlogs())
			require.True(t, proto.Equal(initial, SegmentFieldLogicalSizes(current())))

			// A duplicate may omit a known zero. Zero, absence and a conflicting
			// nonzero value must remain distinguishable after catalog recovery.
			duplicate := proto.CloneOf(first)
			delete(duplicate.Stats.FieldLogicalSizes.Fields, 101)
			require.NoError(t, save(duplicate))
			require.NotNil(t, SegmentFieldLogicalSizes(current()).GetFields()[101])
			duplicate.Stats.FieldLogicalSizes.Fields[102].ValidityBits++
			require.ErrorIs(t, save(duplicate), merr.ErrDataIntegrity)
			require.True(t, proto.Equal(initial, SegmentFieldLogicalSizes(current())))
			seal := proto.CloneOf(first)
			seal.CheckPoints = nil
			seal.Flushed = true
			require.NoError(t, save(seal))

			for _, cp := range []*datapb.CheckPoint{
				{SegmentID: segmentID + 1, NumOfRows: 10, Position: &msgpb.MsgPosition{Timestamp: 11}},
				{SegmentID: segmentID, Position: &msgpb.MsgPosition{Timestamp: 11}},
				{SegmentID: segmentID, NumOfRows: 10},
			} {
				bad := proto.CloneOf(first)
				bad.CheckPoints = []*datapb.CheckPoint{cp}
				require.ErrorIs(t, save(bad), merr.ErrDataIntegrity)
			}
			bad := request(2, 11, 10, initial)
			require.ErrorIs(t, save(bad), merr.ErrDataIntegrity)
			bad = request(2, 11, 12, initial)
			require.ErrorIs(t, save(bad), merr.ErrDataIntegrity)
			require.Equal(t, manifest(1), current().GetManifestPath())

			legacy := request(2, 10, 20, nil)
			legacy.Stats = nil
			require.NoError(t, save(legacy)) // advancing checkpoint, same append-only prefix
			require.True(t, proto.Equal(initial, SegmentFieldLogicalSizes(current())))
			require.Equal(t, manifest(2), current().GetFieldLogicalSizesManifest())
			require.NoError(t, save(request(3, 11, 30, nil))) // one unmetered insert
			require.Nil(t, SegmentFieldLogicalSizes(current()))
			require.NoError(t, save(first)) // lost acknowledgement arrives late
			require.Equal(t, manifest(3), current().GetManifestPath())
			require.EqualValues(t, 11, current().GetNumOfRows())
			require.Nil(t, current().GetStats().GetFieldLogicalSizes())
		})
	}
}

func TestFieldLogicalSizesDowngradeRecovery(t *testing.T) {
	ctx := context.Background()
	mt, err := NewMemoryMeta(t)
	require.NoError(t, err)
	const base = "files/insert_log/2/3/4"
	original := &datapb.SegmentInfo{
		ID: 4, CollectionID: 2, PartitionID: 3, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, NumOfRows: 10, StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath(base, 1),
	}
	stats := &datapb.Statistics{InsertBinlogSize: 100, FieldLogicalSizes: &datapb.FieldLogicalSizes{Version: 1, RowCount: 10, Fields: map[int64]*datapb.FieldLogicalSize{101: {}}}}
	require.NoError(t, mt.installReportedStats(original, stats))
	require.NoError(t, mt.AddSegment(ctx, NewSegmentInfo(original)))
	for _, tc := range []struct {
		name, path, stamp string
		rows              int64
		known             bool
	}{
		{"same revision", `{"base_path":"files/insert_log/2/3/4","size":45,"ver":1}`, original.GetFieldLogicalSizesManifest(), 10, true},
		{"older writer replaced a column", packed.MarshalManifestPath(base, 2), original.GetFieldLogicalSizesManifest(), 10, false},
		{"different base at same revision", packed.MarshalManifestPath("foreign", 1), original.GetFieldLogicalSizesManifest(), 10, false},
		{"unanchored report", original.GetManifestPath(), "", 10, false},
		{"external table path", "table/partition/part.parquet", "table/partition/part.parquet", 10, false},
		{"uncommitted revision", packed.MarshalManifestPath(base, 0), packed.MarshalManifestPath(base, 0), 10, false},
		{"latest selector", packed.MarshalManifestPath(base, -1), packed.MarshalManifestPath(base, -1), 10, false},
		{"missing base", `{"ver":1}`, `{"ver":1}`, 10, false},
		{"older writer appended rows", original.GetManifestPath(), original.GetFieldLogicalSizesManifest(), 11, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate an older binary replacing the catalog record while
			// carrying the optional statistics and stamp through as unknown fields.
			record := proto.CloneOf(original)
			record.ManifestPath, record.FieldLogicalSizesManifest, record.NumOfRows = tc.path, tc.stamp, tc.rows
			require.NoError(t, mt.Catalog.AlterSegments(ctx, []*datapb.SegmentInfo{record}))
			loaded := BootMetaWithLoader(t, mt.Catalog, 2, withoutDomainMetas).GetSegment(ctx, 4)
			require.Empty(t, loaded.GetBinlogs())
			require.EqualValues(t, 100, loaded.GetStats().GetInsertBinlogSize())
			if tc.known {
				require.True(t, proto.Equal(stats.GetFieldLogicalSizes(), SegmentFieldLogicalSizes(loaded.SegmentInfo)))
			} else {
				require.Nil(t, loaded.GetStats().GetFieldLogicalSizes())
				require.Empty(t, loaded.GetFieldLogicalSizesManifest())
			}
		})
	}
}

func TestFieldLogicalSizesExternalCatalog(t *testing.T) {
	ctx := context.Background()
	mt, err := NewMemoryMeta(t)
	require.NoError(t, err)
	mt.AddCollection(&CollectionInfo{ID: 2, Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, ExternalField: "external_id"}}}})
	manifest := packed.MarshalManifestPath("files/insert_log/2/3/4", 1)
	segment := &datapb.SegmentInfo{ID: 4, CollectionID: 2, PartitionID: 3, Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3, NumOfRows: 2, ManifestPath: manifest}
	sizes := &datapb.FieldLogicalSizes{Version: 1, RowCount: 2, Fields: map[int64]*datapb.FieldLogicalSize{100: {}}}
	stats := &datapb.Statistics{InsertBinlogSize: 77, FieldLogicalSizes: sizes}
	require.ErrorIs(t, mt.installReportedStats(segment, stats), merr.ErrDataIntegrity)
	segment.Stats, segment.FieldLogicalSizesManifest = stats, manifest
	require.NoError(t, mt.AddSegment(ctx, NewSegmentInfo(segment)))
	require.NoError(t, LoadAndNormalize(ctx, mt, []int64{2}))
	loaded := mt.GetSegment(ctx, 4)
	require.Nil(t, loaded.GetStats().GetFieldLogicalSizes())
	require.Empty(t, loaded.GetFieldLogicalSizesManifest())
	require.EqualValues(t, 77, loaded.GetStats().GetInsertBinlogSize())
}

func TestFieldLogicalSizesMaterialization(t *testing.T) {
	ctx := context.Background()
	segment := &datapb.SegmentInfo{ID: 4, StorageVersion: storage.StorageV3, NumOfRows: 2, ManifestPath: packed.MarshalManifestPath("base", 1)}
	base := &datapb.Statistics{InsertBinlogSize: 20, FieldLogicalSizes: &datapb.FieldLogicalSizes{Version: 1, RowCount: 2, Fields: map[int64]*datapb.FieldLogicalSize{100: {PayloadBytes: 17}, 101: {PayloadBytes: 4}, 102: {}}}}
	mt, err := NewMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, mt.installReportedStats(segment, base))
	report := &datapb.CompactionSegment{NumOfRows: 2, FieldLogicalSizeUpdateIds: []int64{100, 101}, Stats: &datapb.Statistics{InsertBinlogSize: 5, FieldLogicalSizes: &datapb.FieldLogicalSizes{Version: 1, RowCount: 2, Fields: map[int64]*datapb.FieldLogicalSize{100: {}}}}}
	merged := addStatsDelta(ctx, NewSegmentInfo(segment), report, nil)
	require.EqualValues(t, 25, merged.GetInsertBinlogSize())
	require.True(t, proto.Equal(&datapb.FieldLogicalSize{}, merged.FieldLogicalSizes.Fields[100]))
	require.NotContains(t, merged.FieldLogicalSizes.Fields, int64(101))
	require.NotNil(t, merged.FieldLogicalSizes.Fields[102])
	require.EqualValues(t, 17, segment.Stats.FieldLogicalSizes.Fields[100].PayloadBytes)
	for _, rows := range []int64{1, 3} {
		report.NumOfRows = rows
		partial := addStatsDelta(ctx, NewSegmentInfo(segment), report, nil)
		require.Len(t, partial.FieldLogicalSizes.Fields, 1)
		require.NotNil(t, partial.FieldLogicalSizes.Fields[102])
	}
	for _, legacy := range []*datapb.CompactionSegment{
		{ManifestDelta: &datapb.SegmentManifestDelta{ColumnGroups: []*datapb.ManifestColumnGroup{{Columns: []string{"101"}}}}},
		{InsertLogs: []*datapb.FieldBinlog{{FieldID: 0, ChildFields: []int64{101}}}},
	} {
		got := addStatsDelta(ctx, NewSegmentInfo(segment), legacy, nil)
		require.NotContains(t, got.FieldLogicalSizes.Fields, int64(101))
		require.NotNil(t, got.FieldLogicalSizes.Fields[100])
		require.NotNil(t, got.FieldLogicalSizes.Fields[102])
	}
	schema := &schemapb.CollectionSchema{StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 102, Fields: []*schemapb.FieldSchema{{FieldID: 101}}}}}
	legacyGroup := &datapb.CompactionSegment{InsertLogs: []*datapb.FieldBinlog{{ChildFields: []int64{101}}}}
	got := addStatsDelta(ctx, NewSegmentInfo(segment), legacyGroup, schema)
	require.Len(t, got.FieldLogicalSizes.Fields, 1)
	require.NotNil(t, got.FieldLogicalSizes.Fields[100])
	unknown := addStatsDelta(ctx, NewSegmentInfo(segment), &datapb.CompactionSegment{InsertLogs: []*datapb.FieldBinlog{{FieldID: 101}}}, nil)
	require.Nil(t, unknown.FieldLogicalSizes)
}

func TestSameRowManifestFieldLogicalSizes(t *testing.T) {
	ctx := context.Background()
	mt, err := NewMemoryMeta(t)
	require.NoError(t, err)
	in := newCompactionCreateTsTestSegment(1, datapb.SegmentLevel_L1)
	in.StorageVersion, in.ManifestPath = storage.StorageV3, packed.MarshalManifestPath("base", 3)
	sizes := &datapb.FieldLogicalSizes{Version: 1, RowCount: in.GetNumOfRows(), Fields: map[int64]*datapb.FieldLogicalSize{100: {}}}
	require.NoError(t, mt.installReportedStats(in.SegmentInfo, &datapb.Statistics{FieldLogicalSizes: sizes}))
	require.NoError(t, mt.AddSegment(ctx, in))
	task := &datapb.CompactionTask{PlanID: 5, Type: datapb.CompactionType_BumpSchemaVersionCompaction, CollectionID: in.GetCollectionID(), InputSegments: []int64{1}, Schema: &schemapb.CollectionSchema{Version: 2}}
	out := &datapb.CompactionSegment{SegmentID: 1, StorageVersion: storage.StorageV3, Manifest: in.ManifestPath, BaseManifest: in.ManifestPath, NumOfRows: in.GetNumOfRows()}
	_, err = mt.CompleteCompactionMutation(ctx, task, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{out}})
	require.NoError(t, err)
	require.EqualValues(t, 2, mt.GetSegment(ctx, 1).GetSchemaVersion())
	for _, revision := range []int64{3, 2} {
		require.NoError(t, mt.UpdateSegmentsInfo(ctx, UpdateManifestVersion(1, revision)))
		require.True(t, proto.Equal(sizes, SegmentFieldLogicalSizes(mt.GetSegment(ctx, 1).SegmentInfo)))
	}
	require.NoError(t, mt.UpdateSegmentsInfo(ctx, UpdateManifestVersion(1, 4)))
	require.Nil(t, mt.GetSegment(ctx, 1).GetStats().GetFieldLogicalSizes())
}

func TestFieldLogicalSizesCompactionAdoption(t *testing.T) {
	for _, typ := range []datapb.CompactionType{datapb.CompactionType_MixCompaction, datapb.CompactionType_ClusteringCompaction, datapb.CompactionType_SortCompaction, datapb.CompactionType_BumpSchemaVersionCompaction} {
		t.Run(typ.String(), func(t *testing.T) {
			ctx := context.Background()
			mt, err := NewMemoryMeta(t)
			require.NoError(t, err)
			in := newCompactionCreateTsTestSegment(1, datapb.SegmentLevel_L1)
			in.StorageVersion, in.ManifestPath = storage.StorageV3, packed.MarshalManifestPath("input", 1)
			require.NoError(t, mt.AddSegment(ctx, in))
			task := &datapb.CompactionTask{PlanID: 11, Type: typ, InputSegments: []int64{1}, CollectionID: in.GetCollectionID(), Channel: in.GetInsertChannel(), Schema: &schemapb.CollectionSchema{Version: 2}, PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 10, End: 11}}
			out := &datapb.CompactionSegment{
				SegmentID: 10, NumOfRows: 2, StorageVersion: storage.StorageV3, Manifest: packed.MarshalManifestPath("output", 1), IsSorted: true,
				Stats: &datapb.Statistics{FieldLogicalSizes: &datapb.FieldLogicalSizes{Version: 1, RowCount: 3, Fields: map[int64]*datapb.FieldLogicalSize{100: {PayloadBytes: 18}}}},
			}
			apply := func() error {
				_, err := mt.CompleteCompactionMutation(ctx, task, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{out}})
				return err
			}
			require.ErrorIs(t, apply(), merr.ErrDataIntegrity)
			require.Nil(t, mt.GetSegment(ctx, 10))
			out.Stats.FieldLogicalSizes.RowCount = 2
			require.NoError(t, apply())
			require.NoError(t, apply())
			out.Stats.FieldLogicalSizes.Fields[100].PayloadBytes++
			require.ErrorIs(t, apply(), merr.ErrDataIntegrity)
			got := mt.GetSegment(ctx, 10)
			require.EqualValues(t, 18, SegmentFieldLogicalSizes(got.SegmentInfo).GetFields()[100].GetPayloadBytes())
		})
	}
}
