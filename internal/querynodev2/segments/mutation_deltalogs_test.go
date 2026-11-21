package segments

import (
	"fmt"
	"math"
	"path"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storage/deltacodec"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

func (suite *SegmentLoaderSuite) TestMixedMutationFilesRestoreWithoutTail() {
	t := suite.T()
	ctx := t.Context()
	collection, partition, id := suite.collectionID+1, suite.partitionID, suite.segmentID
	schema := proto.Clone(suite.schema).(*schemapb.CollectionSchema)
	for _, f := range schema.Fields {
		if f.DataType == schemapb.DataType_Int32 {
			f.Nullable = true
		}
	}
	require.NoError(t, suite.manager.Collection.PutOrRef(collection, schema, mock_segcore.GenTestIndexMeta(collection, schema), &querypb.LoadMetaInfo{CollectionID: collection, LoadType: querypb.LoadType_LoadCollection, PartitionIDs: []int64{partition}}))
	defer suite.manager.Collection.Unref(collection, 1)
	vchannel := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collection)
	data, err := mock_segcore.GenInsertData(4, schema)
	require.NoError(t, err)
	var field, pk, extra int64
	for _, f := range schema.Fields {
		if f.GetIsPrimaryKey() {
			pk = f.FieldID
		}
		if f.DataType == schemapb.DataType_Int16 {
			extra = f.FieldID
		}
		if f.DataType == schemapb.DataType_Int32 {
			field = f.FieldID
		}
	}
	score := data.Data[field].(*storage.Int32FieldData)
	score.Nullable = true
	score.ValidData = []bool{true, true, true, true}
	keys := data.Data[pk].(*storage.Int64FieldData)
	keys.Data = []int64{41, 1, 2, 9}
	data.Data[1].(*storage.Int64FieldData).Data = []int64{1, 2, 3, 4}
	logs, stats, err := mock_segcore.SaveBinLogWithData(ctx, collection, partition, id, data, schema, suite.chunkManager)
	require.NoError(t, err)
	legacy, err := mock_segcore.SaveDeltaLog(collection, partition, id, suite.chunkManager)
	require.NoError(t, err)
	makeRow := func(tick uint64, ordinal uint32, value int32) deltacodec.Row {
		return deltacodec.Row{TimeTick: tick, SourceOrdinal: ordinal, Operation: deltacodec.Update, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{41}}}}, Assignments: []*messagespb.UpdateAssignment{{FieldId: field, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_IntData{IntData: value}}}}}}}
	}
	r1, r2 := makeRow(20, 0, 334), makeRow(30, 1, 667)
	omitted := makeRow(20, 0, 99).Assignments[0]
	omitted.FieldId = extra
	r1.Assignments = append(r1.Assignments, omitted)
	null := makeRow(35, 0, 1)
	null.Assignments[0].Literal = &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_NullValue{}}
	file := path.Join(suite.rootPath, fmt.Sprintf("typed-%d", id))
	_, err = storage.WriteMutationDelta(ctx, file, storage.MutationDelta{Layout: deltacodec.Layout{CollectionID: collection, VChannel: vchannel, PartitionID: partition, SchemaVersion: 1, PKType: schemapb.DataType_Int64, Fields: []deltacodec.Field{{ID: extra, Type: schemapb.DataType_Int16}, {ID: field, Type: schemapb.DataType_Int32}}}, Rows: []deltacodec.Row{r1, r2, null}}, createStorageConfig(), nil)
	require.NoError(t, err)
	info := &querypb.SegmentLoadInfo{SegmentID: id, PartitionID: partition, CollectionID: collection, InsertChannel: vchannel, BinlogPaths: logs, Statslogs: stats, NumOfRows: 4, TransformStartAfterTimetick: 11, Deltalogs: append(legacy, &datapb.FieldBinlog{Binlogs: []*datapb.Binlog{{LogPath: file, EntriesNum: 3}}})}
	loaded, err := suite.loader.Load(ctx, collection, SegmentTypeSealed, 0, info)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	local := loaded[0].(*LocalSegment)
	defer local.Release(ctx)
	read := func(local *LocalSegment, ts uint64, field int64) (int32, bool) {
		encoded, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Limit: 4}}, OutputFieldIds: []int64{field}})
		require.NoError(t, err)

		plan, err := segcore.NewRetrievePlan(local.GetCollection().GetCCollection(), encoded, ts, 100, 0, 0, 0)
		require.NoError(t, err)
		defer plan.Delete()
		result, err := local.RetrieveByOffsets(ctx, &segcore.RetrievePlanWithOffsets{RetrievePlan: plan, Offsets: []int64{0}})
		require.NoError(t, err)
		for _, f := range result.FieldsData {
			if f.FieldId == field {
				if valid := f.GetValidData(); len(valid) > 0 && !valid[0] {
					return 0, false
				}
				return f.GetScalars().GetIntData().Data[0], true
			}
		}
		t.Fatal("missing historical field")
		return 0, false
	}
	check := func(ts uint64, want int32, present bool) {
		got, valid := read(local, ts, field)
		require.Equal(t, present, valid)
		if valid {
			require.Equal(t, want, got)
		}
	}
	check(19, score.Data[0], true)
	check(29, 334, true)
	check(30, 667, true)
	check(35, 0, false)
	require.NoError(t, local.ApplyUpdates(ctx, r1.TimeTick, r1.SourceOrdinal, &messagespb.UpdateMessageBody{PrimaryKeys: r1.PrimaryKeys, Assignments: r1.Assignments}))
	check(30, 667, true)

	// An unpublished Transform mutation spans one common and one newly
	// requested field. A narrow physical store must keep the common change.
	narrowScope := local.GetCollection().WithLoadScope([]int64{partition}, []int64{0, 1, pk, field})
	newBase := func(scope *Collection) *LocalSegment {
		seg, err := NewSegment(ctx, scope, suite.manager.Segment, SegmentTypeSealed, 0, info)
		require.NoError(t, err)

		require.NoError(t, suite.loader.LoadSegment(ctx, seg, info))
		require.NoError(t, suite.loader.LoadDeltaLogsWithoutResource(ctx, seg, info))
		return seg.(*LocalSegment)
	}
	// Ordinary segment loading also accepts a strict field set on its owning collection.
	owner, err := NewCollection(collection, schema, nil, &querypb.LoadMetaInfo{
		CollectionID: collection, LoadType: querypb.LoadType_LoadCollection,
		PartitionIDs: []int64{partition}, LoadFields: []int64{0, 1, pk, field},
	})
	require.NoError(t, err)
	defer DeleteCollection(owner)
	partial := newBase(owner)
	defer partial.Release(ctx)
	require.Contains(t, partial.updateFields, field)
	require.NotContains(t, partial.updateFields, extra)
	persisted, valid := read(partial, 20, field)
	require.True(t, valid)
	require.EqualValues(t, 334, persisted)
	persisted, valid = read(local, 20, extra)
	require.True(t, valid)
	require.EqualValues(t, 99, persisted)

	narrow := newBase(narrowScope)
	defer narrow.Release(ctx)
	nonPersisted := makeRow(41, 2, 819)
	second := makeRow(41, 2, 123).Assignments[0]
	second.FieldId = extra
	body := &messagespb.UpdateMessageBody{PrimaryKeys: nonPersisted.PrimaryKeys, Assignments: append(nonPersisted.Assignments, second)}
	require.NoError(t, narrow.ApplyUpdates(ctx, 41, 2, body))
	commonValue, valid := read(narrow, 41, field)
	require.True(t, valid)
	require.EqualValues(t, 819, commonValue)
	wideScope := local.GetCollection().WithLoadScope([]int64{partition}, []int64{0, 1, pk, field, extra})
	versions := func(s *LocalSegment) uint64 {
		n, err := segcore.UpdateVersionCountForTest(s.csegment)
		require.NoError(t, err)
		return n
	}
	before := versions(narrow)
	require.NoError(t, narrow.GCUpdates(ctx, 20))
	require.Equal(t, before, versions(narrow))
	require.NoError(t, narrow.GCUpdates(ctx, 35))
	require.Less(t, versions(narrow), before)
	_, valid = read(narrow, 35, field)
	require.False(t, valid)
	// A compatible new base reconstructs all times from the original files.
	wide := newBase(wideScope)
	restored, valid := read(wide, 20, field)
	require.True(t, valid)
	require.EqualValues(t, 334, restored)
	defer wide.Release(ctx)
	// Replay from the same persisted base/cursor into the wider registration.
	require.NoError(t, wide.ApplyUpdates(ctx, 41, 2, body))
	commonValue, valid = read(wide, 41, field)
	require.True(t, valid)
	require.EqualValues(t, 819, commonValue)
	newValue, valid := read(wide, 41, extra)
	require.True(t, valid)
	require.EqualValues(t, 123, newValue)
	_, valid = read(narrow, 35, field)
	require.False(t, valid)
}

func (suite *SegmentLoaderSuite) TestArrayDeltaReloadAndReplay() {
	t := suite.T()
	ctx := t.Context()
	collection, partition, id := suite.collectionID+1, suite.partitionID, suite.segmentID
	schema := proto.Clone(suite.schema).(*schemapb.CollectionSchema)
	const plain, nullable int64 = 300, 301
	for _, field := range []int64{plain, nullable} {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: field, Name: fmt.Sprintf("values%d", field), DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, Nullable: true, ElementNullable: field == nullable})
	}
	require.NoError(t, suite.manager.Collection.PutOrRef(collection, schema, mock_segcore.GenTestIndexMeta(collection, schema), &querypb.LoadMetaInfo{CollectionID: collection, LoadType: querypb.LoadType_LoadCollection, PartitionIDs: []int64{partition}}))
	defer suite.manager.Collection.Unref(collection, 1)
	data, err := mock_segcore.GenInsertData(4, schema)
	require.NoError(t, err)
	keys := data.Data[GetPkField(schema).FieldID].(*storage.Int64FieldData)
	keys.Data = []int64{10, 11, 12, 13}
	data.Data[1].(*storage.Int64FieldData).Data = []int64{1, 2, 3, 4}
	for _, f := range []int64{plain, nullable} {
		base := data.Data[f].(*storage.ArrayFieldData)
		base.Nullable = true
		base.ElementNullable = f == nullable
		base.ValidData = []bool{true, true, true, true}
		for _, row := range base.Data {
			if f == nullable {
				for range row.GetIntData().Data {
					row.ValidData = append(row.ValidData, true)
				}
			}
		}
	}
	reader, err := storage.NewInsertDataRecordReader(data, schema)
	require.NoError(t, err)
	defer reader.Close()
	record, err := reader.Next()
	require.NoError(t, err)
	writer, err := storage.NewBinlogRecordWriter(ctx, collection, partition, id, schema, allocator.NewLocalAllocator(100, math.MaxInt64), 1<<20, 4, storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(createStorageConfig()), storage.WithWriterFormat("parquet"))
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	logs, _, _, manifest, _ := writer.GetLogs()
	require.NoError(t, err)
	channel := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collection)
	layout := deltacodec.Layout{CollectionID: collection, PartitionID: partition, VChannel: channel, PKType: schemapb.DataType_Int64}
	var rows []deltacodec.Row
	for _, field := range []int64{plain, nullable} {
		// Write the original v1 field directory without an element-nullability marker.
		layout.Fields = append(layout.Fields, deltacodec.Field{ID: field, Type: schemapb.DataType_Array, Element: schemapb.DataType_Int32})
		for i, key := range keys.Data {
			arr := &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{}}}
			value := &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ArrayValue{ArrayValue: arr}}
			switch i {
			case 0, 1:
				arr.GetIntData().Data = []int32{5, 6}
				if field == nullable {
					arr.ValidData = []bool{true, i == 0}
					if i == 1 {
						arr.GetIntData().Data[1] = 0
					}
				}
			case 3:
				value = &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_NullValue{}}
			}
			rows = append(rows, deltacodec.Row{Operation: deltacodec.Update, TimeTick: 20, SourceOrdinal: uint32(len(rows)), PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{key}}}}, Assignments: []*messagespb.UpdateAssignment{{FieldId: field, Literal: value}}})
		}
	}
	base, _, err := packed.UnmarshalManifestPath(manifest)
	require.NoError(t, err)
	file := metautil.BuildDeltaLogPathV3(base, 30000)
	_, err = storage.WriteMutationDelta(ctx, file, storage.MutationDelta{Layout: layout, Rows: rows}, createStorageConfig(), nil)
	require.NoError(t, err)
	manifest, err = packed.AddDeltaLogsToManifest(manifest, createStorageConfig(), []packed.DeltaLogEntry{{Path: file, NumEntries: int64(len(rows))}})
	require.NoError(t, err)
	info := &querypb.SegmentLoadInfo{SegmentID: id, PartitionID: partition, CollectionID: collection, InsertChannel: channel, BinlogPaths: storage.SortFieldBinlogs(logs), StorageVersion: storage.StorageV3, ManifestPath: manifest, NumOfRows: 4}
	loaded, err := suite.loader.Load(ctx, collection, SegmentTypeSealed, 0, info)
	require.NoError(t, err)
	local := loaded[0].(*LocalSegment)
	defer local.Release(ctx)
	for _, r := range rows {
		a := r.Assignments[0]
		// The live payload and the restored entry must agree at the same identity.
		require.NoError(t, local.ApplyUpdates(ctx, r.TimeTick, r.SourceOrdinal, &messagespb.UpdateMessageBody{PrimaryKeys: r.PrimaryKeys, Assignments: r.Assignments}))
		encoded, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Limit: 4}}, OutputFieldIds: []int64{a.FieldId}})
		require.NoError(t, err)
		plan, err := segcore.NewRetrievePlan(local.GetCollection().GetCCollection(), encoded, 20, 100, 0, 0, 0)
		require.NoError(t, err)
		res, err := local.RetrieveByOffsets(ctx, &segcore.RetrievePlanWithOffsets{RetrievePlan: plan, Offsets: []int64{r.PrimaryKeys.GetIntId().Data[0] - 10}})
		plan.Delete()
		require.NoError(t, err)
		var got *schemapb.FieldData
		for _, f := range res.FieldsData {
			if f.FieldId == a.FieldId {
				got = f
			}
		}
		require.NotNil(t, got)
		require.Equal(t, a.Literal.GetArrayValue() != nil, got.ValidData[0])
		if got.ValidData[0] {
			require.True(t, proto.Equal(a.Literal.GetArrayValue(), got.GetScalars().GetArrayData().Data[0]), "field=%d ordinal=%d got=%v", a.FieldId, r.SourceOrdinal, got)
		}
	}
}
