package segments

import (
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (suite *SegmentSuite) TestViewExplicitEmptyCreateLoadsOnlyDependencies() {
	ctx := suite.T().Context()
	local := suite.sealed.(*LocalSegment)
	binlogs, _, err := mock_segcore.SaveBinLog(ctx, suite.collectionID, suite.partitionID,
		suite.segmentID, 100, suite.collection.Schema(), suite.chunkManager)
	suite.Require().NoError(err)
	info := proto.Clone(local.LoadInfo()).(*querypb.SegmentLoadInfo)
	info.BinlogPaths = binlogs
	physical, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection: suite.collection.GetCCollection(), SegmentID: suite.segmentID,
		SegmentType: segcore.SegmentTypeSealed, LoadInfo: info, ExplicitLoadScope: true,
	})
	suite.Require().NoError(err)
	defer physical.Release()
	suite.Require().NoError(physical.Load(ctx))
	suite.True(physical.HasFieldData(GetPkField(suite.collection.Schema()).GetFieldID()))
	suite.False(physical.HasFieldData(106), "an empty explicit scope cannot load all user columns")
	suite.True(local.csegment.HasFieldData(106), "another scope keeps its own column")
	suite.Equal(int64(100), physical.RowNum())
}

func (suite *SegmentSuite) TestViewReadStateSurvivesReopenAndSharesDeletes() {
	ctx := suite.T().Context()
	local := suite.sealed.(*LocalSegment)
	read, err := local.PinReadState()
	suite.Require().NoError(err)
	defer read.ReleaseReadState()
	const removedField = int64(106)
	suite.True(read.HasRawData(removedField))

	query, err := proto.Marshal(&planpb.PlanNode{
		Node:           &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
		OutputFieldIds: []int64{removedField},
	})
	suite.Require().NoError(err)
	plan, err := segcore.NewRetrievePlan(suite.collection.GetCCollection(), query, typeutil.MaxTimestamp, 100, 0, 0, 0)
	suite.Require().NoError(err)
	defer plan.Delete()
	offsets := &segcore.RetrievePlanWithOffsets{RetrievePlan: plan, Offsets: []int64{0, 3}}
	before, err := read.RetrieveByOffsets(ctx, offsets)
	suite.Require().NoError(err)

	oldSchema := suite.collection.Schema()
	schema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
	schema.Version++
	for i, field := range schema.Fields {
		if field.FieldID == removedField {
			schema.Fields = append(schema.Fields[:i], schema.Fields[i+1:]...)
			break
		}
	}
	info := local.LoadInfo()
	suite.Require().NoError(local.csegment.Reopen(ctx, &segcore.ReopenRequest{LoadInfo: info, Schema: schema, SchemaVersion: uint64(schema.Version)}))
	newRead, err := local.PinReadState()
	suite.Require().NoError(err)
	defer newRead.ReleaseReadState()
	suite.False(newRead.(*sealedReadState).native.HasFieldData(removedField))
	suite.True(read.HasRawData(removedField))
	after, err := read.RetrieveByOffsets(ctx, offsets)
	suite.Require().NoError(err)
	suite.True(proto.Equal(before, after), "old view must retain its original output column")

	pks := storage.NewInt64PrimaryKeys(2)
	pks.AppendRaw(0, 1)
	suite.Require().NoError(local.Delete(ctx, pks, []uint64{1000, 1000}))
	suite.Equal(int64(98), read.RowNum())
	suite.Equal(read.RowNum(), newRead.RowNum(), "all read states observe the same applied deletes")
}

func (suite *SegmentSuite) TestViewBorrowedCollectionReopenOutlivesPreparingOwner() {
	ctx := suite.T().Context()
	local := suite.sealed.(*LocalSegment)
	read, err := local.PinReadState()
	suite.Require().NoError(err)
	defer read.ReleaseReadState()
	schema := suite.collection.Schema()
	owner, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: local.Collection(), Schema: schema,
	})
	suite.Require().NoError(err)
	defer func() {
		if owner != nil {
			owner.Release()
		}
	}()
	const removedField = int64(106)
	var fields []int64
	for _, field := range schema.Fields {
		if field.FieldID != removedField {
			fields = append(fields, field.FieldID)
		}
	}
	suite.Require().NoError(read.(*sealedReadState).native.Reopen(ctx, &segcore.ReopenRequest{
		LoadInfo: local.LoadInfo(), Collection: owner, LoadFields: fields,
	}))
	// Preparation has completed. Its owner can disappear while the physical
	// result remains usable by an independently pinned query collection.
	owner.Release()
	owner = nil
	suite.False(read.(*sealedReadState).native.HasFieldData(removedField))
	suite.True(local.csegment.HasFieldData(removedField))
	query, err := proto.Marshal(&planpb.PlanNode{
		Node:           &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
		OutputFieldIds: []int64{GetPkField(schema).GetFieldID()},
	})
	suite.Require().NoError(err)
	plan, err := segcore.NewRetrievePlan(suite.collection.GetCCollection(), query, typeutil.MaxTimestamp, 100, 0, 0, 0)
	suite.Require().NoError(err)
	defer plan.Delete()
	offsets := &segcore.RetrievePlanWithOffsets{RetrievePlan: plan, Offsets: []int64{0, 3}}
	expected, err := local.RetrieveByOffsets(ctx, offsets)
	suite.Require().NoError(err)
	actual, err := read.RetrieveByOffsets(ctx, offsets)
	suite.Require().NoError(err)
	suite.True(proto.Equal(expected, actual))
	suite.Equal(local.RowNum(), read.RowNum())
}

func (suite *SegmentSuite) TestViewReadStateLoadScopeReleasesBaseReferences() {
	ctx := suite.T().Context()
	local := suite.sealed.(*LocalSegment)
	old, err := local.PinReadState()
	suite.Require().NoError(err)
	defer old.ReleaseReadState()
	const fieldID = int64(106)
	suite.True(old.HasRawData(fieldID))
	narrow, err := local.PinReadState()
	suite.Require().NoError(err)
	defer narrow.ReleaseReadState()
	var fields []int64
	for _, field := range suite.collection.Schema().Fields {
		if field.FieldID != fieldID {
			fields = append(fields, field.FieldID)
		}
	}
	suite.Require().NoError(narrow.(*sealedReadState).native.Reopen(ctx, &segcore.ReopenRequest{
		LoadInfo: local.LoadInfo(), Schema: suite.collection.Schema(), LoadFields: fields,
	}))
	suite.False(narrow.(*sealedReadState).native.HasFieldData(fieldID))
	suite.True(old.(*sealedReadState).native.HasFieldData(fieldID), "preparing a different load scope preserves an old query's column")
	suite.Require().NoError(local.AdoptReadState(narrow))
	suite.False(local.csegment.HasFieldData(fieldID), "the base cannot retain the departed View's column")
	suite.True(old.(*sealedReadState).native.HasFieldData(fieldID))
	pks := storage.NewInt64PrimaryKeys(1)
	pks.AppendRaw(0)
	suite.Require().NoError(local.Delete(ctx, pks, []uint64{1000}))
	suite.Equal(old.RowNum(), narrow.RowNum(), "adopting a scope preserves shared deletes")
}

func (suite *SegmentSuite) TestViewReadStatePreparesEarlierSchema() {
	ctx := suite.T().Context()
	local := suite.sealed.(*LocalSegment)
	newer, err := segcore.PinSealedReadState(local.csegment)
	suite.Require().NoError(err)
	defer newer.Release()
	schema := proto.Clone(suite.collection.Schema()).(*schemapb.CollectionSchema)
	schema.Version += 2
	suite.Require().NoError(newer.Reopen(ctx, &segcore.ReopenRequest{
		LoadInfo: local.LoadInfo(), Schema: schema, SchemaVersion: uint64(schema.Version),
	}))
	older, err := segcore.PinSealedReadState(newer)
	suite.Require().NoError(err)
	defer older.Release()
	original := suite.collection.Schema()
	suite.Require().NoError(older.Reopen(ctx, &segcore.ReopenRequest{
		LoadInfo: local.LoadInfo(), Schema: original, SchemaVersion: uint64(original.Version),
	}), "a later-arriving old View can prepare its exact schema from shared resources")
	suite.Equal(newer.RowNum(), older.RowNum())
}

func (suite *SegmentSuite) TestViewReadStatePreparesWhileDeletesContinue() {
	ctx := suite.T().Context()
	local := suite.sealed.(*LocalSegment)
	old, err := local.PinReadState()
	suite.Require().NoError(err)
	defer old.ReleaseReadState()
	schema := proto.Clone(suite.collection.Schema()).(*schemapb.CollectionSchema)
	schema.Version++
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID: 1000, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true,
	})
	var wg sync.WaitGroup
	errors := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(0); i < 50; i++ {
			pks := storage.NewInt64PrimaryKeys(1)
			pks.AppendRaw(i)
			if err := local.Delete(ctx, pks, []uint64{uint64(1000 + i)}); err != nil {
				errors <- err
				return
			}
		}
	}()
	defer wg.Wait()
	for i := 0; i < 10; i++ {
		read, err := segcore.PinSealedReadState(local.csegment)
		suite.Require().NoError(err)
		err = read.Reopen(ctx, &segcore.ReopenRequest{
			LoadInfo: local.LoadInfo(), Schema: schema, SchemaVersion: uint64(schema.Version),
		})
		read.Release()
		suite.Require().NoError(err)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		suite.Require().NoError(err)
	}
	suite.Equal(int64(50), old.RowNum())
	suite.Equal(old.RowNum(), local.RowNum())
}
