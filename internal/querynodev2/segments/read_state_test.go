package segments

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

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
