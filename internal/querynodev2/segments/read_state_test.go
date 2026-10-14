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
