package segments

import (
	"strconv"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Exercise the production preparation path instead of a pin-only Go API.
func (suite *SegmentSuite) prepareReadState(local *LocalSegment) (ReadState, error) {
	var fields []int64
	for _, field := range typeutil.GetAllFieldSchemas(suite.collection.Schema()) {
		fields = append(fields, field.GetFieldID())
	}
	collection := suite.collection.WithLoadScope([]int64{suite.partitionID}, fields)
	info := proto.Clone(local.LoadInfo()).(*querypb.SegmentLoadInfo)
	return local.PrepareReadState(suite.T().Context(), nil, info, collection)
}

func (suite *SegmentSuite) TestViewReadStateArrowSurvivesAdopt() {
	ctx := suite.T().Context()
	local := suite.sealed.(*LocalSegment)
	old, err := suite.prepareReadState(local)
	suite.Require().NoError(err)
	defer old.ReleaseReadState()
	const fieldID = int64(106)
	schema := suite.collection.Schema()
	outputs := []int64{GetPkField(schema).GetFieldID(), fieldID}
	expr, err := buildArrowTestRetrievePlanExpr(schema, outputs)
	suite.Require().NoError(err)
	planNode := &planpb.PlanNode{}
	suite.Require().NoError(proto.Unmarshal(expr, planNode))
	plan, err := buildArrowTestRetrievePlan(suite.collection.GetCCollection(), outputs)
	suite.Require().NoError(err)
	defer plan.Delete()
	req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{
		CollectionID: suite.collectionID, OutputFieldsId: outputs,
		SerializedExprPlan: expr, Limit: typeutil.Unlimited,
	}, Scope: querypb.DataScope_Historical}

	params := paramtable.Get()
	key := params.CommonCfg.InterfaceZeroCopyEnabled.Key
	previous := params.CommonCfg.InterfaceZeroCopyEnabled.GetValue()
	defer params.Save(key, previous)
	retrieve := func(segment Segment, useArrow bool) *segcorepb.RetrieveResults {
		suite.Require().NoError(params.Save(key, strconv.FormatBool(useArrow)))
		selected := []Segment{segment}
		results, err := RetrieveSelectedSegments(ctx, plan, req, selected)
		suite.Require().NoError(err)
		defer ReleaseRecords(results)
		suite.Require().Len(results, 1)
		var records []arrow.Record
		if useArrow {
			suite.Require().NotNil(results[0].Record)
			records = []arrow.Record{results[0].Record}
		} else {
			suite.Nil(results[0].Record)
		}
		result, selection, err := RunQNQueryPipeline(ctx, req, schema, planNode,
			[]*segcorepb.RetrieveResults{results[0].Result}, records, selected, suite.manager, plan)
		suite.Require().NoError(err)
		suite.Require().NoError(queryutil.MaterializeArrowSelection(result, selection, schema))
		return result
	}
	expected := retrieve(old, false)
	suite.Equal([]int64{1, 2, 3}, expected.GetIds().GetIntId().GetData())
	suite.True(proto.Equal(expected, retrieve(old, true)))
	suite.True(proto.Equal(expected, retrieve(local, true)), "the legacy segment also retains Arrow behavior")

	narrow, err := suite.prepareReadState(local)
	suite.Require().NoError(err)
	defer narrow.ReleaseReadState()
	var fields []int64
	for _, field := range schema.Fields {
		if field.FieldID != fieldID {
			fields = append(fields, field.FieldID)
		}
	}
	suite.Require().NoError(narrow.(*sealedReadState).native.Reopen(ctx, &segcore.ReopenRequest{
		LoadInfo: local.LoadInfo(), Collection: suite.collection.GetCCollection(), LoadFields: fields,
	}))
	suite.Require().NoError(local.AdoptReadState(narrow))
	suite.False(local.csegment.HasFieldData(fieldID))
	suite.True(old.(*sealedReadState).native.HasFieldData(fieldID))
	suite.True(proto.Equal(expected, retrieve(old, false)))
	suite.True(proto.Equal(expected, retrieve(old, true)), "Arrow must read the old View's columns after the base adopts another scope")
	suite.False(local.csegment.HasFieldData(fieldID), "querying the retained View must not restore columns on the base")
}

func (suite *SegmentSuite) TestViewReadStateArrowUsesQuerySchema() {
	read, err := suite.prepareReadState(suite.sealed.(*LocalSegment))
	suite.Require().NoError(err)
	defer read.ReleaseReadState()
	const fieldID = int64(106)
	schema := proto.Clone(suite.collection.Schema()).(*schemapb.CollectionSchema)
	schema.Version++
	for _, field := range schema.Fields {
		if field.FieldID == fieldID {
			field.Name = "name_from_query_view"
		}
	}
	owner, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: suite.collectionID, Schema: schema,
	})
	suite.Require().NoError(err)
	defer owner.Release()
	plan, err := buildArrowTestRetrievePlan(owner, []int64{common.TimeStampField, fieldID})
	suite.Require().NoError(err)
	defer plan.Delete()
	_, record, err := read.RetrieveArrow(suite.T().Context(), plan)
	suite.Require().NoError(err)
	suite.Require().NotNil(record)
	defer record.Release()
	suite.EqualValues(3, record.NumRows())
	suite.Require().Len(record.Schema().Fields(), 1)
	suite.Equal("name_from_query_view", record.Schema().Field(0).Name, "Arrow output must use the query's schema, not the segment's creating collection")
}

func (suite *SegmentSuite) TestLegacySchemaEvolutionFillsNullableColumn() {
	ctx := suite.T().Context()
	local := suite.sealed.(*LocalSegment)
	oldRead, err := suite.prepareReadState(local)
	suite.Require().NoError(err)
	defer oldRead.ReleaseReadState()

	schema := proto.Clone(suite.collection.Schema()).(*schemapb.CollectionSchema)
	var addedField int64 = 100
	for _, field := range schema.Fields {
		addedField = max(addedField, field.GetFieldID()+1)
	}
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID: addedField, Name: "added_nullable", DataType: schemapb.DataType_Int64, Nullable: true,
	})
	schema.Version++
	owner := suite.collection.GetCCollection()
	suite.Require().NoError(owner.UpdateSchema(schema, uint64(schema.Version)))
	query, err := proto.Marshal(&planpb.PlanNode{
		Node:           &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
		OutputFieldIds: []int64{addedField},
	})
	suite.Require().NoError(err)
	plan, err := segcore.NewRetrievePlan(owner, query, typeutil.MaxTimestamp, 100, 0, 0, 0)
	suite.Require().NoError(err)
	defer plan.Delete()
	nativeResult, err := local.csegment.RetrieveByOffsets(ctx, &segcore.RetrievePlanWithOffsets{
		RetrievePlan: plan, Offsets: []int64{0, 3},
	})
	suite.Require().NoError(err)
	defer nativeResult.Release()
	result, err := nativeResult.GetResult()
	suite.Require().NoError(err)
	suite.Require().Len(result.GetFieldsData(), 1)
	suite.Equal(addedField, result.GetFieldsData()[0].GetFieldId())
	suite.Equal([]bool{false, false}, typeutil.GetFieldDataValidData(result.GetFieldsData()[0]))
	suite.True(local.csegment.HasFieldData(addedField))
	suite.False(oldRead.(*sealedReadState).native.HasFieldData(addedField), "schema evolution must not mutate a pinned old state")
}

func (suite *SegmentSuite) TestLegacyLoadFieldsPreserveLazyColumns() {
	ctx := suite.T().Context()
	schema := proto.Clone(suite.collection.Schema()).(*schemapb.CollectionSchema)
	schema.Version++
	pk := GetPkField(schema).GetFieldID()
	owner, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: suite.collectionID, Schema: schema, LoadFieldList: []int64{pk},
	})
	suite.Require().NoError(err)
	defer owner.Release()
	binlogs, _, err := mock_segcore.SaveBinLog(ctx, suite.collectionID, suite.partitionID,
		suite.segmentID, 100, schema, suite.chunkManager)
	suite.Require().NoError(err)
	info := proto.Clone(suite.sealed.(*LocalSegment).LoadInfo()).(*querypb.SegmentLoadInfo)
	info.BinlogPaths = binlogs
	physical, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection: owner, SegmentID: suite.segmentID, SegmentType: segcore.SegmentTypeSealed,
		LoadInfo: info, LoadFields: []int64{pk},
	})
	suite.Require().NoError(err)
	defer physical.Release()
	suite.Require().NoError(physical.Load(ctx))
	suite.True(physical.HasFieldData(106), "legacy preload selection must retain other lazy columns")
}

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
	read, err := suite.prepareReadState(local)
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
	newRead, err := segcore.PinSealedReadState(local.csegment)
	suite.Require().NoError(err)
	defer newRead.Release()
	suite.False(newRead.HasFieldData(removedField))
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
	read, err := suite.prepareReadState(local)
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
	old, err := suite.prepareReadState(local)
	suite.Require().NoError(err)
	defer old.ReleaseReadState()
	const fieldID = int64(106)
	suite.True(old.HasRawData(fieldID))
	narrow, err := suite.prepareReadState(local)
	suite.Require().NoError(err)
	defer narrow.ReleaseReadState()
	var fields []int64
	for _, field := range suite.collection.Schema().Fields {
		if field.FieldID != fieldID {
			fields = append(fields, field.FieldID)
		}
	}
	suite.Require().NoError(narrow.(*sealedReadState).native.Reopen(ctx, &segcore.ReopenRequest{
		LoadInfo: local.LoadInfo(), Collection: suite.collection.GetCCollection(), LoadFields: fields,
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
	old, err := suite.prepareReadState(local)
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
