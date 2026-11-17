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

package importv2

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

type DMLGroup struct {
	insertRowNums []int
}

type SourceCollectionInfo struct {
	collectionID int64
	partitionID  int64
	SegmentIDs   []int64
	insertedIDs  *schemapb.IDs
	snapshotURI  string
	// Exact timestamp range retained in the source SegmentStats.
	l1MinTs uint64
	l1MaxTs uint64
}

// SnapshotSchemaImportSuite exercises schema conversion through the public
// insert/snapshot APIs, the import scheduler, and queries of imported rows.
type SnapshotSchemaImportSuite struct {
	importSuite
}

func (s *SnapshotSchemaImportSuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableImportV3.Key, "false")
	s.importSuite.SetupSuite()
}

func TestSnapshotSchemaImportSuite(t *testing.T) {
	suite.Run(t, new(SnapshotSchemaImportSuite))
}

func (s *SnapshotSchemaImportSuite) TestSnapshotSchemaConversion() {
	for _, format := range []string{"vortex", "parquet"} {
		s.Run(format, func() {
			restore := s.Cluster.MustModifyMilvusConfig(map[string]string{
				paramtable.Get().DataNodeCfg.StorageFormat.Key: format,
			})
			defer restore()
			for _, tc := range []struct {
				name                  string
				dim                   int
				targetType            schemapb.DataType
				sourceElement         schemapb.DataType
				targetElement         schemapb.DataType
				sourceElementNullable bool
				targetElementNullable bool
				text                  string
				sourceLength          int
				targetLength          int
				sourceNullable        bool
				targetNullable        bool
				withDefault           bool
				admissionErr          string
			}{
				{name: "smaller_dimension", dim: 64, admissionErr: "vector dimension mismatch"},
				{name: "larger_dimension", dim: 256, admissionErr: "vector dimension mismatch"},
				{
					name: "float_to_bfloat16_equal_width", dim: 256, targetType: schemapb.DataType_BFloat16Vector,
					admissionErr: "vector data type mismatch",
				},
				{
					name: "array_float_to_binary", sourceElement: schemapb.DataType_FloatVector,
					targetElement: schemapb.DataType_BinaryVector, admissionErr: "vector element type mismatch",
				},
				{
					name: "array_float16_to_bfloat16", sourceElement: schemapb.DataType_Float16Vector,
					targetElement: schemapb.DataType_BFloat16Vector, admissionErr: "vector element type mismatch",
				},
				{
					name: "array_nullable_to_required", sourceElement: schemapb.DataType_FloatVector,
					targetElement: schemapb.DataType_FloatVector, sourceElementNullable: true,
					admissionErr: "vector element nullable mismatch",
				},
				{
					name: "array_required_to_nullable", sourceElement: schemapb.DataType_FloatVector,
					targetElement: schemapb.DataType_FloatVector, targetElementNullable: true,
					// Nullable elements in the target are already rejected by the Proxy import gate.
					admissionErr: "element-nullable or nested Array field group.vectors is not supported",
				},
				{name: "ascii_boundary", text: "abcdef", sourceLength: 6, sourceNullable: true, targetNullable: true},
				{name: "utf8_boundary", text: "中文", sourceLength: 6, sourceNullable: true, targetNullable: true},
				{name: "ascii_oversized", text: "abcdefg", targetLength: 6, sourceNullable: true, targetNullable: true, admissionErr: "max_length"},
				{name: "utf8_oversized", text: "中文a", targetLength: 6, sourceNullable: true, targetNullable: true, admissionErr: "max_length"},
				{name: "narrower_target_short_value", text: "abc", targetLength: 6, sourceNullable: true, targetNullable: true, admissionErr: "max_length"},
				{name: "wider_target", text: "abc", sourceLength: 6, targetLength: 64, sourceNullable: true, targetNullable: true},
				{name: "nullable_to_required_with_default", sourceNullable: true, withDefault: true, admissionErr: "nullable mismatch"},
				{name: "nullable_default", sourceNullable: true, targetNullable: true, withDefault: true},
				{name: "nullable_no_default", sourceNullable: true, targetNullable: true},
				{name: "nullable_to_required", sourceNullable: true, admissionErr: "nullable mismatch"},
				{name: "required_to_nullable", targetNullable: true, admissionErr: "nullable mismatch"},
				{name: "required_to_nullable_with_default", targetNullable: true, withDefault: true, admissionErr: "nullable mismatch"},
				{name: "both_required"},
				{name: "both_required_with_default", withDefault: true},
			} {
				s.Run(tc.name, func() {
					ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 3*time.Minute)
					defer cancel()
					create := func(schema *schemapb.CollectionSchema) {
						encoded, err := proto.Marshal(schema)
						s.Require().NoError(err)
						status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
							CollectionName: schema.GetName(), Schema: encoded, ShardsNum: 1,
						})
						s.Require().NoError(merr.CheckRPCCall(status, err))
						s.T().Cleanup(func() {
							cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
							defer cleanupCancel()
							status, err := s.Cluster.MilvusClient.DropCollection(cleanupCtx,
								&milvuspb.DropCollectionRequest{CollectionName: schema.GetName()})
							s.NoError(merr.CheckRPCCall(status, err))
						})
					}
					const sourceDim = 128
					source := &schemapb.CollectionSchema{Name: "snapshot_schema_src_" + funcutil.RandomString(8), Fields: []*schemapb.FieldSchema{
						{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
						{Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
						{Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}}},
						{Name: "value", DataType: schemapb.DataType_Int64, Nullable: tc.sourceNullable},
					}}
					if tc.sourceElement != schemapb.DataType_None {
						source.StructArrayFields = []*schemapb.StructArrayFieldSchema{{Name: "group", Fields: []*schemapb.FieldSchema{
							{
								Name: "vectors", DataType: schemapb.DataType_ArrayOfVector, ElementType: tc.sourceElement,
								ElementNullable: tc.sourceElementNullable,
								TypeParams:      []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "32"}, {Key: common.MaxCapacityKey, Value: "1000"}},
							},
						}}}
					}
					if tc.sourceLength != 0 {
						source.Fields[2].TypeParams[0].Value = fmt.Sprint(tc.sourceLength)
					}
					create(source)
					values := integration.NewInt64FieldData("value", 3)
					values.GetScalars().GetLongData().Data = []int64{7, 0, 5}
					if tc.sourceNullable {
						values.GetScalars().GetLongData().Data = []int64{0, 5}
						typeutil.SetFieldDataValidData(values, []bool{false, true, true})
					}
					text := integration.NewVarCharFieldData("text", 3, false)
					typeutil.SetFieldDataValidData(text, nil)
					text.GetScalars().GetStringData().Data = []string{"", "a", tc.text}
					fieldsData := []*schemapb.FieldData{
						integration.NewInt64FieldData("pk", 3),
						integration.NewFloatVectorFieldData("vector", 3, sourceDim), text, values,
					}
					if tc.sourceElement != schemapb.DataType_None {
						group := integration.NewStructArrayFieldData(source.StructArrayFields[0], "group", 3, 32)
						if tc.sourceElementNullable {
							for _, row := range group.GetStructArrays().GetFields()[0].GetVectors().GetVectorArray().GetData() {
								row.GetFloatVector().Data = make([]float32, 32)
								row.ValidData = []bool{true, false}
							}
						}
						fieldsData = append(fieldsData, group)
					}
					insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
						CollectionName: source.GetName(), NumRows: 3, HashKeys: integration.GenerateHashKeys(3),
						FieldsData: fieldsData,
					})
					s.Require().NoError(merr.CheckRPCCall(insert, err))
					s.Require().EqualValues(3, insert.GetInsertCnt())
					flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{source.GetName()}})
					s.Require().NoError(merr.CheckRPCCall(flush, err))
					s.Require().Eventually(func() bool {
						segments, err := s.Cluster.ShowSegments(source.GetName())
						if err != nil {
							return false
						}
						var rows int64
						for _, segment := range segments {
							if segment.GetState() == commonpb.SegmentState_Flushed && segment.GetLevel() == datapb.SegmentLevel_L1 {
								if segment.GetStorageVersion() != storage.StorageV3 || segment.GetManifestPath() == "" {
									return false
								}
								rows += segment.GetNumOfRows()
							}
						}
						return rows == 3
					}, time.Minute, 100*time.Millisecond)
					description, err := s.Cluster.MilvusClient.DescribeCollection(ctx,
						&milvuspb.DescribeCollectionRequest{CollectionName: source.GetName()})
					s.Require().NoError(merr.CheckRPCCall(description, err))
					valueField, found := lo.Find(description.GetSchema().GetFields(), func(field *schemapb.FieldSchema) bool {
						return field.GetName() == "value"
					})
					s.Require().True(found)
					valueID := fmt.Sprint(valueField.GetFieldID())
					segments, err := s.Cluster.ShowSegments(source.GetName())
					s.Require().NoError(err)
					foundValue := false
					for _, segment := range segments {
						if segment.GetState() != commonpb.SegmentState_Flushed || segment.GetLevel() != datapb.SegmentLevel_L1 {
							continue
						}
						groups, err := packed.ReadManifestColumnGroups(segment.GetManifestPath(), packed.CreateStorageConfig())
						s.Require().NoError(err)
						for _, group := range groups {
							if lo.Contains(group.Columns, valueID) {
								foundValue = true
								// Prove the nullable column actually exercises this format.
								s.Require().Equal(format, group.Format)
							}
						}
					}
					s.Require().True(foundValue)
					snapshotName := "schema_" + funcutil.RandomString(8)
					status, err := s.Cluster.MilvusClient.CreateSnapshot(ctx, &milvuspb.CreateSnapshotRequest{Name: snapshotName, CollectionName: source.GetName()})
					s.Require().NoError(merr.CheckRPCCall(status, err))
					s.T().Cleanup(func() {
						cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
						defer cleanupCancel()
						status, err := s.Cluster.MilvusClient.DropSnapshot(cleanupCtx,
							&milvuspb.DropSnapshotRequest{Name: snapshotName, CollectionName: source.GetName()})
						s.NoError(merr.CheckRPCCall(status, err))
					})
					snapshot, err := s.Cluster.MilvusClient.DescribeSnapshot(ctx, &milvuspb.DescribeSnapshotRequest{Name: snapshotName, CollectionName: source.GetName()})
					s.Require().NoError(merr.CheckRPCCall(snapshot, err))
					s.Require().NotEmpty(snapshot.GetS3Location())
					target := proto.Clone(source).(*schemapb.CollectionSchema)
					target.Name = "snapshot_schema_dst_" + funcutil.RandomString(8)
					if tc.dim != 0 {
						target.Fields[1].TypeParams[0].Value = fmt.Sprint(tc.dim)
					}
					if tc.targetType != schemapb.DataType_None {
						target.Fields[1].DataType = tc.targetType
					}
					if tc.targetElement != schemapb.DataType_None {
						target.StructArrayFields[0].Fields[0].ElementType = tc.targetElement
						target.StructArrayFields[0].Fields[0].ElementNullable = tc.targetElementNullable
					}
					if tc.targetLength != 0 {
						target.Fields[2].TypeParams[0].Value = fmt.Sprint(tc.targetLength)
					}
					target.Fields[3].Nullable = tc.targetNullable
					if tc.withDefault {
						target.Fields[3].DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}}
					}
					create(target)
					response, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
						CollectionName: target.GetName(), Files: []*internalpb.ImportFile{{Paths: []string{snapshot.GetS3Location()}}},
						Options: []*commonpb.KeyValuePair{{Key: "backup", Value: "true"}, {Key: importutilv2.SourceType, Value: importutilv2.SourceTypeSnapshot}},
					})
					if tc.admissionErr != "" {
						s.Require().ErrorContains(merr.CheckRPCCall(response, err), tc.admissionErr)
						s.Require().Empty(response.GetJobID(), "schema rejection must precede job creation")
						// A rejected schema must leave the service available.
						response, err := s.Cluster.MilvusClient.DescribeCollection(ctx,
							&milvuspb.DescribeCollectionRequest{CollectionName: target.GetName()})
						s.Require().NoError(merr.CheckRPCCall(response, err))
						return
					}
					s.Require().NoError(merr.CheckRPCCall(response, err))
					s.Require().NotEmpty(response.GetJobID())
					// Bound polling here rather than using WaitForImportDone, whose
					// loop does not stop when the context expires.
					var progress *internalpb.GetImportProgressResponse
					s.Require().Eventually(func() bool {
						var err error
						progress, err = s.Cluster.ProxyClient.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{JobID: response.GetJobID()})
						return err == nil && merr.Ok(progress.GetStatus()) &&
							(progress.GetState() == internalpb.ImportJobState_Completed || progress.GetState() == internalpb.ImportJobState_Failed)
					}, time.Minute, 100*time.Millisecond)
					s.Require().Equal(internalpb.ImportJobState_Completed, progress.GetState(), progress.GetReason())
					status, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
						CollectionName: target.GetName(), FieldName: "vector", IndexName: "vector_idx",
						ExtraParams: integration.ConstructIndexParam(sourceDim, integration.IndexFaissIDMap, metric.L2),
					})
					s.Require().NoError(merr.CheckRPCCall(status, err))
					s.WaitForIndexBuilt(ctx, target.GetName(), "vector")
					status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: target.GetName()})
					s.Require().NoError(merr.CheckRPCCall(status, err))
					s.WaitForLoad(ctx, target.GetName())
					for pk := int64(0); pk < 3; pk++ {
						result, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
							CollectionName: target.GetName(), Expr: fmt.Sprintf("pk == %d", pk),
							OutputFields: []string{"pk", "text", "value"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
						})
						s.Require().NoError(merr.CheckRPCCall(result, err))
						fields := lo.KeyBy(result.GetFieldsData(), func(field *schemapb.FieldData) string { return field.GetFieldName() })
						s.Require().Equal([]int64{pk}, fields["pk"].GetScalars().GetLongData().GetData())
						s.Equal([]string{"", "a", tc.text}[pk], fields["text"].GetScalars().GetStringData().GetData()[0])
						if pk == 0 && tc.sourceNullable && !tc.withDefault {
							s.Require().Equal([]bool{false}, typeutil.GetFieldDataValidData(fields["value"]))
						} else {
							want := []int64{7, 0, 5}[pk]
							if pk == 0 && tc.sourceNullable {
								want = 42
							}
							if valid := typeutil.GetFieldDataValidData(fields["value"]); len(valid) != 0 {
								s.Equal([]bool{true}, valid)
							}
							s.Equal([]int64{want}, fields["value"].GetScalars().GetLongData().GetData())
						}
					}
				})
			}
		})
	}
}

func (s *importTestBase) PrepareSourceCollection(dim int, dmlGroup *DMLGroup) *SourceCollectionInfo {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	c := s.Cluster

	collectionName := "TestBinlogImport_A_" + funcutil.RandomString(8)

	schema := integration.ConstructSchemaOfVecDataTypeWithStruct(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(merr.CheckRPCCall(createCollectionStatus, err))

	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		CollectionNames: []string{collectionName},
	})
	s.NoError(merr.CheckRPCCall(showCollectionsResp, err))
	mlog.Info(context.TODO(), "ShowCollections result", mlog.Any("showCollectionsResp", showCollectionsResp))

	showPartitionsResp, err := c.MilvusClient.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(showPartitionsResp, err))
	mlog.Info(context.TODO(), "ShowPartitions result", mlog.Any("showPartitionsResp", showPartitionsResp))

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(merr.CheckRPCCall(createIndexStatus, err))
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	name := typeutil.ConcatStructFieldName(integration.StructArrayField, integration.StructSubFloatVecField)
	createIndexResult, err := c.MilvusClient.CreateIndex(context.TODO(), &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      name,
		IndexName:      "array_of_vector_index",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexHNSW, metric.MaxSim),
	})
	s.NoError(err)
	s.Require().Equal(createIndexResult.GetErrorCode(), commonpb.ErrorCode_Success)
	s.WaitForIndexBuilt(context.TODO(), collectionName, name)

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoad(ctx, collectionName)

	var (
		totalInsertRowNum = 0
		totalInsertedIDs  = &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: make([]int64, 0),
				},
			},
		}
	)

	for i := range dmlGroup.insertRowNums {
		insRow := dmlGroup.insertRowNums[i]
		totalInsertRowNum += insRow

		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, insRow, dim)
		structColumn := integration.NewStructArrayFieldData(schema.StructArrayFields[0], integration.StructArrayField, insRow, dim)
		hashKeys := integration.GenerateHashKeys(insRow)
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{fVecColumn, structColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(insRow),
		})
		s.NoError(merr.CheckRPCCall(insertResult, err))
		insertedIDs := insertResult.GetIDs()
		totalInsertedIDs.IdField.(*schemapb.IDs_IntId).IntId.Data = append(
			totalInsertedIDs.IdField.(*schemapb.IDs_IntId).IntId.Data, insertedIDs.IdField.(*schemapb.IDs_IntId).IntId.Data...)

		// flush
		flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
			CollectionNames: []string{collectionName},
		})
		s.NoError(merr.CheckRPCCall(flushResp, err))
		// The per-collection flush segment map is filled asynchronously by the
		// flusher and can still be empty when the RPC returns. Discover the
		// sealed L1 segments from meta instead of trusting the response body.
		ids := flushResp.GetFlushCollSegIDs()[collectionName].GetData()
		if len(ids) == 0 {
			s.Require().Eventually(func() bool {
				segments, err := c.ShowSegments(collectionName)
				if err != nil {
					return false
				}
				ids = ids[:0]
				for _, segment := range segments {
					if segment.GetState() == commonpb.SegmentState_Flushed && segment.GetLevel() == datapb.SegmentLevel_L1 {
						ids = append(ids, segment.GetID())
					}
				}
				return len(ids) > 0
			}, 60*time.Second, 200*time.Millisecond)
		}
		s.Require().NotEmpty(ids)
		if flushTs, ok := flushResp.GetCollFlushTs()[collectionName]; ok && flushTs != 0 {
			s.WaitForFlush(ctx, ids, flushTs, "", collectionName)
		}
		segments, err := c.ShowSegments(collectionName)
		s.NoError(err)
		s.NotEmpty(segments)
		for _, segment := range segments {
			mlog.Info(context.TODO(), "ShowSegments result", mlog.String("segment", segment.String()))
		}
	}

	// check segments
	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	segments = lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
		return segment.GetState() == commonpb.SegmentState_Flushed && segment.GetLevel() == datapb.SegmentLevel_L1
	})
	s.Require().NotEmpty(segments)
	for _, segment := range segments {
		s.Require().EqualValues(storage.StorageV3, segment.GetStorageVersion())
		s.Require().NotEmpty(segment.GetManifestPath())
	}

	// V3 persists exact row timestamp bounds in SegmentStats; legacy
	// FieldBinlog arrays need not be present in the catalog.
	var l1MinTs uint64 = math.MaxUint64
	var l1MaxTs uint64 = 0
	for _, segment := range segments {
		stats := segment.GetStats()
		s.Require().NotNil(stats)
		s.Require().Positive(stats.GetTimestampFrom())
		s.Require().GreaterOrEqual(stats.GetTimestampTo(), stats.GetTimestampFrom())
		l1MinTs = min(l1MinTs, stats.GetTimestampFrom())
		l1MaxTs = max(l1MaxTs, stats.GetTimestampTo())
	}

	s.Require().NotEqual(uint64(math.MaxUint64), l1MinTs)
	s.Require().Positive(l1MaxTs)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.MilvusClient.Search(ctx, searchReq)

	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	expectResult := nq * topk
	if expectResult > totalInsertRowNum {
		expectResult = totalInsertRowNum
	}
	s.Equal(expectResult, len(searchResult.GetResults().GetScores()))

	// query
	expr = fmt.Sprintf("%s >= 0", integration.Int64Field)
	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{"count(*)"},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count := int(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	s.Equal(totalInsertRowNum, count)

	// query 2
	expr = fmt.Sprintf("%s < %d", integration.Int64Field, totalInsertedIDs.GetIntId().GetData()[10])
	queryResult, err = c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count = len(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	expectCount := 10
	s.Equal(expectCount, count)

	// get collectionID and partitionID
	collectionID := showCollectionsResp.GetCollectionIds()[0]
	partitionID := showPartitionsResp.GetPartitionIDs()[0]

	// StorageV3 backup import consumes snapshot metadata containing exact
	// Manifest versions; legacy insert_log prefixes are not a V3 input format.
	snapshotName := "binlog_source_" + funcutil.RandomString(8)
	status, err := c.MilvusClient.CreateSnapshot(ctx, &milvuspb.CreateSnapshotRequest{
		Name: snapshotName, CollectionName: collectionName,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.T().Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		status, err := c.MilvusClient.DropSnapshot(cleanupCtx, &milvuspb.DropSnapshotRequest{
			Name: snapshotName, CollectionName: collectionName,
		})
		s.NoError(merr.CheckRPCCall(status, err))
	})
	snapshot, err := c.MilvusClient.DescribeSnapshot(ctx, &milvuspb.DescribeSnapshotRequest{
		Name: snapshotName, CollectionName: collectionName,
	})
	s.Require().NoError(merr.CheckRPCCall(snapshot, err))
	s.Require().NotEmpty(snapshot.GetS3Location())

	return &SourceCollectionInfo{
		collectionID: collectionID,
		partitionID:  partitionID,
		SegmentIDs: lo.Map(segments, func(segment *datapb.SegmentInfo, _ int) int64 {
			return segment.GetID()
		}),
		insertedIDs: totalInsertedIDs,
		snapshotURI: snapshot.GetS3Location(),
		l1MinTs:     l1MinTs,
		l1MaxTs:     l1MaxTs,
	}
}

func (s *importTestBase) runBinlogTest(dmlGroup *DMLGroup) {
	const dim = 128

	sourceCollectionInfo := s.PrepareSourceCollection(dim, dmlGroup)
	collectionID := sourceCollectionInfo.collectionID
	partitionID := sourceCollectionInfo.partitionID
	segmentIDs := sourceCollectionInfo.SegmentIDs
	insertedIDs := sourceCollectionInfo.insertedIDs

	mlog.Info(context.TODO(), "prepare source collection done",
		mlog.FieldCollectionID(collectionID),
		mlog.FieldPartitionID(partitionID),
		mlog.Int64s("segments", segmentIDs))

	c := s.Cluster
	ctx := c.GetContext()

	totalInsertRowNum := lo.SumBy(dmlGroup.insertRowNums, func(num int) int {
		return num
	})

	collectionName := "TestBinlogImport_B_" + funcutil.RandomString(8)

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(merr.CheckRPCCall(createCollectionStatus, err))

	describeCollectionResp, err := c.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(describeCollectionResp, err))
	newCollectionID := describeCollectionResp.GetCollectionID()

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(merr.CheckRPCCall(createIndexStatus, err))

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// Import the V3 snapshot while retaining source primary keys and timestamps.
	files := []*internalpb.ImportFile{{Paths: []string{sourceCollectionInfo.snapshotURI}}}

	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		Files:          files,
		Options: []*commonpb.KeyValuePair{
			{Key: "backup", Value: "true"},
			{Key: importutilv2.SourceType, Value: importutilv2.SourceTypeSnapshot},
		},
	})
	s.NoError(merr.CheckRPCCall(importResp, err))
	mlog.Info(context.TODO(), "Import result", mlog.Any("importResp", importResp))

	jobID := importResp.GetJobID()
	err = WaitForImportDone(ctx, c, jobID)
	s.NoError(err)

	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	segments = lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
		return segment.GetCollectionID() == newCollectionID
	})
	mlog.Info(context.TODO(), "Show segments", mlog.Any("segments", segments))
	s.Equal(2, len(segments))
	segment, ok := lo.Find(segments, func(segment *datapb.SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Flushed
	})
	s.True(ok)
	s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
	s.EqualValues(storage.StorageV3, segment.GetStorageVersion())
	s.NotEmpty(segment.GetManifestPath())
	s.Empty(segment.GetDeltalogs())

	// Verify L1 segment positions match actual timestamps from source collection
	s.Equal(sourceCollectionInfo.l1MinTs, segment.GetStartPosition().GetTimestamp(),
		"L1 segment StartPosition should match actual min timestamp from source binlogs")
	s.Equal(sourceCollectionInfo.l1MaxTs, segment.GetDmlPosition().GetTimestamp(),
		"L1 segment DmlPosition should match actual max timestamp from source binlogs")
	mlog.Info(context.TODO(), "L1 segment position verification passed")

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoad(ctx, collectionName)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Eventually

	searchResult, err := c.MilvusClient.Search(ctx, searchReq)

	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	expectResult := nq * topk
	if expectResult > totalInsertRowNum {
		expectResult = totalInsertRowNum
	}
	s.Equal(expectResult, len(searchResult.GetResults().GetScores()))
	// check ids from collectionA, because during binlog import, even if the primary key's autoID is set to true,
	// the primary key from the binlog should be used instead of being reassigned.
	insertedIDsMap := lo.SliceToMap(insertedIDs.GetIntId().GetData(), func(id int64) (int64, struct{}) {
		return id, struct{}{}
	})
	for _, id := range searchResult.GetResults().GetIds().GetIntId().GetData() {
		_, ok := insertedIDsMap[id]
		s.True(ok)
	}

	// query
	expr = fmt.Sprintf("%s >= 0", integration.Int64Field)
	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             expr,
		OutputFields:     []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Eventually,
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count := int(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	s.Equal(totalInsertRowNum, count)

	// query 2
	expr = fmt.Sprintf("%s < %d", integration.Int64Field, insertedIDs.GetIntId().GetData()[10])
	queryResult, err = c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             expr,
		OutputFields:     []string{},
		ConsistencyLevel: commonpb.ConsistencyLevel_Eventually,
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count = len(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	expectCount := 10
	s.Equal(expectCount, count)
}

func (s *BulkInsertSuite) TestInvalidInput() {
	const dim = 128
	c := s.Cluster
	ctx := c.GetContext()

	collectionName := "TestBinlogImport_InvalidInput_" + funcutil.RandomString(8)
	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(merr.CheckRPCCall(createCollectionStatus, err))

	describeCollectionResp, err := c.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(describeCollectionResp, err))

	s.False(paramtable.Get().DataCoordCfg.EnableL0Import.GetAsBool())
	l0Resp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          []*internalpb.ImportFile{{Paths: []string{"delta_log/source"}}},
		Options:        []*commonpb.KeyValuePair{{Key: "l0_import", Value: "true"}},
	})
	s.NoError(err)
	s.ErrorContains(merr.CheckRPCCall(l0Resp, err), "l0 import is disabled")

	// Backup paths are an insert-log prefix and an optional delta-log prefix.
	// A third path is invalid input and must not trigger service retries.
	files := []*internalpb.ImportFile{
		{
			Paths: []string{"invalid-path", "invalid-path", "invalid-path"},
		},
	}
	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		Files:          files,
		Options: []*commonpb.KeyValuePair{
			{Key: "backup", Value: "true"},
		},
	})
	s.Require().NoError(err)
	err = merr.CheckRPCCall(importResp, err)
	s.ErrorContains(err, "too many input paths for binlog import")
	s.ErrorIs(err, merr.ErrImportFailed)
	s.Equal(merr.InputError, merr.GetErrorType(err))
	s.False(importResp.GetStatus().GetRetriable())
	mlog.Info(context.TODO(), "Import result", mlog.Any("importResp", importResp))
}

// Independent L0 import is disabled; retain ordinary binlog restore coverage.
func (s *BulkInsertSuite) TestBinlogImport_NoDelete() {
	s.runBinlogTest(&DMLGroup{insertRowNums: []int{500, 500, 500}})
}
