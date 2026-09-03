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

package cmek

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

const (
	rawDataRows = 512
	rawDataDim  = 8
)

type rawDataCampaign struct {
	name       string
	schema     *schemapb.CollectionSchema
	fields     []*schemapb.FieldData
	loadFields []string
	index      bool
	search     bool
}

type RawDataV2Suite struct {
	integration.MiniClusterSuite
	dbName string
	ezID   int64
}

func (s *RawDataV2Suite) SetupSuite() {
	s.WithOptions(integration.WithoutResetDeploymentWhenTestTearDown())
	s.WithMilvusConfig("common.storage.useLoonFFI", "false")
	s.WithMilvusConfig("dataNode.storage.format", "parquet")
	s.WithMilvusConfig("common.storage.enableGrowingSourceFlush", "false")
	s.WithMilvusConfig("indexCoord.segment.minSegmentNumRowsToEnableIndex", "1024")
	s.WithMilvusConfig("queryNode.segcore.interimIndex.enableIndex", "false")
	s.WithMilvusConfig("queryNode.segcore.tieredStorage.warmup.scalarField", "sync")
	s.WithMilvusConfig("queryNode.segcore.tieredStorage.warmup.vectorField", "sync")
	s.MiniClusterSuite.SetupSuite()

	ctx := s.Cluster.GetContext()
	s.dbName = "cmek_raw_v2_" + funcutil.GenRandomStr()
	status, err := s.Cluster.MilvusClient.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: s.dbName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.EncryptionEnabledKey, Value: "true"},
			{Key: common.EncryptionRootKeyKey, Value: "fixture-root-key"},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	describe, err := s.Cluster.MilvusClient.DescribeDatabase(ctx, &milvuspb.DescribeDatabaseRequest{DbName: s.dbName})
	s.Require().NoError(merr.CheckRPCCall(describe, err))
	s.ezID = describe.GetDbID()
	s.Require().Positive(s.ezID)
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(describe.GetProperties(), common.EncryptionEzIDKey))
}

func (s *RawDataV2Suite) TearDownSuite() {
	if s.Cluster != nil && s.dbName != "" {
		status, err := s.Cluster.MilvusClient.DropDatabase(context.Background(), &milvuspb.DropDatabaseRequest{DbName: s.dbName})
		s.NoError(merr.CheckRPCCall(status, err))
	}
	s.MiniClusterSuite.TearDownSuite()
}

func (s *RawDataV2Suite) TestRawScalar() {
	s.runRawDataCampaign(newRawScalarCampaign())
}

func (s *RawDataV2Suite) TestRawVector() {
	s.runRawDataCampaign(newRawVectorCampaign())
}

func (s *RawDataV2Suite) TestStructArray() {
	s.runRawDataCampaign(newStructArrayCampaign())
}

func TestRawDataV2Suite(t *testing.T) {
	suite.Run(t, new(RawDataV2Suite))
}

func (s *RawDataV2Suite) runRawDataCampaign(c rawDataCampaign) {
	ctx := s.Cluster.GetContext()
	collectionName := "cmek_raw_" + c.name + "_" + funcutil.GenRandomStr()
	c.schema.Name = collectionName
	loadFieldIDs := requestedFieldIDs(c.schema, c.loadFields)
	marshaled, err := proto.Marshal(c.schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName: s.dbName, CollectionName: collectionName, Schema: marshaled, ShardsNum: common.DefaultShardsNum,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	defer s.cleanupRawCollection(collectionName)

	describe, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		DbName: s.dbName, CollectionName: collectionName,
	})
	s.Require().NoError(merr.CheckRPCCall(describe, err))
	collectionID := describe.GetCollectionID()
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(describe.GetProperties(), common.EncryptionEzIDKey))
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(describe.GetSchema().GetProperties(), common.EncryptionEzIDKey))
	if c.index {
		// Complete logical-index broadcasts before insert and flush start the
		// segment lifecycle. A post-flush CreateIndex was observed remaining
		// pending indefinitely while those workflows overlapped. The post-flush
		// metadata assertion below still proves that these small segments have no
		// physical vector index files.
		s.createRawVectorIndexes(ctx, collectionName, c.schema)
	}

	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName: s.dbName, CollectionName: collectionName, FieldsData: c.fields,
		HashKeys: integration.GenerateHashKeys(rawDataRows), NumRows: rawDataRows,
	})
	s.Require().NoError(merr.CheckRPCCall(insert, err))
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName: s.dbName, CollectionNames: []string{collectionName},
	})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	flushedIDs := flush.GetCollSegIDs()[collectionName].GetData()
	s.Require().NotEmpty(flushedIDs)
	s.WaitForFlush(ctx, flushedIDs, flush.GetCollFlushTs()[collectionName], s.dbName, collectionName)

	segments := s.rawSealedSegments(collectionName)
	s.assertFlushProducedCurrentSegment(flushedIDs, segments)
	s.inspectRawObjects(ctx, segments, collectionID)
	if c.index {
		s.assertNoPhysicalVectorIndex(ctx, segments, c.schema)
	}

	// Refresh authoritative metadata immediately before release and inspect the
	// complete current set, including normal compaction replacements.
	segments = s.rawSealedSegments(collectionName)
	s.inspectRawObjects(ctx, segments, collectionID)
	if c.index {
		s.assertNoPhysicalVectorIndex(ctx, segments, c.schema)
	}
	release, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName: s.dbName, CollectionName: collectionName,
	})
	s.Require().NoError(merr.CheckRPCCall(release, err))
	s.CheckCollectionCacheReleased(collectionID)

	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName: s.dbName, CollectionName: collectionName, ReplicaNumber: 1, LoadFields: c.loadFields,
	})
	s.Require().NoError(merr.CheckRPCCall(load, err))
	s.WaitForLoadWithDB(ctx, s.dbName, collectionName)
	s.assertLoadedFields(ctx, collectionID, loadFieldIDs)
	s.assertRawLoadedSegments(ctx, collectionID, segments)
	s.assertRawDataOracle(ctx, collectionName, c.fields, c.loadFields)
	if c.search {
		s.assertExactFloatSearch(ctx, collectionName, "float_vector", firstFloatVector(c.fields, "float_vector", rawDataDim), rawDataRows)
	}
}

func (s *RawDataV2Suite) inspectRawObjects(ctx context.Context, segments []*datapb.SegmentInfo, collectionID int64) {
	objects, err := inspector.LocateRawDataV2(s.Cluster.RootPath(), segments)
	s.Require().NoError(err)
	s.Require().NotEmpty(objects)
	reader := inspector.ObjectReader{ChunkManager: s.Cluster.ChunkManager}
	for _, object := range objects {
		raw, readErr := reader.Read(ctx, inspector.Object{Path: object.Path})
		s.Require().NoError(readErr, "collection=%d segment=%d field=%d path=%s storage_version=%d",
			object.CollectionID, object.SegmentID, object.FieldID, object.Path, object.StorageVersion)
		s.Require().NoError(inspector.InspectRawDataV2(raw, s.ezID, collectionID),
			"collection=%d segment=%d field=%d path=%s storage_version=%d",
			object.CollectionID, object.SegmentID, object.FieldID, object.Path, object.StorageVersion)
	}
}

func (s *RawDataV2Suite) createRawVectorIndexes(ctx context.Context, collection string, schema *schemapb.CollectionSchema) {
	create := func(fieldName string, field *schemapb.FieldSchema) {
		if !typeutil.IsVectorType(field.GetDataType()) {
			return
		}
		indexType := integration.IndexFaissIDMap
		metricType := metric.L2
		vectorType := field.GetDataType()
		if field.GetDataType() == schemapb.DataType_ArrayOfVector {
			vectorType = field.GetElementType()
			indexType = integration.IndexHNSW
			metricType = metric.MaxSim
		}
		switch vectorType {
		case schemapb.DataType_BinaryVector:
			if field.GetDataType() == schemapb.DataType_ArrayOfVector {
				metricType = metric.MaxSimHamming
			} else {
				indexType = integration.IndexFaissBinIDMap
				metricType = metric.JACCARD
			}
		case schemapb.DataType_SparseFloatVector:
			indexType = integration.IndexSparseInvertedIndex
			metricType = metric.IP
		case schemapb.DataType_Int8Vector:
			indexType = integration.IndexHNSW
		}
		status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			DbName: s.dbName, CollectionName: collection, FieldName: fieldName, IndexName: "raw_" + field.GetName(),
			ExtraParams: integration.ConstructIndexParam(rawDataDim, indexType, metricType),
		})
		s.Require().NoError(merr.CheckRPCCall(status, err), field.GetName())
		s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, fieldName)
	}
	for _, field := range schema.GetFields() {
		create(field.GetName(), field)
	}
	for _, structField := range schema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			create(typeutil.ConcatStructFieldName(structField.GetName(), field.GetName()), field)
		}
	}
}

func (s *RawDataV2Suite) assertNoPhysicalVectorIndex(ctx context.Context, segments []*datapb.SegmentInfo, schema *schemapb.CollectionSchema) {
	ids := make([]int64, 0, len(segments))
	for _, segment := range segments {
		ids = append(ids, segment.GetID())
	}
	response, err := s.Cluster.MixCoordClient.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{
		CollectionID: segments[0].GetCollectionID(), SegmentIDs: ids,
	})
	s.Require().NoError(merr.CheckRPCCall(response, err))
	vectorIDs := make(map[int64]struct{})
	for _, field := range schema.GetFields() {
		if typeutil.IsVectorType(field.GetDataType()) {
			vectorIDs[field.GetFieldID()] = struct{}{}
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			if typeutil.IsVectorType(field.GetDataType()) {
				vectorIDs[field.GetFieldID()] = struct{}{}
			}
		}
	}
	for _, segment := range segments {
		seen := make(map[int64]int, len(vectorIDs))
		for _, info := range response.GetSegmentInfo()[segment.GetID()].GetIndexInfos() {
			if _, target := vectorIDs[info.GetFieldID()]; target {
				seen[info.GetFieldID()]++
				s.Require().Empty(info.GetIndexFilePaths(), "raw vector segment %d field %d unexpectedly has physical index files", segment.GetID(), info.GetFieldID())
			}
		}
		for fieldID := range vectorIDs {
			s.Require().Equal(1, seen[fieldID], "raw vector segment %d must report exactly one logical index for field %d", segment.GetID(), fieldID)
		}
	}
}

func (s *RawDataV2Suite) assertLoadedFields(ctx context.Context, collectionID int64, expected []int64) {
	response, err := s.Cluster.MixCoordClient.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{CollectionIDs: []int64{collectionID}})
	s.Require().NoError(merr.CheckRPCCall(response, err))
	s.Require().Equal([]int64{collectionID}, response.GetCollectionIDs())
	s.Require().Len(response.GetLoadFields(), 1)
	actual := append([]int64(nil), response.GetLoadFields()[0].GetData()...)
	sort.Slice(actual, func(i, j int) bool { return actual[i] < actual[j] })
	sort.Slice(expected, func(i, j int) bool { return expected[i] < expected[j] })
	s.Require().Equal(expected, actual)
}

func (s *RawDataV2Suite) assertRawLoadedSegments(ctx context.Context, collectionID int64, expected []*datapb.SegmentInfo) {
	expectedIDs := make(map[int64]struct{}, len(expected))
	for _, segment := range expected {
		expectedIDs[segment.GetID()] = struct{}{}
	}
	s.Require().Eventually(func() bool {
		seen := make(map[int64]int, len(expectedIDs))
		for _, process := range s.Cluster.GetAllQueryNodes() {
			client := process.MustGetClient(ctx)
			request, err := metricsinfo.ConstructGetMetricsRequest(map[string]interface{}{
				metricsinfo.MetricTypeKey: metricsinfo.SegmentKey, metricsinfo.MetricRequestParamCollectionIDKey: collectionID,
			})
			if err != nil {
				return false
			}
			response, err := client.GetMetrics(ctx, request)
			if err = merr.CheckRPCCall(response, err); err != nil {
				return false
			}
			var segments []*metricsinfo.Segment
			if err := json.Unmarshal([]byte(response.GetResponse()), &segments); err != nil {
				return false
			}
			for _, segment := range segments {
				if segment.CollectionID != collectionID || segment.State != "Sealed" {
					return false
				}
				if _, ok := expectedIDs[segment.SegmentID]; !ok {
					return false
				}
				seen[segment.SegmentID]++
			}
		}
		for segmentID := range expectedIDs {
			if seen[segmentID] != 1 {
				return false
			}
		}
		return true
	}, 3*time.Minute, 500*time.Millisecond)
}

func (s *RawDataV2Suite) assertRawDataOracle(ctx context.Context, collection string, inserted []*schemapb.FieldData, loadFields []string) {
	count, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName: s.dbName, CollectionName: collection, Expr: "", OutputFields: []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(count, err))
	s.Require().Equal(int64(rawDataRows), count.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])

	query, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName: s.dbName, CollectionName: collection,
		Expr: fmt.Sprintf("%s in [0, %d]", fixturePrimaryKey, rawDataRows-1), OutputFields: loadFields,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(query, err))
	wantedNames := make(map[string]struct{}, len(loadFields))
	for _, name := range loadFields {
		wantedNames[name] = struct{}{}
	}
	selected := make([]*schemapb.FieldData, 0, len(loadFields))
	for _, field := range inserted {
		if _, ok := wantedNames[field.GetFieldName()]; ok {
			selected = append(selected, field)
		}
	}
	expected := typeutil.PrepareResultFieldData(selected, 2)
	for i := range selected {
		typeutil.AppendFieldDataByColumn(expected[i], selected[i], []int64{0, rawDataRows - 1})
	}
	expectedByName := fieldDataByName(expected)
	for _, actual := range query.GetFieldsData() {
		want, ok := expectedByName[actual.GetFieldName()]
		s.Require().True(ok, "query returned unexpected field %s", actual.GetFieldName())
		actualCopy := proto.Clone(actual).(*schemapb.FieldData)
		wantCopy := proto.Clone(want).(*schemapb.FieldData)
		actualCopy.FieldId, wantCopy.FieldId = 0, 0
		switch actual.GetType() {
		case schemapb.DataType_JSON:
			actualRows := actual.GetScalars().GetJsonData().GetData()
			wantRows := want.GetScalars().GetJsonData().GetData()
			s.Require().Len(actualRows, len(wantRows), "field %s row count", actual.GetFieldName())
			for i := range wantRows {
				s.Require().JSONEq(string(wantRows[i]), string(actualRows[i]), "field %s row %d differs after cold load", actual.GetFieldName(), i)
			}
		case schemapb.DataType_SparseFloatVector:
			actualRows := actual.GetVectors().GetSparseFloatVector().GetContents()
			wantRows := want.GetVectors().GetSparseFloatVector().GetContents()
			s.Require().Len(actualRows, len(wantRows), "field %s row count", actual.GetFieldName())
			for i := range wantRows {
				s.Require().Equal(typeutil.SparseFloatBytesToMap(wantRows[i]), typeutil.SparseFloatBytesToMap(actualRows[i]),
					"field %s row %d differs after cold load", actual.GetFieldName(), i)
			}
		case schemapb.DataType_ArrayOfStruct:
			actualFields := actualCopy.GetStructArrays().GetFields()
			wantFields := wantCopy.GetStructArrays().GetFields()
			sort.Slice(actualFields, func(i, j int) bool { return actualFields[i].GetFieldId() < actualFields[j].GetFieldId() })
			sort.Slice(wantFields, func(i, j int) bool { return wantFields[i].GetFieldId() < wantFields[j].GetFieldId() })
			s.Require().True(proto.Equal(wantCopy, actualCopy), "field %s differs after cold load", actual.GetFieldName())
		default:
			s.Require().True(proto.Equal(wantCopy, actualCopy), "field %s differs after cold load", actual.GetFieldName())
		}
		delete(expectedByName, actual.GetFieldName())
	}
	s.Require().Empty(expectedByName)
}

func (s *RawDataV2Suite) assertExactFloatSearch(ctx context.Context, collection, field string, vector []float32, ef int) {
	request := integration.ConstructSearchRequest(s.dbName, collection, "", field, schemapb.DataType_FloatVector,
		[]string{fixturePrimaryKey}, metric.L2, map[string]any{"ef": ef}, 1, len(vector), 1, -1)
	value := make([]byte, len(vector)*4)
	for i, item := range vector {
		binary.LittleEndian.PutUint32(value[i*4:], math.Float32bits(item))
	}
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{value},
	}}})
	s.Require().NoError(err)
	request.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder}
	result, err := s.Cluster.MilvusClient.Search(ctx, request)
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.Require().Equal([]int64{0}, result.GetResults().GetIds().GetIntId().GetData())
	s.Require().Len(result.GetResults().GetScores(), 1)
	s.Require().InDelta(0, result.GetResults().GetScores()[0], 1e-6)
}

func (s *RawDataV2Suite) rawSealedSegments(collection string) []*datapb.SegmentInfo {
	var segments []*datapb.SegmentInfo
	s.Require().Eventually(func() bool {
		current, err := s.Cluster.ShowSegmentsWithDB(s.dbName, collection)
		if err != nil {
			return false
		}
		segments = segments[:0]
		for _, segment := range current {
			if (segment.GetState() == commonpb.SegmentState_Sealed || segment.GetState() == commonpb.SegmentState_Flushed) &&
				segment.GetNumOfRows() > 0 && !segment.GetCompacted() && !segment.GetIsInvisible() {
				segments = append(segments, segment)
			}
		}
		return len(segments) > 0
	}, 2*time.Minute, 500*time.Millisecond)
	return segments
}

func (s *RawDataV2Suite) assertFlushProducedCurrentSegment(flushed []int64, current []*datapb.SegmentInfo) {
	flushedIDs := make(map[int64]struct{}, len(flushed))
	for _, id := range flushed {
		flushedIDs[id] = struct{}{}
	}
	for _, segment := range current {
		if _, ok := flushedIDs[segment.GetID()]; ok {
			return
		}
		for _, sourceID := range segment.GetCompactionFrom() {
			if _, ok := flushedIDs[sourceID]; ok {
				return
			}
		}
	}
	s.FailNow("no current sealed segment came from this flush", "flush=%v", flushed)
}

func (s *RawDataV2Suite) cleanupRawCollection(collection string) {
	ctx := context.Background()
	_, _ = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{DbName: s.dbName, CollectionName: collection})
	status, err := s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.NoError(merr.CheckRPCCall(status, err))
}

func newRawScalarCampaign() rawDataCampaign {
	fields := []*schemapb.FieldSchema{{Name: fixturePrimaryKey, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}
	data := []*schemapb.FieldData{testutils.NewInt64FieldData(fixturePrimaryKey, rawDataRows)}
	loadFields := []string{fixturePrimaryKey}
	scalarTypes := []struct {
		name   string
		typeID schemapb.DataType
	}{
		{"bool_value", schemapb.DataType_Bool},
		{"int8_value", schemapb.DataType_Int8},
		{"int16_value", schemapb.DataType_Int16},
		{"int32_value", schemapb.DataType_Int32},
		{"int64_value", schemapb.DataType_Int64},
		{"float_value", schemapb.DataType_Float},
		{"double_value", schemapb.DataType_Double},
		{"varchar_value", schemapb.DataType_VarChar},
		{"geometry_value", schemapb.DataType_Geometry},
	}
	for _, item := range scalarTypes {
		field := &schemapb.FieldSchema{Name: item.name, DataType: item.typeID}
		if item.typeID == schemapb.DataType_VarChar {
			field.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}}
		}
		fields = append(fields, field)
		fieldData := testutils.GenerateScalarFieldData(item.typeID, item.name, rawDataRows)
		if item.typeID == schemapb.DataType_Geometry {
			values := make([]string, rawDataRows)
			for i := range values {
				values[i] = fmt.Sprintf("POINT (%d %d)", i%180, i%90)
			}
			fieldData = &schemapb.FieldData{
				Type:      schemapb.DataType_Geometry,
				FieldName: item.name,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: values}},
				}},
			}
		}
		if item.typeID == schemapb.DataType_Int8 {
			for i := range fieldData.GetScalars().GetIntData().Data {
				fieldData.GetScalars().GetIntData().Data[i] = int32(i % 100)
			}
		}
		data = append(data, fieldData)
		loadFields = append(loadFields, item.name)
	}
	timestamps := make([]string, rawDataRows)
	baseTimestamp := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	for i := range timestamps {
		timestamps[i] = baseTimestamp.Add(time.Duration(i) * time.Microsecond).Format(time.RFC3339Nano)
	}
	fields = append(fields, &schemapb.FieldSchema{Name: "timestamptz_value", DataType: schemapb.DataType_Timestamptz})
	data = append(data, &schemapb.FieldData{Type: schemapb.DataType_Timestamptz, FieldName: "timestamptz_value", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: timestamps}}}}})
	loadFields = append(loadFields, "timestamptz_value")

	arrayTypes := []schemapb.DataType{
		schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Int64, schemapb.DataType_Float, schemapb.DataType_Double, schemapb.DataType_VarChar,
	}
	for _, elementType := range arrayTypes {
		name := "array_" + elementType.String()
		typeParams := []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "4"}}
		if elementType == schemapb.DataType_VarChar {
			typeParams = append(typeParams, &commonpb.KeyValuePair{Key: common.MaxLengthKey, Value: "128"})
		}
		fields = append(fields, &schemapb.FieldSchema{Name: name, DataType: schemapb.DataType_Array, ElementType: elementType, TypeParams: typeParams})
		data = append(data, deterministicArrayField(name, elementType, rawDataRows))
		loadFields = append(loadFields, name)
	}
	fields = append(fields, &schemapb.FieldSchema{Name: "json_value", DataType: schemapb.DataType_JSON})
	data = append(data, deterministicJSONField("json_value", rawDataRows, false))
	loadFields = append(loadFields, "json_value", common.MetaFieldName)
	data = append(data, deterministicJSONField(common.MetaFieldName, rawDataRows, true))
	// Partial load requires one vector field. This helper is loaded only to
	// satisfy that collection-level invariant; scalar assertions remain complete.
	fields = append(fields, vectorSchema("scalar_helper", schemapb.DataType_FloatVector, rawDataDim))
	data = append(data, deterministicFloatVectors("scalar_helper", rawDataRows, rawDataDim))
	loadFields = append(loadFields, "scalar_helper")
	return rawDataCampaign{name: "scalar", schema: &schemapb.CollectionSchema{EnableDynamicField: true, Fields: fields}, fields: data, loadFields: loadFields, index: true}
}

func newRawVectorCampaign() rawDataCampaign {
	fields := []*schemapb.FieldSchema{{Name: fixturePrimaryKey, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}
	data := []*schemapb.FieldData{testutils.NewInt64FieldData(fixturePrimaryKey, rawDataRows)}
	loadFields := []string{fixturePrimaryKey}
	types := []struct {
		name   string
		typeID schemapb.DataType
	}{
		{"binary_vector", schemapb.DataType_BinaryVector},
		{"float_vector", schemapb.DataType_FloatVector},
		{"float16_vector", schemapb.DataType_Float16Vector},
		{"bfloat16_vector", schemapb.DataType_BFloat16Vector},
		{"sparse_vector", schemapb.DataType_SparseFloatVector},
		{"int8_vector", schemapb.DataType_Int8Vector},
	}
	for _, item := range types {
		fields = append(fields, vectorSchema(item.name, item.typeID, rawDataDim))
		data = append(data, deterministicVectorField(item.name, item.typeID, rawDataRows, rawDataDim))
		loadFields = append(loadFields, item.name)
	}
	return rawDataCampaign{name: "vector", schema: &schemapb.CollectionSchema{Fields: fields}, fields: data, loadFields: loadFields, index: true, search: true}
}

func newStructArrayCampaign() rawDataCampaign {
	children := []*schemapb.FieldSchema{{
		Name: "ints", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
	}}
	for _, item := range []struct {
		name   string
		typeID schemapb.DataType
	}{
		{"binary_vectors", schemapb.DataType_BinaryVector},
		{"float_vectors", schemapb.DataType_FloatVector},
		{"float16_vectors", schemapb.DataType_Float16Vector},
		{"bfloat16_vectors", schemapb.DataType_BFloat16Vector},
		{"int8_vectors", schemapb.DataType_Int8Vector},
	} {
		children = append(children, &schemapb.FieldSchema{
			Name: item.name, DataType: schemapb.DataType_ArrayOfVector, ElementType: item.typeID,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(rawDataDim)}, {Key: common.MaxCapacityKey, Value: "100"}},
		})
	}
	structField := &schemapb.StructArrayFieldSchema{Name: "structs", Fields: children}
	regularFields := []*schemapb.FieldSchema{{Name: fixturePrimaryKey, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}, vectorSchema("struct_helper", schemapb.DataType_FloatVector, rawDataDim)}
	regularFields[0].FieldID, regularFields[1].FieldID = 100, 101
	structField.FieldID = 102
	for i, child := range children {
		child.FieldID = int64(103 + i)
	}
	schema := &schemapb.CollectionSchema{
		Fields:            regularFields,
		StructArrayFields: []*schemapb.StructArrayFieldSchema{structField},
	}
	structChildren := []*schemapb.FieldData{deterministicArrayField("ints", schemapb.DataType_Int32, rawDataRows)}
	structChildren[0].FieldId = children[0].GetFieldID()
	for _, child := range children[1:] {
		structChildren = append(structChildren, deterministicVectorArrayField(child.GetName(), child.GetFieldID(), child.GetElementType(), rawDataRows, rawDataDim))
	}
	structData := &schemapb.FieldData{
		Type: schemapb.DataType_ArrayOfStruct, FieldName: structField.GetName(), FieldId: structField.GetFieldID(),
		Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: structChildren}},
	}
	return rawDataCampaign{
		name: "struct_array", schema: schema,
		fields:     []*schemapb.FieldData{testutils.NewInt64FieldData(fixturePrimaryKey, rawDataRows), deterministicFloatVectors("struct_helper", rawDataRows, rawDataDim), structData},
		loadFields: []string{fixturePrimaryKey, structField.GetName()}, index: true,
	}
}

func vectorSchema(name string, dataType schemapb.DataType, dim int) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{Name: name, DataType: dataType}
	if dataType != schemapb.DataType_SparseFloatVector {
		field.TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(dim)}}
	}
	return field
}

func deterministicFloatVectors(name string, rows, dim int) *schemapb.FieldData {
	values := make([]float32, rows*dim)
	for row := 0; row < rows; row++ {
		values[row*dim] = float32(row)
		for column := 1; column < dim; column++ {
			values[row*dim+column] = float32(column) / 100
		}
	}
	return testutils.NewFloatVectorFieldDataWithValue(name, values, dim)
}

func deterministicVectorField(name string, dataType schemapb.DataType, rows, dim int) *schemapb.FieldData {
	floatValues := make([]float32, rows*dim)
	for row := 0; row < rows; row++ {
		for column := 0; column < dim; column++ {
			floatValues[row*dim+column] = float32((row+1)*(column+1)) / 100
		}
	}
	switch dataType {
	case schemapb.DataType_BinaryVector:
		values := make([]byte, rows*dim/8)
		for row := 0; row < rows; row++ {
			values[row*dim/8] = byte(row)
		}
		return testutils.NewBinaryVectorFieldDataWithValue(name, values, dim)
	case schemapb.DataType_FloatVector:
		return deterministicFloatVectors(name, rows, dim)
	case schemapb.DataType_Float16Vector:
		return testutils.NewFloat16VectorFieldDataWithValue(name, typeutil.Float32ArrayToFloat16Bytes(floatValues), dim)
	case schemapb.DataType_BFloat16Vector:
		return testutils.NewBFloat16VectorFieldDataWithValue(name, typeutil.Float32ArrayToBFloat16Bytes(floatValues), dim)
	case schemapb.DataType_SparseFloatVector:
		contents := make([][]byte, rows)
		for row := 0; row < rows; row++ {
			contents[row] = typeutil.CreateSparseFloatRow([]uint32{uint32(row % dim)}, []float32{float32(row + 1)})
		}
		return &schemapb.FieldData{Type: dataType, FieldName: name, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: int64(dim), Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: int64(dim), Contents: contents}},
		}}}
	case schemapb.DataType_Int8Vector:
		values := make([]byte, rows*dim)
		for row := 0; row < rows; row++ {
			for column := 0; column < dim; column++ {
				values[row*dim+column] = byte((row + column) % 100)
			}
		}
		return testutils.NewInt8VectorFieldDataWithValue(name, values, dim)
	default:
		panic("unsupported deterministic vector type: " + dataType.String())
	}
}

func deterministicVectorArrayField(name string, fieldID int64, elementType schemapb.DataType, rows, dim int) *schemapb.FieldData {
	rowValues := make([]*schemapb.VectorField, rows)
	for row := 0; row < rows; row++ {
		floatValues := make([]float32, 2*dim)
		for i := range floatValues {
			floatValues[i] = float32((row+1)*(i+1)) / 100
		}
		value := &schemapb.VectorField{Dim: int64(dim)}
		switch elementType {
		case schemapb.DataType_BinaryVector:
			value.Data = &schemapb.VectorField_BinaryVector{BinaryVector: []byte{byte(row), byte(row + 1)}}
		case schemapb.DataType_FloatVector:
			value.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: floatValues}}
		case schemapb.DataType_Float16Vector:
			value.Data = &schemapb.VectorField_Float16Vector{Float16Vector: typeutil.Float32ArrayToFloat16Bytes(floatValues)}
		case schemapb.DataType_BFloat16Vector:
			value.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: typeutil.Float32ArrayToBFloat16Bytes(floatValues)}
		case schemapb.DataType_Int8Vector:
			bytes := make([]byte, 2*dim)
			for i := range bytes {
				bytes[i] = byte((row + i) % 100)
			}
			value.Data = &schemapb.VectorField_Int8Vector{Int8Vector: bytes}
		}
		rowValues[row] = value
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_ArrayOfVector, FieldName: name, FieldId: fieldID,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: int64(dim), Data: &schemapb.VectorField_VectorArray{
			VectorArray: &schemapb.VectorArray{Dim: int64(dim), ElementType: elementType, Data: rowValues},
		}}},
	}
}

func firstFloatVector(fields []*schemapb.FieldData, name string, dim int) []float32 {
	for _, field := range fields {
		if field.GetFieldName() == name {
			return append([]float32(nil), field.GetVectors().GetFloatVector().GetData()[:dim]...)
		}
	}
	return nil
}

func deterministicJSONField(name string, rows int, dynamic bool) *schemapb.FieldData {
	values := make([][]byte, rows)
	for i := range values {
		values[i] = []byte(fmt.Sprintf(`{"row":%d,"kind":"%s"}`, i, name))
	}
	field := testutils.NewJSONFieldDataWithValue(name, values)
	field.IsDynamic = dynamic
	return field
}

func deterministicArrayField(name string, elementType schemapb.DataType, rows int) *schemapb.FieldData {
	values := make([]*schemapb.ScalarField, rows)
	for row := range values {
		switch elementType {
		case schemapb.DataType_Bool:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{row%2 == 0, row%3 == 0}}}}
		case schemapb.DataType_Int8:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{int32(row % 100), int32((row + 1) % 100)}}}}
		case schemapb.DataType_Int16, schemapb.DataType_Int32:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{int32(row), int32(row + 1)}}}}
		case schemapb.DataType_Int64:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{int64(row), int64(row + 1)}}}}
		case schemapb.DataType_Float:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{float32(row), float32(row) + .5}}}}
		case schemapb.DataType_Double:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{float64(row), float64(row) + .5}}}}
		case schemapb.DataType_VarChar:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{fmt.Sprintf("row-%d", row), "tail"}}}}
		}
	}
	return &schemapb.FieldData{Type: schemapb.DataType_Array, FieldName: name, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: values, ElementType: elementType}}}}}
}

func requestedFieldIDs(schema *schemapb.CollectionSchema, names []string) []int64 {
	nameToID := make(map[string]int64)
	structChildren := make(map[string][]int64)
	nextID := int64(common.StartOfUserFieldID)
	for _, field := range schema.GetFields() {
		nameToID[field.GetName()] = nextID
		field.FieldID = nextID
		nextID++
	}
	if schema.GetEnableDynamicField() {
		nameToID[common.MetaFieldName] = nextID
		nextID++
	}
	for _, field := range schema.GetStructArrayFields() {
		field.FieldID = nextID
		nextID++
		for _, child := range field.GetFields() {
			child.FieldID = nextID
			structChildren[field.GetName()] = append(structChildren[field.GetName()], nextID)
			nextID++
		}
	}
	ids := make([]int64, 0, len(names))
	for _, name := range names {
		if children := structChildren[name]; len(children) > 0 {
			ids = append(ids, children...)
		} else {
			ids = append(ids, nameToID[name])
		}
	}
	return ids
}

func fieldDataByName(fields []*schemapb.FieldData) map[string]*schemapb.FieldData {
	result := make(map[string]*schemapb.FieldData, len(fields))
	for _, field := range fields {
		result[field.GetFieldName()] = field
	}
	return result
}
