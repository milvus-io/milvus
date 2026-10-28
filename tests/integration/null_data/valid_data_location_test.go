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

package hellomilvus

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	validDataScalarField = "nullableScalar"
	validDataVectorField = "nullableVector"
	validDataDim         = 8
)

// validDataRowValid is the row validity the test writes: even primary keys
// are valid, odd ones are null. A valid scalar row stores its primary key.
func validDataRowValid(pk int64) bool {
	return pk%2 == 0
}

// validDataLocation says where a test batch puts its row validity.
type validDataLocation int

const (
	// validDataLegacyOnly writes only FieldData.valid_data, as an SDK built
	// before the field-specific location existed does.
	validDataLegacyOnly validDataLocation = iota
	// validDataFieldSpecificOnly writes only ScalarField/VectorField.valid_data.
	validDataFieldSpecificOnly
	// validDataBoth writes both locations through typeutil.SetFieldDataValidData.
	validDataBoth
)

// newValidDataBatch builds the two nullable columns for primary keys
// [base, base+rows), with the row validity at the given location.
func newValidDataBatch(base int64, rows int, location validDataLocation) []*schemapb.FieldData {
	pks := make([]int64, 0, rows)
	validData := make([]bool, 0, rows)
	values := make([]int64, 0, rows)
	vectors := make([]float32, 0, rows*validDataDim)
	for i := 0; i < rows; i++ {
		pk := base + int64(i)
		pks = append(pks, pk)
		valid := validDataRowValid(pk)
		validData = append(validData, valid)
		if valid {
			values = append(values, pk)
			for d := 0; d < validDataDim; d++ {
				vectors = append(vectors, float32(i+d))
			}
		}
	}
	pkField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: integration.Int64Field,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
		}},
	}
	scalar := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: validDataScalarField,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
		}},
	}
	vector := &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: validDataVectorField,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  validDataDim,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vectors}},
		}},
	}
	switch location {
	case validDataLegacyOnly:
		scalar.ValidData = validData
		vector.ValidData = validData
	case validDataFieldSpecificOnly:
		scalar.GetScalars().ValidData = validData
		vector.GetVectors().ValidData = validData
	default:
		typeutil.SetFieldDataValidData(scalar, validData)
		typeutil.SetFieldDataValidData(vector, validData)
	}
	return []*schemapb.FieldData{
		pkField,
		integration.NewFloatVectorFieldData(integration.FloatVecField, rows, validDataDim),
		scalar,
		vector,
	}
}

// checkValidDataLocations checks that every returned nullable column carries
// the same row validity in the legacy and the field-specific location, and that
// the validity and scalar values match the primary keys of the rows. withVector
// requires the nullable vector column as well.
func (s *NullDataSuite) checkValidDataLocations(label string, pks []int64, fieldDatas []*schemapb.FieldData, withVector bool) {
	s.Require().NotEmpty(pks, label)
	scalar := getTargetFieldData(validDataScalarField, fieldDatas)
	s.Require().NotNil(scalar, label)
	s.Require().Len(scalar.GetValidData(), len(pks), label)
	s.Equal(scalar.GetValidData(), scalar.GetScalars().GetValidData(), label)
	values := scalar.GetScalars().GetLongData().GetData()
	s.Require().Len(values, len(pks), label)
	for i, pk := range pks {
		s.Equal(validDataRowValid(pk), scalar.GetValidData()[i], "%s scalar row %d pk %d", label, i, pk)
		if validDataRowValid(pk) {
			s.Equal(pk, values[i], "%s scalar row %d", label, i)
		}
	}

	if !withVector {
		return
	}
	vector := getTargetFieldData(validDataVectorField, fieldDatas)
	s.Require().NotNil(vector, label)
	s.Require().Len(vector.GetValidData(), len(pks), label)
	s.Equal(vector.GetValidData(), vector.GetVectors().GetValidData(), label)
	for i, pk := range pks {
		s.Equal(validDataRowValid(pk), vector.GetValidData()[i], "%s vector row %d pk %d", label, i, pk)
	}
}

func (s *NullDataSuite) checkValidDataResults(ctx context.Context, label, collectionName string) {
	c := s.Cluster
	const (
		nq           = 2
		topk         = 10
		roundDecimal = -1
	)
	params := integration.GetSearchParams(integration.IndexFaissIDMap, metric.L2)
	outputFields := []string{validDataScalarField, validDataVectorField}

	// Search
	searchReq := integration.ConstructSearchRequest("", collectionName, "", integration.FloatVecField,
		schemapb.DataType_FloatVector, outputFields, metric.L2, params, nq, validDataDim, topk, roundDecimal)
	searchResult, err := c.MilvusClient.Search(ctx, searchReq)
	s.Require().NoError(merr.CheckRPCCall(searchResult, err))
	s.checkValidDataLocations(label+" search", searchResult.GetResults().GetIds().GetIntId().GetData(),
		searchResult.GetResults().GetFieldsData(), true)

	// Query
	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           fmt.Sprintf("%s >= 0", integration.Int64Field),
		OutputFields:   append([]string{integration.Int64Field}, outputFields...),
	})
	s.Require().NoError(merr.CheckRPCCall(queryResult, err))
	pkData := getTargetFieldData(integration.Int64Field, queryResult.GetFieldsData())
	s.checkValidDataLocations(label+" query", pkData.GetScalars().GetLongData().GetData(), queryResult.GetFieldsData(), true)

	// HybridSearch over the plain and the nullable vector field
	rrfParams, err := json.Marshal(map[string]float64{proxy.RRFParamsKey: 60})
	s.Require().NoError(err)
	hybridResult, err := c.MilvusClient.HybridSearch(ctx, &milvuspb.HybridSearchRequest{
		CollectionName: collectionName,
		Requests: []*milvuspb.SearchRequest{
			integration.ConstructSearchRequest("", collectionName, "", integration.FloatVecField,
				schemapb.DataType_FloatVector, nil, metric.L2, params, nq, validDataDim, topk, roundDecimal),
			integration.ConstructSearchRequest("", collectionName, "", validDataVectorField,
				schemapb.DataType_FloatVector, nil, metric.L2, params, nq, validDataDim, topk, roundDecimal),
		},
		OutputFields: outputFields,
		RankParams: []*commonpb.KeyValuePair{
			{Key: proxy.RankTypeKey, Value: "rrf"},
			{Key: proxy.ParamsKey, Value: string(rrfParams)},
			{Key: proxy.LimitKey, Value: strconv.Itoa(topk)},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(hybridResult, err))
	s.checkValidDataLocations(label+" hybrid search", hybridResult.GetResults().GetIds().GetIntId().GetData(),
		hybridResult.GetResults().GetFieldsData(), true)

	// Group-by search on the nullable scalar field: the group values carry row
	// validity too.
	groupReq := integration.ConstructSearchRequest("", collectionName, "", integration.FloatVecField,
		schemapb.DataType_FloatVector, outputFields, metric.L2, params, nq, validDataDim, topk, roundDecimal)
	groupReq.SearchParams = append(groupReq.SearchParams, &commonpb.KeyValuePair{
		Key:   proxy.GroupByFieldKey,
		Value: validDataScalarField,
	})
	groupResult, err := c.MilvusClient.Search(ctx, groupReq)
	s.Require().NoError(merr.CheckRPCCall(groupResult, err))
	groupPKs := groupResult.GetResults().GetIds().GetIntId().GetData()
	s.checkValidDataLocations(label+" group-by search", groupPKs, groupResult.GetResults().GetFieldsData(), true)
	var groupValues []*schemapb.FieldData
	if value := groupResult.GetResults().GetGroupByFieldValue(); value != nil {
		groupValues = append(groupValues, value)
	}
	groupValues = append(groupValues, groupResult.GetResults().GetGroupByFieldValues()...)
	s.Require().NotEmpty(groupValues, label)
	for i, groupValue := range groupValues {
		renamed := proto.Clone(groupValue).(*schemapb.FieldData)
		renamed.FieldName = validDataScalarField
		s.checkValidDataLocations(fmt.Sprintf("%s group-by value %d", label, i), groupPKs, []*schemapb.FieldData{renamed}, false)
	}
}

// TestNullData_ValidDataLocations checks that Search, Query, HybridSearch and
// group-by Search return row validity of nullable scalar and vector fields in
// both the legacy FieldData.valid_data and the field-specific location, for
// rows inserted with validity in either location, from growing and from sealed
// segments.
func (s *NullDataSuite) TestNullData_ValidDataLocations() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const rows = 100
	collectionName := "TestNullDataValidDataLocations" + funcutil.GenRandomStr()
	schema := integration.ConstructSchemaOfVecDataType(collectionName, validDataDim, false, schemapb.DataType_FloatVector)
	schema.Fields = append(schema.Fields,
		&schemapb.FieldSchema{FieldID: 102, Name: validDataScalarField, DataType: schemapb.DataType_Int64, Nullable: true},
		&schemapb.FieldSchema{
			FieldID:    103,
			Name:       validDataVectorField,
			DataType:   schemapb.DataType_FloatVector,
			Nullable:   true,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(validDataDim)}},
		},
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName:   collectionName,
		Schema:           marshaledSchema,
		ShardsNum:        common.DefaultShardsNum,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))

	for _, field := range []string{integration.FloatVecField, validDataVectorField} {
		status, err = c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: collectionName,
			FieldName:      field,
			IndexName:      field,
			ExtraParams:    integration.ConstructIndexParam(validDataDim, integration.IndexFaissIDMap, metric.L2),
		})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.WaitForIndexBuiltWithIndexName(ctx, collectionName, field, field)
	}
	load := func() {
		status, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: collectionName})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.WaitForLoad(ctx, collectionName)
	}
	load()

	// The batches carry the row validity at both locations, only at the legacy
	// location, and only at the field-specific location.
	for _, batch := range []struct {
		base     int64
		location validDataLocation
	}{{1000, validDataBoth}, {2000, validDataLegacyOnly}, {3000, validDataFieldSpecificOnly}} {
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			CollectionName: collectionName,
			FieldsData:     newValidDataBatch(batch.base, rows, batch.location),
			HashKeys:       integration.GenerateHashKeys(rows),
			NumRows:        rows,
		})
		s.Require().NoError(merr.CheckRPCCall(insertResult, err))
	}
	s.checkValidDataResults(ctx, "growing", collectionName)

	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{collectionName}})
	s.Require().NoError(merr.CheckRPCCall(flushResp, err))
	segmentIDs, has := flushResp.GetFlushCollSegIDs()[collectionName]
	s.Require().True(has)
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	s.Require().True(has)
	s.WaitForFlush(ctx, segmentIDs.GetData(), flushTs, "", collectionName)

	// Reload so that every row is served from a sealed segment.
	status, err = c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: collectionName})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	load()
	s.checkValidDataResults(ctx, "sealed", collectionName)

	status, err = c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: collectionName})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	status, err = c.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: collectionName})
	s.Require().NoError(merr.CheckRPCCall(status, err))
}
