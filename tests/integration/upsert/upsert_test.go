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

package upsert

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

type UpsertSuite struct {
	integration.MiniClusterSuite
}

func (s *UpsertSuite) initCollection(dbName, collectionName string, dim int, schema *schemapb.CollectionSchema) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.CreateCollection(ctx, &integration.CreateCollectionConfig{
		DBName:         dbName,
		Dim:            dim,
		CollectionName: collectionName,
		ChannelNum:     1,
	}, schema)

	// load
	loadStatus, err := s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.True(merr.Ok(loadStatus))
	s.WaitForLoad(ctx, collectionName)
	log.Info("initCollection Done")
}

func (s *UpsertSuite) TestUpsertAutoIDFalse() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestUpsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000
	start := 0

	schema := integration.ConstructSchema(collectionName, dim, false)
	s.initCollection(dbName, collectionName, dim, schema)

	pkFieldData := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, int64(start))
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	upsertResult, err := c.Proxy.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))

	// flush
	s.Flush(ctx, dbName, collectionName)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, "")
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{integration.Int64Field}, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Strong

	searchResult, _ := c.Proxy.Search(ctx, searchReq)
	checkFunc := func(data int) error {
		if data < start || data > start+rowNum {
			// return errors.New("upsert check pk fail")
			return errors.Newf("upsert check pk fail, data: %d, start: %d, rowNum: %d", data, start, rowNum)
		}
		return nil
	}
	for _, id := range searchResult.Results.Ids.GetIntId().GetData() {
		s.NoError(checkFunc(int(id)))
	}
	for _, data := range searchResult.Results.FieldsData[0].GetScalars().GetLongData().GetData() {
		s.NoError(checkFunc(int(data)))
	}

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)

	log.Info("===========================")
	log.Info("===========================")
	log.Info("TestUpsertAutoIDFalse succeed")
	log.Info("===========================")
	log.Info("===========================")
}

func (s *UpsertSuite) TestUpsertAutoIDTrue() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestUpsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000
	start := 0

	schema := integration.ConstructSchema(collectionName, dim, true)
	s.initCollection(dbName, collectionName, dim, schema)

	pkFieldData := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, 0)
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	upsertResult, err := c.Proxy.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))

	// flush
	s.Flush(ctx, dbName, collectionName)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, "")
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{integration.Int64Field}, metric.L2, params, nq, dim, topk, roundDecimal)

	searchResult, _ := c.Proxy.Search(ctx, searchReq)
	checkFunc := func(data int) error {
		if data >= start && data <= start+rowNum {
			return errors.New("upsert check pk fail")
		}
		return nil
	}
	for _, id := range searchResult.Results.Ids.GetIntId().GetData() {
		s.NoError(checkFunc(int(id)))
	}
	for _, data := range searchResult.Results.FieldsData[0].GetScalars().GetLongData().GetData() {
		s.NoError(checkFunc(int(data)))
	}

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)

	log.Info("===========================")
	log.Info("===========================")
	log.Info("TestUpsertAutoIDTrue succeed")
	log.Info("===========================")
	log.Info("===========================")
}

func (s *UpsertSuite) TestUpsert_ExecuteDeleteAndInsert() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestUpsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000
	start := 0

	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         integration.FloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}
	int64Field := &schemapb.FieldSchema{
		FieldID:    102,
		Name:       "int64_field",
		DataType:   schemapb.DataType_Int64,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}},
	}
	schema := integration.ConstructSchema(collectionName, dim, false, pk, fVec, int64Field)
	s.initCollection(dbName, collectionName, dim, schema)

	pkFieldData := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, int64(start))
	int64FieldData := integration.NewInt64FieldDataWithStart(int64Field.Name, rowNum, int64(100000))
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	upsertResult, err := c.Proxy.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn, int64FieldData},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))
	s.Equal(int64(0), upsertResult.GetDeleteCnt())
	s.Equal(int64(rowNum), upsertResult.GetInsertCnt())

	// flush
	s.Flush(ctx, dbName, collectionName)

	// search
	expr := fmt.Sprintf("%s > %d", integration.Int64Field, start)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, "")
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{int64Field.Name}, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Strong

	searchResult, _ := c.Proxy.Search(ctx, searchReq)
	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)
	checkFunc := func(data int, leftBound, rightBound int) error {
		if data < leftBound || data > rightBound {
			return errors.Newf("upsert check pk fail, data: %d, leftBound: %d, rightBound: %d", data, leftBound, rightBound)
		}
		return nil
	}
	for _, data := range searchResult.Results.FieldsData[0].GetScalars().GetLongData().GetData() {
		s.NoError(checkFunc(int(data), 100000, 100000+rowNum))
	}

	// expected to delete old records and insert new records
	updateInt64FieldData := integration.NewInt64FieldDataWithStart(int64Field.Name, rowNum, int64(200000))
	updateUpsertResult2, err := c.Proxy.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn, updateInt64FieldData},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))
	s.Equal(int64(rowNum), updateUpsertResult2.GetDeleteCnt())
	s.Equal(int64(rowNum), updateUpsertResult2.GetInsertCnt())

	// flush
	s.Flush(ctx, dbName, collectionName)

	// verify result
	expr = fmt.Sprintf("%s > %d", int64Field.Name, start)
	searchReq = integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{int64Field.Name}, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
	searchResult, _ = c.Proxy.Search(ctx, searchReq)
	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)
	for _, data := range searchResult.Results.FieldsData[0].GetScalars().GetLongData().GetData() {
		s.NoError(checkFunc(int(data), 200000, 200000+rowNum))
	}

	log.Info("===========================")
	log.Info("===========================")
	log.Info("TestUpsert_ExecuteDeleteAndInsert succeed")
	log.Info("===========================")
	log.Info("===========================")
}

func (s *UpsertSuite) TestUpsert_PartialUpdateScalarField() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestUpsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 30
	start := 0

	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         integration.FloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}
	int64Field := &schemapb.FieldSchema{
		FieldID:    102,
		Name:       "int64_field",
		DataType:   schemapb.DataType_Int64,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}},
	}
	schema := integration.ConstructSchema(collectionName, dim, false, pk, fVec, int64Field)
	s.initCollection(dbName, collectionName, dim, schema)

	pkFieldData := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, int64(start))
	int64FieldData := integration.NewInt64FieldDataWithStart(int64Field.Name, rowNum, int64(100000))
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	upsertResult, err := c.Proxy.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn, int64FieldData},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))

	// flush
	s.Flush(ctx, dbName, collectionName)

	// search
	expr := fmt.Sprintf("%s > %d", integration.Int64Field, start)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, "")
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{int64Field.Name}, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Strong

	searchResult, _ := c.Proxy.Search(ctx, searchReq)
	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)
	checkFunc := func(data int, leftBound, rightBound int) error {
		if data < leftBound || data > rightBound {
			return errors.Newf("upsert check pk fail, data: %d, leftBound: %d, rightBound: %d", data, leftBound, rightBound)
		}
		return nil
	}
	s.Equal(nq*topk, typeutil.GetSizeOfIDs(searchResult.Results.GetIds()))
	for _, data := range searchResult.Results.FieldsData[0].GetScalars().GetLongData().GetData() {
		s.NoError(checkFunc(int(data), 100000, 100000+rowNum))
	}

	// expected to delete old records and insert new records
	updateInt64FieldData := integration.NewInt64FieldDataWithStart(int64Field.Name, rowNum, int64(200000))
	updateUpsertResult2, err := c.Proxy.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, updateInt64FieldData}, // only pass pk and int64 field
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))
	s.Equal(int64(rowNum), updateUpsertResult2.GetDeleteCnt())
	s.Equal(int64(rowNum), updateUpsertResult2.GetInsertCnt())

	// flush
	s.Flush(ctx, dbName, collectionName)

	// verify result
	searchReq = integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{int64Field.Name}, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
	searchResult, _ = c.Proxy.Search(ctx, searchReq)
	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)
	s.Equal(nq*topk, typeutil.GetSizeOfIDs(searchResult.Results.GetIds()))
	for _, data := range searchResult.Results.FieldsData[0].GetScalars().GetLongData().GetData() {
		s.NoError(checkFunc(int(data), 200000, 200000+rowNum))
	}

	log.Info("===========================")
	log.Info("===========================")
	log.Info("TestUpsert_PartialUpdateScalarField succeed")
	log.Info("===========================")
	log.Info("===========================")
}

func (s *UpsertSuite) TestUpsert_PartialUpdateVectorField() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestUpsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 30
	start := 0

	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         integration.FloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}
	int64Field := &schemapb.FieldSchema{
		FieldID:    102,
		Name:       "int64_field",
		DataType:   schemapb.DataType_Int64,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}},
	}
	schema := integration.ConstructSchema(collectionName, dim, false, pk, fVec, int64Field)
	s.initCollection(dbName, collectionName, dim, schema)

	pkFieldData := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, int64(start))
	int64FieldData := integration.NewInt64FieldDataWithStart(int64Field.Name, rowNum, int64(100000))
	total := rowNum * dim
	ret := make([]float32, 0, total)
	for i := 0; i < total; i++ {
		ret = append(ret, float32(111))
	}
	fVecColumn := &schemapb.FieldData{
		Type:      schemapb.DataType_Float16Vector,
		FieldName: integration.FloatVecField,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: ret,
					},
				},
			},
		},
	}
	hashKeys := integration.GenerateHashKeys(rowNum)
	upsertResult, err := c.Proxy.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn, int64FieldData},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))

	// flush
	s.Flush(ctx, dbName, collectionName)

	// search
	expr := fmt.Sprintf("%s > %d", integration.Int64Field, start)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, "")
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{integration.FloatVecField}, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Strong

	searchResult, _ := c.Proxy.Search(ctx, searchReq)
	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)
	s.Equal(nq*topk, typeutil.GetSizeOfIDs(searchResult.Results.GetIds()))
	for _, fieldData := range searchResult.Results.FieldsData {
		if fieldData.GetFieldName() == integration.FloatVecField {
			for _, v := range fieldData.GetVectors().GetFloatVector().GetData() {
				s.Equal(float32(111), v)
			}
		}
	}

	// expected to delete old records and insert new records
	ret = make([]float32, 0, total)
	for i := 0; i < total; i++ {
		ret = append(ret, float32(222))
	}
	updateFVecColumn := &schemapb.FieldData{
		Type:      schemapb.DataType_Float16Vector,
		FieldName: integration.FloatVecField,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: ret,
					},
				},
			},
		},
	}
	updateUpsertResult2, err := c.Proxy.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, updateFVecColumn}, // only pass pk and vector field
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))
	s.Equal(int64(rowNum), updateUpsertResult2.GetDeleteCnt())
	s.Equal(int64(rowNum), updateUpsertResult2.GetInsertCnt())

	// flush
	s.Flush(ctx, dbName, collectionName)

	// verify result
	searchReq = integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{integration.FloatVecField}, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
	searchResult, _ = c.Proxy.Search(ctx, searchReq)
	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)
	s.Equal(nq*topk, typeutil.GetSizeOfIDs(searchResult.Results.GetIds()))
	for _, fieldData := range searchResult.Results.FieldsData {
		if fieldData.GetFieldName() == integration.FloatVecField {
			for _, v := range fieldData.GetVectors().GetFloatVector().GetData() {
				s.Equal(float32(222), v)
			}
		}
	}

	log.Info("===========================")
	log.Info("===========================")
	log.Info("TestUpsert_PartialUpdateVectorField succeed")
	log.Info("===========================")
	log.Info("===========================")
}

func (s *UpsertSuite) TestUpsert_PartialUpdateDynamicField() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestUpsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 30
	start := 0

	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         integration.FloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}
	int64Field := &schemapb.FieldSchema{
		FieldID:    102,
		Name:       "int64_field",
		DataType:   schemapb.DataType_Int64,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}},
	}
	schema := integration.ConstructSchema(collectionName, dim, false, pk, fVec, int64Field)
	schema.EnableDynamicField = true
	s.initCollection(dbName, collectionName, dim, schema)

	pkFieldData := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, int64(start))
	int64FieldData := integration.NewInt64FieldDataWithStart(int64Field.Name, rowNum, int64(100000))
	ret := make([][]byte, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		data := map[string]interface{}{
			"a": "a_value_1",
			"b": 1,
		}
		v, _ := json.Marshal(data)
		ret = append(ret, v)
	}
	dynamicFieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "$meta",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: ret,
					},
				},
			},
		},
		IsDynamic: true,
	}
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	upsertResult, err := c.Proxy.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn, int64FieldData, dynamicFieldData},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))

	// flush
	s.Flush(ctx, dbName, collectionName)

	// search
	expr := fmt.Sprintf("%s > %d", integration.Int64Field, start)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, "")
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{dynamicFieldData.FieldName}, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Strong

	searchResult, _ := c.Proxy.Search(ctx, searchReq)
	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)
	s.Equal(nq*topk, typeutil.GetSizeOfIDs(searchResult.Results.GetIds()))
	for _, fieldData := range searchResult.Results.FieldsData {
		if fieldData.IsDynamic {
			for _, data := range fieldData.GetScalars().GetJsonData().GetData() {
				var v map[string]interface{}
				err := json.Unmarshal(data, &v)
				s.NoError(err)
				s.Equal(v["a"], "a_value_1")
				s.Equal(v["b"], float64(1))
			}
		}
	}

	// expected to delete old records and insert new records
	ret = make([][]byte, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		data := map[string]interface{}{
			"a": "a_value_2",
			"b": 2,
			"c": 2,
		}
		v, _ := json.Marshal(data)
		ret = append(ret, v)
	}
	updateDynamicFieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "$meta",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: ret,
					},
				},
			},
		},
		IsDynamic: true,
	}
	updateUpsertResult2, err := c.Proxy.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, updateDynamicFieldData}, // only pass pk and int64 field
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))
	s.Equal(int64(rowNum), updateUpsertResult2.GetDeleteCnt())
	s.Equal(int64(rowNum), updateUpsertResult2.GetInsertCnt())

	// flush
	s.Flush(ctx, dbName, collectionName)

	// verify result
	searchReq = integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{dynamicFieldData.FieldName}, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
	searchResult, _ = c.Proxy.Search(ctx, searchReq)
	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)
	s.Equal(nq*topk, typeutil.GetSizeOfIDs(searchResult.Results.GetIds()))
	for _, fieldData := range searchResult.Results.FieldsData {
		if fieldData.IsDynamic {
			for _, data := range fieldData.GetScalars().GetJsonData().GetData() {
				var v map[string]interface{}
				err := json.Unmarshal(data, &v)
				s.NoError(err)
				s.Equal(v["a"], "a_value_2")
				s.Equal(v["b"], float64(2))
				s.Equal(v["c"], float64(2))
			}
		}
	}

	log.Info("===========================")
	log.Info("===========================")
	log.Info("TestUpsert_PartialUpdateDynamicField succeed")
	log.Info("===========================")
	log.Info("===========================")
}

func TestUpsert(t *testing.T) {
	suite.Run(t, new(UpsertSuite))
}
