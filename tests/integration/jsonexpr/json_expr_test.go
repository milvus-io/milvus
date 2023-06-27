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

package jsonexpr

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/stretchr/testify/suite"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"go.uber.org/zap"
)

type JSONExprSuite struct {
	integration.MiniClusterSuite
}

func (s *JSONExprSuite) TestJsonEnableDynamicSchema() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()
	prefix := "TestHelloMilvus"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 100

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         integration.Int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         integration.FloatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:               collectionName,
			Description:        "",
			AutoID:             false,
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema := constructCollectionSchema()
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.NoError(err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.Equal(showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	describeCollectionResp, err := c.Proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collectionName})
	s.NoError(err)
	s.True(describeCollectionResp.Schema.EnableDynamicField)
	s.Equal(2, len(describeCollectionResp.GetSchema().GetFields()))

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	jsonData := newJSONData(common.MetaFieldName, rowNum)
	jsonData.IsDynamic = true
	s.insertFlushIndexLoad(ctx, dbName, collectionName, rowNum, dim, []*schemapb.FieldData{fVecColumn, jsonData})

	s.checkSearch(collectionName, common.MetaFieldName, dim)
}

func (s *JSONExprSuite) TestJSON_InsertWithoutDynamicData() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestHelloMilvus"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 100

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         integration.Int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         integration.FloatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:               collectionName,
			Description:        "",
			AutoID:             false,
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema := constructCollectionSchema()
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.NoError(err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.Equal(showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	describeCollectionResp, err := c.Proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collectionName})
	s.NoError(err)
	s.True(describeCollectionResp.Schema.EnableDynamicField)
	s.Equal(2, len(describeCollectionResp.GetSchema().GetFields()))

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	s.insertFlushIndexLoad(ctx, dbName, collectionName, rowNum, dim, []*schemapb.FieldData{fVecColumn})

	expr := ""
	// search
	expr = `$meta["A"] > 90`
	checkFunc := func(result *milvuspb.SearchResults) {
		for _, f := range result.Results.GetFieldsData() {
			s.Nil(f)
		}
	}
	s.doSearch(collectionName, []string{common.MetaFieldName}, expr, dim, checkFunc)
	log.Info("GT expression run successfully")
}

func (s *JSONExprSuite) TestJSON_DynamicSchemaWithJSON() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestHelloMilvus"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 100

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         integration.Int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         integration.FloatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		j := &schemapb.FieldSchema{
			Name:        integration.JSONField,
			Description: "json field",
			DataType:    schemapb.DataType_JSON,
		}
		return &schemapb.CollectionSchema{
			Name:               collectionName,
			Description:        "",
			AutoID:             false,
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
				j,
			},
		}
	}
	schema := constructCollectionSchema()
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.NoError(err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.Equal(showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	describeCollectionResp, err := c.Proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collectionName})
	s.NoError(err)
	s.True(describeCollectionResp.Schema.EnableDynamicField)
	s.Equal(3, len(describeCollectionResp.GetSchema().GetFields()))

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	jsonData := newJSONData(integration.JSONField, rowNum)
	dynamicData := newJSONData(common.MetaFieldName, rowNum)
	dynamicData.IsDynamic = true
	s.insertFlushIndexLoad(ctx, dbName, collectionName, rowNum, dim, []*schemapb.FieldData{fVecColumn, jsonData, dynamicData})

	s.checkSearch(collectionName, common.MetaFieldName, dim)

	expr := ""
	// search
	expr = `jsonField["A"] < 10`
	checkFunc := func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(integration.JSONField, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(5, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{integration.JSONField}, expr, dim, checkFunc)
	log.Info("LT expression run successfully")

	expr = `jsonField["A"] <= 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(integration.JSONField, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(3, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{integration.JSONField}, expr, dim, checkFunc)
	log.Info("LE expression run successfully")

	expr = `jsonField["A"] == 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(integration.JSONField, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(1, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{integration.JSONField}, expr, dim, checkFunc)
	log.Info("EQ expression run successfully")

	expr = `jsonField["C"][0] in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(integration.JSONField, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(4, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{integration.JSONField}, expr, dim, checkFunc)
	log.Info("IN expression run successfully")

	expr = `jsonField["C"][0] not in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(integration.JSONField, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{integration.JSONField}, expr, dim, checkFunc)
	log.Info("NIN expression run successfully")

	expr = `jsonField["E"]["G"] > 100`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(integration.JSONField, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(9, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{integration.JSONField}, expr, dim, checkFunc)
	log.Info("nested path expression run successfully")

	expr = `jsonField == ""`
	s.doSearchWithInvalidExpr(collectionName, []string{integration.JSONField}, expr, dim)
}

func (s *JSONExprSuite) checkSearch(collectionName, fieldName string, dim int) {
	expr := ""
	// search
	expr = `$meta["A"] > 90`
	checkFunc := func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(5, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{"A"}, expr, dim, checkFunc)
	log.Info("GT expression run successfully")

	expr = `$meta["A"] < 10`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(5, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{"B"}, expr, dim, checkFunc)
	log.Info("LT expression run successfully")

	expr = `$meta["A"] <= 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(3, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{"C"}, expr, dim, checkFunc)
	log.Info("LE expression run successfully")

	expr = `A >= 95`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(3, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("GE expression run successfully")

	expr = `$meta["A"] == 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(1, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("EQ expression run successfully")

	expr = `A != 95`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("NE expression run successfully")

	expr = `not (A != 95)`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(1, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("NOT NE expression run successfully")

	expr = `A > 90 && B < 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(2, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("NE expression run successfully")

	expr = `A > 95 || 5 > B`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(4, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("NE expression run successfully")

	expr = `not (A == 95)`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("NOT expression run successfully")

	expr = `A in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(3, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("IN expression run successfully")

	expr = `A not in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("NIN expression run successfully")

	expr = `C[0] in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(4, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("IN expression run successfully")

	expr = `C[0] not in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("NIN expression run successfully")

	expr = `0 <= A < 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(2, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("BinaryRange expression run successfully")

	expr = `100 > A >= 90`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(5, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("BinaryRange expression run successfully")

	expr = `1+5 <= A < 5+10`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(4, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("BinaryRange expression run successfully")

	expr = `A + 5 == 10`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(1, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("Arithmetic expression run successfully")

	expr = `exists A`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("EXISTS expression run successfully")

	expr = `exists AAA`
	checkFunc = func(result *milvuspb.SearchResults) {
		for _, f := range result.Results.GetFieldsData() {
			s.Nil(f)
		}
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("EXISTS expression run successfully")

	expr = `not exists A`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("NOT EXISTS expression run successfully")

	expr = `E["G"] > 100`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(9, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("nested path expression run successfully")

	expr = `D like "name-%"`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("like expression run successfully")

	expr = `D like "name-11"`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(1, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("like expression run successfully")

	expr = `A like "10"`
	checkFunc = func(result *milvuspb.SearchResults) {
		for _, f := range result.Results.GetFieldsData() {
			s.Nil(f)
		}
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("like expression run successfully")

	expr = `str1 like 'abc\"def-%'`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("like expression run successfully")

	expr = `str1 like 'abc"def-%'`
	checkFunc = func(result *milvuspb.SearchResults) {
		for _, f := range result.Results.GetFieldsData() {
			s.Nil(f)
		}
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("like expression run successfully")

	expr = `str2 like 'abc\"def-%'`
	checkFunc = func(result *milvuspb.SearchResults) {
		for _, f := range result.Results.GetFieldsData() {
			s.Nil(f)
		}
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("like expression run successfully")

	expr = `str2 like 'abc"def-%'`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("like expression run successfully")

	expr = `A in []`
	checkFunc = func(result *milvuspb.SearchResults) {
		for _, f := range result.Results.GetFieldsData() {
			s.Nil(f)
		}
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("term empty expression run successfully")

	expr = `A not in []`
	checkFunc = func(result *milvuspb.SearchResults) {
		s.Equal(1, len(result.Results.FieldsData))
		s.Equal(fieldName, result.Results.FieldsData[0].GetFieldName())
		s.Equal(schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		s.Equal(10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	s.doSearch(collectionName, []string{fieldName}, expr, dim, checkFunc)
	log.Info("term empty expression run successfully")

	// invalid expr
	expr = `E[F] > 100`
	s.doSearchWithInvalidExpr(collectionName, []string{fieldName}, expr, dim)

	expr = `A >> 10`
	s.doSearchWithInvalidExpr(collectionName, []string{fieldName}, expr, dim)

	expr = `not A > 5`
	s.doSearchWithInvalidExpr(collectionName, []string{fieldName}, expr, dim)

	expr = `not A == 5`
	s.doSearchWithInvalidExpr(collectionName, []string{fieldName}, expr, dim)

	expr = `A > B`
	s.doSearchWithInvalidExpr(collectionName, []string{fieldName}, expr, dim)

	expr = `A > Int64Field`
	s.doSearchWithInvalidExpr(collectionName, []string{fieldName}, expr, dim)

	expr = `A like abc`
	s.doSearchWithInvalidExpr(collectionName, []string{fieldName}, expr, dim)

	expr = `D like "%name-%"`
	s.doSearchWithInvalidExpr(collectionName, []string{fieldName}, expr, dim)

	expr = `D like "na%me"`
	s.doSearchWithInvalidExpr(collectionName, []string{fieldName}, expr, dim)

	expr = `1+5 <= A+1 < 5+10`
	s.doSearchWithInvalidExpr(collectionName, []string{fieldName}, expr, dim)

	expr = `$meta == ""`
	s.doSearchWithInvalidExpr(collectionName, []string{fieldName}, expr, dim)
}

func (s *JSONExprSuite) insertFlushIndexLoad(ctx context.Context, dbName, collectionName string, rowNum int, dim int, fieldData []*schemapb.FieldData) {
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := s.Cluster.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     fieldData,
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := s.Cluster.Proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	ids := segmentIDs.GetData()
	s.NotEmpty(segmentIDs)

	segments, err := s.Cluster.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}

	if has && len(ids) > 0 {
		flushed := func() bool {
			resp, err := s.Cluster.Proxy.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
				SegmentIDs: ids,
			})
			if err != nil {
				//panic(errors.New("GetFlushState failed"))
				return false
			}
			return resp.GetFlushed()
		}
		for !flushed() {
			// respect context deadline/cancel
			select {
			case <-ctx.Done():
				panic(errors.New("deadline exceeded"))
			default:
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	// create index
	createIndexStatus, err := s.Cluster.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(dim),
			},
			{
				Key:   common.MetricTypeKey,
				Value: distance.L2,
			},
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
			{
				Key:   "nlist",
				Value: strconv.Itoa(10),
			},
		},
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load
	loadStatus, err := s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	if loadStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("loadStatus fail reason", zap.String("reason", loadStatus.GetReason()))
	}
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	for {
		loadProgress, err := s.Cluster.Proxy.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
			CollectionName: collectionName,
		})
		if err != nil {
			panic("GetLoadingProgress fail")
		}
		if loadProgress.GetProgress() == 100 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *JSONExprSuite) doSearch(collectionName string, outputField []string, expr string, dim int, checkFunc func(results *milvuspb.SearchResults)) {
	nq := 1
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, distance.L2)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, outputField, distance.L2, params, nq, dim, topk, roundDecimal)

	searchResult, err := s.Cluster.Proxy.Search(context.Background(), searchReq)

	if searchResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("searchResult fail reason", zap.String("reason", searchResult.GetStatus().GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())

	log.Info("TestHelloMilvus succeed", zap.Any("result", searchResult.Results),
		zap.Any("result num", len(searchResult.Results.FieldsData)))
	for _, data := range searchResult.Results.FieldsData {
		log.Info("output field", zap.Any("outputfield", data.String()))
	}
	checkFunc(searchResult)
}

func newJSONData(fieldName string, rowNum int) *schemapb.FieldData {
	jsonData := make([][]byte, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		data := map[string]interface{}{
			"A": i,
			"B": rowNum - i,
			"C": []int{i, rowNum - i},
			"D": fmt.Sprintf("name-%d", i),
			"E": map[string]interface{}{
				"F": i,
				"G": i + 10,
			},
			"str1": `abc\"def-` + string(rune(i)),
			"str2": fmt.Sprintf("abc\"def-%d", i),
		}
		if i%2 == 0 {
			data = map[string]interface{}{
				"B": rowNum - i,
				"C": []int{i, rowNum - i},
				"D": fmt.Sprintf("name-%d", i),
				"E": map[string]interface{}{
					"F": i,
					"G": i + 10,
				},
			}
		}
		if i == 100 {
			data = nil
		}
		jsonBytes, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			return nil
		}
		jsonData = append(jsonData, jsonBytes)
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: jsonData,
					},
				},
			},
		},
	}
}

func (s *JSONExprSuite) doSearchWithInvalidExpr(collectionName string, outputField []string, expr string, dim int) {
	nq := 1
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, distance.L2)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, outputField, distance.L2, params, nq, dim, topk, roundDecimal)

	searchResult, err := s.Cluster.Proxy.Search(context.Background(), searchReq)

	if searchResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("searchResult fail reason", zap.String("reason", searchResult.GetStatus().GetReason()))
	}
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())
}

func TestJsonExpr(t *testing.T) {
	suite.Run(t, new(JSONExprSuite))
}
