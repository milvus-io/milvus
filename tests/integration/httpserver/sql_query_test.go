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

package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	SqlTestPort = 40002
	sqlDim      = 128
	sqlRowNum   = 100
)

// sqlResp is the common HTTP response envelope for the v2 API.
type sqlResp struct {
	Code    int             `json:"code"`
	Data    json.RawMessage `json:"data"`
	Message string          `json:"message"`
}

// SqlQuerySuite tests SQL query functionality end-to-end.
// Data is prepared via gRPC (create/insert/flush/index/load),
// then SQL queries are sent via the REST API endpoint.
type SqlQuerySuite struct {
	integration.MiniClusterSuite
	baseURL        string
	collectionName string
}

func (s *SqlQuerySuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().HTTPCfg.Port.Key, fmt.Sprintf("%d", SqlTestPort))
	s.MiniClusterSuite.SetupSuite()
	s.baseURL = "http://localhost:" + strconv.Itoa(SqlTestPort) + "/v2/vectordb"
	s.collectionName = "test_sql_" + funcutil.GenRandomStr()
}

// ---------- helpers ----------

// postJSON sends a POST request and returns the parsed response.
func (s *SqlQuerySuite) postJSON(path string, body any) sqlResp {
	payload, err := json.Marshal(body)
	s.Require().NoError(err)
	req, err := http.NewRequest(http.MethodPost, s.baseURL+path, bytes.NewBuffer(payload))
	s.Require().NoError(err)
	req.Header.Set("Content-Type", "application/json")

	client := http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	s.Require().NoError(err)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	s.Require().NoError(err)

	var result sqlResp
	err = json.Unmarshal(raw, &result)
	s.Require().NoError(err, "response body: %s", string(raw))
	return result
}

// sqlQuery sends a SQL query via REST and returns the response.
func (s *SqlQuerySuite) sqlQuery(sql string) sqlResp {
	body := map[string]any{"sql": sql}
	result := s.postJSON(httpserver.EntityCategory+httpserver.SqlQueryAction, body)
	log.Info("sql_query", zap.String("sql", sql), zap.Int("code", result.Code), zap.String("msg", result.Message))
	return result
}

// sqlQueryRows sends a SQL query and parses the data as []map[string]any.
func (s *SqlQuerySuite) sqlQueryRows(sql string) []map[string]any {
	resp := s.sqlQuery(sql)
	s.Require().Equal(0, resp.Code, "sql query failed: %s", resp.Message)
	var rows []map[string]any
	err := json.Unmarshal(resp.Data, &rows)
	s.Require().NoError(err)
	return rows
}

// prepareCollection creates a collection via gRPC with the following schema:
//
//	pk        Int64  (primary key, auto ID)
//	age       Int64
//	name      VarChar(256)
//	score     Float
//	data      JSON
//	embedding FloatVector(128)
//
// Then inserts sqlRowNum rows, flushes, creates vector index, and loads.
func (s *SqlQuerySuite) prepareCollection() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	collName := s.collectionName

	// --- schema ---
	pk := &schemapb.FieldSchema{
		FieldID: 100, Name: "pk", IsPrimaryKey: true,
		DataType: schemapb.DataType_Int64, AutoID: true,
	}
	ageField := &schemapb.FieldSchema{
		FieldID: 101, Name: "age", DataType: schemapb.DataType_Int64,
	}
	nameField := &schemapb.FieldSchema{
		FieldID: 102, Name: "name", DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "256"}},
	}
	scoreField := &schemapb.FieldSchema{
		FieldID: 103, Name: "score", DataType: schemapb.DataType_Float,
	}
	jsonField := &schemapb.FieldSchema{
		FieldID: 104, Name: "data", DataType: schemapb.DataType_JSON,
	}
	vecField := &schemapb.FieldSchema{
		FieldID: 105, Name: "embedding", DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(sqlDim)}},
	}
	schema := &schemapb.CollectionSchema{
		Name:   collName,
		Fields: []*schemapb.FieldSchema{pk, ageField, nameField, scoreField, jsonField, vecField},
	}
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	// --- create collection ---
	createResp, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(createResp))

	// --- insert data ---
	rng := rand.New(rand.NewSource(42))
	categories := []string{"electronics", "books", "clothing", "food", "sports"}

	ages := make([]int64, sqlRowNum)
	names := make([]string, sqlRowNum)
	scores := make([]float32, sqlRowNum)
	jsonRows := make([][]byte, sqlRowNum)
	vectors := make([]float32, sqlRowNum*sqlDim)

	for i := 0; i < sqlRowNum; i++ {
		ages[i] = int64(20 + (i % 50))
		names[i] = fmt.Sprintf("user_%d", i)
		scores[i] = float32(i) * 1.5
		jdata := map[string]any{
			"category": categories[i%len(categories)],
			"tags":     []string{"tag_a", "tag_b"},
			"info":     map[string]any{"level": i % 5},
		}
		jb, _ := json.Marshal(jdata)
		jsonRows[i] = jb
		for j := 0; j < sqlDim; j++ {
			vectors[i*sqlDim+j] = rng.Float32()
		}
	}

	fieldsData := []*schemapb.FieldData{
		{Type: schemapb.DataType_Int64, FieldName: "age", Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ages}}}}},
		{Type: schemapb.DataType_VarChar, FieldName: "name", Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: names}}}}},
		{Type: schemapb.DataType_Float, FieldName: "score", Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: scores}}}}},
		{Type: schemapb.DataType_JSON, FieldName: "data", Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: jsonRows}}}}},
		{Type: schemapb.DataType_FloatVector, FieldName: "embedding", Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{Dim: sqlDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vectors}}}}},
	}
	hashKeys := integration.GenerateHashKeys(sqlRowNum)
	insertResp, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collName,
		FieldsData:     fieldsData,
		HashKeys:       hashKeys,
		NumRows:        uint32(sqlRowNum),
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(insertResp.GetStatus()))
	log.Info("inserted rows", zap.Int("count", sqlRowNum))

	// --- flush ---
	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{collName},
	})
	s.Require().NoError(err)
	segIDs := flushResp.GetCollSegIDs()[collName].GetData()
	flushTs := flushResp.GetCollFlushTs()[collName]
	s.WaitForFlush(ctx, segIDs, flushTs, "", collName)

	// --- create vector index ---
	createIdxResp, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collName,
		FieldName:      "embedding",
		IndexName:      "_default",
		ExtraParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: strconv.Itoa(sqlDim)},
			{Key: common.MetricTypeKey, Value: metric.L2},
			{Key: common.IndexTypeKey, Value: "IVF_FLAT"},
			{Key: "nlist", Value: "10"},
		},
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(createIdxResp))
	s.WaitForIndexBuilt(ctx, collName, "embedding")

	// --- load ---
	loadResp, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collName,
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(loadResp))
	s.WaitForLoad(ctx, collName)
	log.Info("collection ready", zap.String("collection", collName))
}

// ======================== Test Cases ========================

// TestSimpleSelect tests basic SELECT with WHERE and LIMIT.
func (s *SqlQuerySuite) TestSimpleSelect() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf("SELECT pk, age, name FROM %s WHERE age > 30 LIMIT 10", s.collectionName))
	s.LessOrEqual(len(rows), 10)
	for _, row := range rows {
		age, _ := row["age"].(float64)
		s.Greater(age, float64(30))
	}
}

// TestSelectStar tests SELECT * returns all scalar fields.
func (s *SqlQuerySuite) TestSelectStar() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf("SELECT * FROM %s LIMIT 5", s.collectionName))
	s.LessOrEqual(len(rows), 5)
	if len(rows) > 0 {
		_, hasPK := rows[0]["pk"]
		_, hasAge := rows[0]["age"]
		s.True(hasPK, "SELECT * should include pk")
		s.True(hasAge, "SELECT * should include age")
	}
}

// TestJSONFieldAccess tests JSON field access with ->> operator.
func (s *SqlQuerySuite) TestJSONFieldAccess() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf(
		`SELECT pk, data->>'category' AS category FROM %s WHERE data->>'category' = 'books' LIMIT 10`,
		s.collectionName))
	s.Greater(len(rows), 0)
	for _, row := range rows {
		cat, _ := row["category"].(string)
		s.Equal("books", cat)
	}
}

// TestNestedJSONAccess tests multi-level JSON: data->'info'->>'level'.
func (s *SqlQuerySuite) TestNestedJSONAccess() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf(
		`SELECT pk, data->'info'->>'level' AS level FROM %s WHERE data->'info'->>'level' = '0' LIMIT 10`,
		s.collectionName))
	s.Greater(len(rows), 0)
}

// TestCountStar tests COUNT(*) aggregation.
func (s *SqlQuerySuite) TestCountStar() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf("SELECT count(*) FROM %s", s.collectionName))
	s.Len(rows, 1)
}

// TestCountWithWhere tests COUNT(*) with a WHERE filter.
func (s *SqlQuerySuite) TestCountWithWhere() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf("SELECT count(*) FROM %s WHERE age >= 40", s.collectionName))
	s.Len(rows, 1)
}

// TestSumAvg tests SUM and AVG aggregation.
func (s *SqlQuerySuite) TestSumAvg() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf("SELECT sum(score), avg(score) FROM %s", s.collectionName))
	s.Len(rows, 1)
}

// TestMinMax tests MIN and MAX aggregation.
func (s *SqlQuerySuite) TestMinMax() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf("SELECT min(age), max(age) FROM %s", s.collectionName))
	s.Len(rows, 1)
}

// TestGroupBy tests aggregate with GROUP BY on a JSON field.
func (s *SqlQuerySuite) TestGroupBy() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf(
		`SELECT data->>'category' AS category, count(*) FROM %s GROUP BY data->>'category'`,
		s.collectionName))
	s.Greater(len(rows), 0)
	s.LessOrEqual(len(rows), 5) // 5 categories
}

// TestOrderByLimit tests ORDER BY with LIMIT.
func (s *SqlQuerySuite) TestOrderByLimit() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf(
		"SELECT pk, age, score FROM %s ORDER BY score DESC LIMIT 5", s.collectionName))
	s.LessOrEqual(len(rows), 5)
	for i := 1; i < len(rows); i++ {
		prev, _ := rows[i-1]["score"].(float64)
		curr, _ := rows[i]["score"].(float64)
		s.GreaterOrEqual(prev, curr, "should be ordered by score DESC")
	}
}

// TestWhereComplex tests complex WHERE with AND/OR.
func (s *SqlQuerySuite) TestWhereComplex() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf(
		"SELECT pk, age, name FROM %s WHERE (age > 30 AND score < 100) OR name = 'user_0' LIMIT 20",
		s.collectionName))
	s.Greater(len(rows), 0)
}

// TestWhereIn tests WHERE ... IN (...) syntax.
func (s *SqlQuerySuite) TestWhereIn() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf(
		"SELECT pk, name FROM %s WHERE name IN ('user_0', 'user_1', 'user_2') LIMIT 10",
		s.collectionName))
	s.Greater(len(rows), 0)
}

// TestWhereBetween tests WHERE ... BETWEEN ... AND ... syntax.
func (s *SqlQuerySuite) TestWhereBetween() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf(
		"SELECT pk, age FROM %s WHERE age BETWEEN 25 AND 35 LIMIT 20", s.collectionName))
	for _, row := range rows {
		age, _ := row["age"].(float64)
		s.True(age >= 25 && age <= 35, "age %v not in BETWEEN range", age)
	}
}

// TestWhereLike tests WHERE ... LIKE '...' syntax.
func (s *SqlQuerySuite) TestWhereLike() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf(
		`SELECT pk, name FROM %s WHERE name LIKE 'user_1%%' LIMIT 20`, s.collectionName))
	s.Greater(len(rows), 0, "LIKE 'user_1%%' should match user_1, user_10..user_19")
}

// TestVectorSearch tests vector search via SQL with the <-> distance operator.
func (s *SqlQuerySuite) TestVectorSearch() {
	s.prepareCollection()

	rng := rand.New(rand.NewSource(99))
	vec := make([]float32, sqlDim)
	for i := range vec {
		vec[i] = rng.Float32()
	}
	vecStr := "["
	for i, v := range vec {
		if i > 0 {
			vecStr += ","
		}
		vecStr += fmt.Sprintf("%f", v)
	}
	vecStr += "]"

	rows := s.sqlQueryRows(fmt.Sprintf(
		`SELECT pk, embedding <-> '%s'::vector AS distance FROM %s ORDER BY distance LIMIT 5`,
		vecStr, s.collectionName))
	s.LessOrEqual(len(rows), 5)
}

// TestInvalidSQL tests error handling for invalid SQL.
func (s *SqlQuerySuite) TestInvalidSQL() {
	resp := s.sqlQuery("THIS IS NOT SQL")
	s.NotEqual(0, resp.Code, "invalid SQL should return error")
}

// TestEmptyResult tests a query that matches nothing.
func (s *SqlQuerySuite) TestEmptyResult() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf(
		"SELECT pk FROM %s WHERE age > 99999 LIMIT 10", s.collectionName))
	s.Len(rows, 0)
}

// TestCountDistinct tests COUNT(DISTINCT field).
func (s *SqlQuerySuite) TestCountDistinct() {
	s.prepareCollection()

	rows := s.sqlQueryRows(fmt.Sprintf("SELECT count(DISTINCT age) FROM %s", s.collectionName))
	s.Len(rows, 1)
}

func TestSqlQuery(t *testing.T) {
	suite.Run(t, new(SqlQuerySuite))
}
