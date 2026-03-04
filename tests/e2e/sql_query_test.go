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

// Package e2e provides standalone E2E tests that connect to an external
// Milvus instance via the REST API. No embedded cluster is started.
//
// Usage:
//
//	# Start Milvus externally, then:
//	cd tests/e2e
//	go test -v -run TestSqlQuery -milvus-addr=localhost:19530
package e2e

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var milvusAddr = flag.String("milvus-addr", "localhost:19530", "Milvus REST API address (host:port)")

// ---------- helpers ----------

type apiResp struct {
	Code    int             `json:"code"`
	Data    json.RawMessage `json:"data"`
	Message string          `json:"message"`
}

func baseURL() string {
	return "http://" + *milvusAddr + "/v2/vectordb"
}

func postJSON(t *testing.T, path string, body any) apiResp {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, baseURL()+path, bytes.NewBuffer(payload))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	client := http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var result apiResp
	err = json.Unmarshal(raw, &result)
	require.NoError(t, err, "body: %s", string(raw))
	return result
}

func sqlQuery(t *testing.T, sql string) apiResp {
	t.Helper()
	return postJSON(t, "/entities/sql_query", map[string]any{"sql": sql})
}

func sqlQueryRows(t *testing.T, sql string) []map[string]any {
	t.Helper()
	resp := sqlQuery(t, sql)
	require.Equal(t, 0, resp.Code, "sql query failed: %s (sql: %s)", resp.Message, sql)
	var rows []map[string]any
	require.NoError(t, json.Unmarshal(resp.Data, &rows))
	return rows
}

// ---------- test data setup ----------

const (
	collName = "test_sql_e2e"
	dim      = 128
	rowCount = 100
)

// setupCollection creates the collection, inserts data, creates index, and loads.
// It uses the REST API only — no gRPC dependency.
func setupCollection(t *testing.T) {
	t.Helper()

	// Drop if exists (ignore error)
	postJSON(t, "/collections/drop", map[string]any{
		"collectionName": collName,
	})

	// Create collection
	resp := postJSON(t, "/collections/create", map[string]any{
		"collectionName": collName,
		"schema": map[string]any{
			"fields": []map[string]any{
				{"fieldName": "pk", "dataType": "Int64", "isPrimary": true},
				{"fieldName": "age", "dataType": "Int64"},
				{"fieldName": "name", "dataType": "VarChar", "elementTypeParams": map[string]any{"max_length": 256}},
				{"fieldName": "score", "dataType": "Float"},
				{"fieldName": "data", "dataType": "JSON"},
				{"fieldName": "embedding", "dataType": "FloatVector", "elementTypeParams": map[string]any{"dim": dim}},
			},
		},
	})
	require.Equal(t, 0, resp.Code, "create collection failed: %s", resp.Message)

	// Insert data
	rng := rand.New(rand.NewSource(42))
	categories := []string{"electronics", "books", "clothing", "food", "sports"}
	rows := make([]map[string]any, rowCount)
	for i := 0; i < rowCount; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()
		}
		rows[i] = map[string]any{
			"pk":    int64(i),
			"age":   20 + (i % 50),
			"name":  fmt.Sprintf("user_%d", i),
			"score": float64(i) * 1.5,
			"data": map[string]any{
				"category": categories[i%len(categories)],
				"tags":     []string{"tag_a", "tag_b"},
				"info":     map[string]any{"level": i % 5},
			},
			"embedding": vec,
		}
	}
	resp = postJSON(t, "/entities/insert", map[string]any{
		"collectionName": collName,
		"data":           rows,
	})
	require.Equal(t, 0, resp.Code, "insert failed: %s", resp.Message)

	// Create index
	resp = postJSON(t, "/indexes/create", map[string]any{
		"collectionName": collName,
		"indexParams": []map[string]any{
			{
				"fieldName":  "embedding",
				"indexName":  "vec_idx",
				"metricType": "L2",
				"indexType":  "IVF_FLAT",
				"params":     map[string]any{"nlist": 10},
			},
		},
	})
	require.Equal(t, 0, resp.Code, "create index failed: %s", resp.Message)

	// Load collection
	resp = postJSON(t, "/collections/load", map[string]any{
		"collectionName": collName,
	})
	require.Equal(t, 0, resp.Code, "load collection failed: %s", resp.Message)

	// Wait for load to complete
	for i := 0; i < 60; i++ {
		resp = postJSON(t, "/collections/get_load_state", map[string]any{
			"collectionName": collName,
		})
		if resp.Code == 0 {
			var state map[string]any
			if json.Unmarshal(resp.Data, &state) == nil {
				if s, ok := state["loadState"].(string); ok && s == "LoadStateLoaded" {
					t.Log("collection loaded")
					return
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("timeout waiting for collection to load")
}

// cleanupCollection drops the test collection.
func cleanupCollection(t *testing.T) {
	t.Helper()
	postJSON(t, "/collections/drop", map[string]any{
		"collectionName": collName,
	})
}

// ---------- test cases ----------

func TestSqlQuery(t *testing.T) {
	setupCollection(t)
	defer cleanupCollection(t)

	t.Run("SimpleSelect", func(t *testing.T) {
		rows := sqlQueryRows(t, fmt.Sprintf("SELECT pk, age, name FROM %s WHERE age > 30 LIMIT 10", collName))
		assert.LessOrEqual(t, len(rows), 10)
		for _, row := range rows {
			age, _ := row["age"].(float64)
			assert.Greater(t, age, float64(30))
		}
	})

	t.Run("SelectStar", func(t *testing.T) {
		rows := sqlQueryRows(t, fmt.Sprintf("SELECT * FROM %s LIMIT 5", collName))
		assert.LessOrEqual(t, len(rows), 5)
		if len(rows) > 0 {
			_, hasPK := rows[0]["pk"]
			_, hasAge := rows[0]["age"]
			assert.True(t, hasPK, "SELECT * should include pk")
			assert.True(t, hasAge, "SELECT * should include age")
		}
	})

	t.Run("JSONFieldAccess", func(t *testing.T) {
		sql := fmt.Sprintf(`SELECT pk, data->>'category' AS category FROM %s WHERE data->>'category' = 'books' LIMIT 10`, collName)
		rows := sqlQueryRows(t, sql)
		assert.Greater(t, len(rows), 0)
		for _, row := range rows {
			cat, _ := row["category"].(string)
			assert.Equal(t, "books", cat)
		}
	})

	t.Run("NestedJSONAccess", func(t *testing.T) {
		sql := fmt.Sprintf(`SELECT pk, data->'info'->>'level' AS level FROM %s WHERE data->'info'->>'level' = '0' LIMIT 10`, collName)
		rows := sqlQueryRows(t, sql)
		assert.Greater(t, len(rows), 0)
	})

	t.Run("CountStar", func(t *testing.T) {
		rows := sqlQueryRows(t, fmt.Sprintf("SELECT count(*) FROM %s", collName))
		assert.Len(t, rows, 1)
	})

	t.Run("CountWithWhere", func(t *testing.T) {
		rows := sqlQueryRows(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE age >= 40", collName))
		assert.Len(t, rows, 1)
	})

	t.Run("SumAvg", func(t *testing.T) {
		rows := sqlQueryRows(t, fmt.Sprintf("SELECT sum(score), avg(score) FROM %s", collName))
		assert.Len(t, rows, 1)
	})

	t.Run("MinMax", func(t *testing.T) {
		rows := sqlQueryRows(t, fmt.Sprintf("SELECT min(age), max(age) FROM %s", collName))
		assert.Len(t, rows, 1)
	})

	t.Run("GroupBy", func(t *testing.T) {
		sql := fmt.Sprintf(`SELECT data->>'category' AS category, count(*) FROM %s GROUP BY data->>'category'`, collName)
		rows := sqlQueryRows(t, sql)
		assert.Greater(t, len(rows), 0)
		assert.LessOrEqual(t, len(rows), 5)
	})

	t.Run("OrderByLimit", func(t *testing.T) {
		rows := sqlQueryRows(t, fmt.Sprintf("SELECT pk, age, score FROM %s ORDER BY score DESC LIMIT 5", collName))
		assert.LessOrEqual(t, len(rows), 5)
		for i := 1; i < len(rows); i++ {
			prev, _ := rows[i-1]["score"].(float64)
			curr, _ := rows[i]["score"].(float64)
			assert.GreaterOrEqual(t, prev, curr, "should be ordered by score DESC")
		}
	})

	t.Run("OrderByAsc", func(t *testing.T) {
		rows := sqlQueryRows(t, fmt.Sprintf("SELECT pk, age FROM %s ORDER BY age ASC LIMIT 10", collName))
		assert.LessOrEqual(t, len(rows), 10)
		for i := 1; i < len(rows); i++ {
			prev, _ := rows[i-1]["age"].(float64)
			curr, _ := rows[i]["age"].(float64)
			assert.LessOrEqual(t, prev, curr, "should be ordered by age ASC")
		}
	})

	t.Run("OrderByWithWhere", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT pk, age, score FROM %s WHERE age > 30 ORDER BY score DESC LIMIT 5", collName)
		rows := sqlQueryRows(t, sql)
		assert.LessOrEqual(t, len(rows), 5)
		for _, row := range rows {
			age, _ := row["age"].(float64)
			assert.Greater(t, age, float64(30))
		}
		for i := 1; i < len(rows); i++ {
			prev, _ := rows[i-1]["score"].(float64)
			curr, _ := rows[i]["score"].(float64)
			assert.GreaterOrEqual(t, prev, curr)
		}
	})

	t.Run("OrderByDefaultAsc", func(t *testing.T) {
		// No direction specified → default ASC
		rows := sqlQueryRows(t, fmt.Sprintf("SELECT pk, score FROM %s ORDER BY score LIMIT 5", collName))
		assert.LessOrEqual(t, len(rows), 5)
		for i := 1; i < len(rows); i++ {
			prev, _ := rows[i-1]["score"].(float64)
			curr, _ := rows[i]["score"].(float64)
			assert.LessOrEqual(t, prev, curr, "default direction should be ASC")
		}
	})

	t.Run("GroupByOrderByCount", func(t *testing.T) {
		sql := fmt.Sprintf(`SELECT data->>'category' AS category, count(*) AS cnt FROM %s GROUP BY data->>'category' ORDER BY cnt DESC`, collName)
		rows := sqlQueryRows(t, sql)
		assert.Greater(t, len(rows), 0)
		for i := 1; i < len(rows); i++ {
			prev, _ := rows[i-1]["cnt"].(float64)
			curr, _ := rows[i]["cnt"].(float64)
			assert.GreaterOrEqual(t, prev, curr, "should be ordered by count DESC")
		}
	})

	t.Run("GroupByPlainFieldOrderByCount", func(t *testing.T) {
		// Use age (Int64) as group key to avoid segcore's JSON-output limitation
		sql := fmt.Sprintf(`SELECT age, count(*) AS cnt FROM %s GROUP BY age ORDER BY cnt DESC LIMIT 10`, collName)
		rows := sqlQueryRows(t, sql)
		assert.Greater(t, len(rows), 0)
		for i := 1; i < len(rows); i++ {
			prev, _ := rows[i-1]["cnt"].(float64)
			curr, _ := rows[i]["cnt"].(float64)
			assert.GreaterOrEqual(t, prev, curr, "should be ordered by count DESC")
		}
	})

	t.Run("GroupByPlainFieldOrderByKey", func(t *testing.T) {
		sql := fmt.Sprintf(`SELECT age, count(*) AS cnt FROM %s GROUP BY age ORDER BY age ASC LIMIT 10`, collName)
		rows := sqlQueryRows(t, sql)
		assert.Greater(t, len(rows), 0)
		for i := 1; i < len(rows); i++ {
			prev, _ := rows[i-1]["age"].(float64)
			curr, _ := rows[i]["age"].(float64)
			assert.LessOrEqual(t, prev, curr, "should be ordered by age ASC")
		}
	})

	t.Run("GroupByOrderByGroupKey", func(t *testing.T) {
		sql := fmt.Sprintf(`SELECT data->>'category' AS category, count(*) AS cnt FROM %s GROUP BY data->>'category' ORDER BY category ASC`, collName)
		rows := sqlQueryRows(t, sql)
		assert.Greater(t, len(rows), 0)
		// Categories should be sorted alphabetically
		for i := 1; i < len(rows); i++ {
			prev, _ := rows[i-1]["category"].(string)
			curr, _ := rows[i]["category"].(string)
			assert.LessOrEqual(t, prev, curr, "categories should be ASC")
		}
	})

	t.Run("WhereComplex", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT pk, age, name FROM %s WHERE (age > 30 AND score < 100) OR name = 'user_0' LIMIT 20", collName)
		rows := sqlQueryRows(t, sql)
		assert.Greater(t, len(rows), 0)
	})

	t.Run("WhereIn", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT pk, name FROM %s WHERE name IN ('user_0', 'user_1', 'user_2') LIMIT 10", collName)
		rows := sqlQueryRows(t, sql)
		assert.Greater(t, len(rows), 0)
	})

	t.Run("WhereBetween", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT pk, age FROM %s WHERE age BETWEEN 25 AND 35 LIMIT 20", collName)
		rows := sqlQueryRows(t, sql)
		for _, row := range rows {
			age, _ := row["age"].(float64)
			assert.True(t, age >= 25 && age <= 35, "age %v not in BETWEEN range", age)
		}
	})

	t.Run("WhereLike", func(t *testing.T) {
		sql := fmt.Sprintf(`SELECT pk, name FROM %s WHERE name LIKE 'user_1%%' LIMIT 20`, collName)
		rows := sqlQueryRows(t, sql)
		assert.Greater(t, len(rows), 0)
	})

	t.Run("VectorSearch", func(t *testing.T) {
		rng := rand.New(rand.NewSource(99))
		vec := make([]float32, dim)
		for i := range vec {
			vec[i] = rng.Float32()
		}
		// Build vector JSON array for searchParams
		vecBytes, _ := json.Marshal(vec)

		body := map[string]any{
			"sql": fmt.Sprintf("SELECT pk, l2_distance(embedding, $1) AS distance FROM %s ORDER BY distance LIMIT 5", collName),
			"searchParams": map[string]string{
				"$1": string(vecBytes),
			},
		}
		resp := postJSON(t, "/entities/sql_query", body)
		assert.Equal(t, 0, resp.Code, "vector search failed: %s", resp.Message)
		var rows []map[string]any
		if resp.Code == 0 {
			_ = json.Unmarshal(resp.Data, &rows)
			assert.LessOrEqual(t, len(rows), 5)
		}
	})

	t.Run("InvalidSQL", func(t *testing.T) {
		resp := sqlQuery(t, "THIS IS NOT SQL")
		assert.NotEqual(t, 0, resp.Code)
	})

	t.Run("EmptyResult", func(t *testing.T) {
		rows := sqlQueryRows(t, fmt.Sprintf("SELECT pk FROM %s WHERE age > 99999 LIMIT 10", collName))
		assert.Len(t, rows, 0)
	})

	t.Run("CountDistinct", func(t *testing.T) {
		rows := sqlQueryRows(t, fmt.Sprintf("SELECT count(DISTINCT age) FROM %s", collName))
		assert.Len(t, rows, 1)
	})
}
