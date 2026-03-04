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

// JSONBench 1M benchmark for Milvus SQL Parser.
//
// Usage:
//
//	cd tests/e2e
//	go test -v -run TestJSONBench1M -milvus-addr=localhost:19530 \
//	    -bluesky-data=$HOME/data/bluesky/file_0001.json.gz -timeout 30m
package e2e

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var blueskyDataFile = flag.String("bluesky-data", "", "Path to bluesky file_0001.json.gz")

const (
	benchCollName = "bluesky"
	insertBatch   = 5000
)

// ---------- JSONBench queries (Milvus SQL dialect) ----------

// Q1: Top event types
var benchQ1 = `SELECT data->'commit'->>'collection' AS event, count(*) AS count
FROM bluesky
GROUP BY event
ORDER BY count DESC`

// Q2: Top event types with unique users (uses COUNT DISTINCT)
var benchQ2 = `SELECT data->'commit'->>'collection' AS event, count(*) AS count,
       count(DISTINCT data->>'did') AS users
FROM bluesky
WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create'
GROUP BY event
ORDER BY count DESC`

// Q3: When do people use BlueSky (temporal analysis)
var benchQ3 = `SELECT data->'commit'->>'collection' AS event,
       extract(hour FROM to_timestamp((data->>'time_us')::bigint / 1000000.0)) AS hour_of_day,
       count(*) AS count
FROM bluesky
WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create'
  AND data->'commit'->>'collection' IN ('app.bsky.feed.post', 'app.bsky.feed.repost',
                                         'app.bsky.feed.like')
GROUP BY event, hour_of_day
ORDER BY hour_of_day, event`

// Q4: Top 3 earliest posters
var benchQ4 = `SELECT (data->>'did')::text AS user_id,
       min(to_timestamp((data->>'time_us')::bigint / 1000000.0)) AS first_post_ts
FROM bluesky
WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create'
  AND data->'commit'->>'collection' = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY first_post_ts ASC LIMIT 3`

// Q5: Top 3 users with longest activity span
var benchQ5 = `SELECT (data->>'did')::text AS user_id,
       extract(epoch FROM (
           max(to_timestamp((data->>'time_us')::bigint / 1000000.0)) -
           min(to_timestamp((data->>'time_us')::bigint / 1000000.0))
       )) * 1000 AS activity_span
FROM bluesky
WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create'
  AND data->'commit'->>'collection' = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC LIMIT 3`

// ---------- data loading ----------

func loadBlueskyData(t *testing.T, dataPath string) {
	t.Helper()

	// Check if collection already has data
	resp := postJSON(t, "/entities/sql_query", map[string]any{
		"sql": "SELECT count(*) AS cnt FROM bluesky",
	})
	if resp.Code == 0 {
		var rows []map[string]any
		if json.Unmarshal(resp.Data, &rows) == nil && len(rows) > 0 {
			if cnt, ok := rows[0]["cnt"]; ok {
				t.Logf("bluesky collection already has data: count=%v, skipping load", cnt)
				return
			}
		}
	}

	// Drop and recreate
	postJSON(t, "/collections/drop", map[string]any{"collectionName": benchCollName})
	time.Sleep(1 * time.Second)

	// Create collection with JSON field + dummy vector (Milvus requires at least one vector field)
	resp = postJSON(t, "/collections/create", map[string]any{
		"collectionName": benchCollName,
		"schema": map[string]any{
			"autoId": true,
			"fields": []map[string]any{
				{"fieldName": "pk", "dataType": "Int64", "isPrimary": true, "autoID": true},
				{"fieldName": "data", "dataType": "JSON"},
				{"fieldName": "dummy_vec", "dataType": "FloatVector", "elementTypeParams": map[string]any{"dim": 2}},
			},
		},
	})
	require.Equal(t, 0, resp.Code, "create collection failed: %s", resp.Message)
	t.Log("collection created")

	// Open gzip file
	f, err := os.Open(dataPath)
	require.NoError(t, err)
	defer f.Close()

	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB buffer per line

	batch := make([]map[string]any, 0, insertBatch)
	total := 0
	start := time.Now()

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Parse the raw JSON into a map to use as the "data" field value
		var doc map[string]any
		if err := json.Unmarshal(line, &doc); err != nil {
			continue // skip malformed lines
		}

		batch = append(batch, map[string]any{
			"data":      doc,
			"dummy_vec": []float32{0.0, 0.0},
		})

		if len(batch) >= insertBatch {
			resp := postJSON(t, "/entities/insert", map[string]any{
				"collectionName": benchCollName,
				"data":           batch,
			})
			require.Equal(t, 0, resp.Code, "insert batch failed at row %d: %s", total, resp.Message)
			total += len(batch)
			if total%50000 == 0 {
				elapsed := time.Since(start)
				rate := float64(total) / elapsed.Seconds()
				t.Logf("  loaded %d rows (%.0f rows/s)", total, rate)
			}
			batch = batch[:0]
		}
	}

	// Insert remaining
	if len(batch) > 0 {
		resp := postJSON(t, "/entities/insert", map[string]any{
			"collectionName": benchCollName,
			"data":           batch,
		})
		require.Equal(t, 0, resp.Code, "insert final batch failed: %s", resp.Message)
		total += len(batch)
	}

	elapsed := time.Since(start)
	t.Logf("loaded %d rows in %v (%.0f rows/s)", total, elapsed.Round(time.Second), float64(total)/elapsed.Seconds())

	// Wait for data to be sealed (no explicit flush API in v2 REST)
	t.Log("waiting for data to be sealed...")
	time.Sleep(5 * time.Second)

	// Create index on dummy vector field (required before load)
	resp = postJSON(t, "/indexes/create", map[string]any{
		"collectionName": benchCollName,
		"indexParams": []map[string]any{
			{
				"fieldName":  "dummy_vec",
				"indexName":  "dummy_idx",
				"metricType": "L2",
				"indexType":  "FLAT",
			},
		},
	})
	t.Logf("create index: code=%d msg=%s", resp.Code, resp.Message)

	// Load collection
	resp = postJSON(t, "/collections/load", map[string]any{
		"collectionName": benchCollName,
	})
	require.Equal(t, 0, resp.Code, "load collection failed: %s", resp.Message)

	// Wait for load
	for i := 0; i < 120; i++ {
		resp = postJSON(t, "/collections/get_load_state", map[string]any{
			"collectionName": benchCollName,
		})
		if resp.Code == 0 {
			var state map[string]any
			if json.Unmarshal(resp.Data, &state) == nil {
				if s, ok := state["loadState"].(string); ok && s == "LoadStateLoaded" {
					t.Log("collection loaded and ready")
					return
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("timeout waiting for collection to load")
}

// ---------- benchmark runner ----------

type queryResult struct {
	name    string
	sql     string
	cold    time.Duration
	hot1    time.Duration
	hot2    time.Duration
	rows    int
	err     string
	sample  string // first row as JSON
}

func runBenchQuery(t *testing.T, name, sql string) queryResult {
	t.Helper()
	result := queryResult{name: name, sql: sql}

	run := func() (time.Duration, int, string, string) {
		start := time.Now()

		payload, _ := json.Marshal(map[string]any{"sql": sql})
		req, err := http.NewRequest(http.MethodPost, baseURL()+"/entities/sql_query", strings.NewReader(string(payload)))
		if err != nil {
			return time.Since(start), 0, "", err.Error()
		}
		req.Header.Set("Content-Type", "application/json")

		client := http.Client{Timeout: 5 * time.Minute}
		resp, err := client.Do(req)
		if err != nil {
			return time.Since(start), 0, "", err.Error()
		}
		defer resp.Body.Close()

		raw, _ := io.ReadAll(resp.Body)
		elapsed := time.Since(start)

		var apiResp struct {
			Code    int             `json:"code"`
			Data    json.RawMessage `json:"data"`
			Message string          `json:"message"`
		}
		if err := json.Unmarshal(raw, &apiResp); err != nil {
			return elapsed, 0, "", fmt.Sprintf("parse response: %v", err)
		}
		if apiResp.Code != 0 {
			return elapsed, 0, "", apiResp.Message
		}

		var rows []map[string]any
		if err := json.Unmarshal(apiResp.Data, &rows); err != nil {
			return elapsed, 0, "", fmt.Sprintf("parse data: %v", err)
		}

		sample := ""
		if len(rows) > 0 {
			bs, _ := json.Marshal(rows[0])
			sample = string(bs)
		}
		return elapsed, len(rows), sample, ""
	}

	// Cold run
	result.cold, result.rows, result.sample, result.err = run()
	if result.err != "" {
		return result
	}
	// Hot run 1
	result.hot1, _, _, result.err = run()
	if result.err != "" {
		return result
	}
	// Hot run 2
	result.hot2, _, _, _ = run()

	return result
}

// ---------- main test ----------

func TestJSONBench1M(t *testing.T) {
	if *blueskyDataFile == "" {
		t.Skip("skip: -bluesky-data not provided")
	}

	// Load data
	t.Log("=== Loading 1M bluesky data ===")
	loadBlueskyData(t, *blueskyDataFile)

	// Define queries to run
	queries := []struct {
		name string
		sql  string
	}{
		{"Q1", benchQ1},
		{"Q2", benchQ2},
		{"Q3", benchQ3},
		{"Q4", benchQ4},
		{"Q5", benchQ5},
	}

	// Run all queries
	t.Log("\n=== Running JSONBench Queries ===")
	results := make([]queryResult, 0, len(queries))
	for _, q := range queries {
		t.Logf("\n--- %s ---", q.name)
		t.Logf("SQL: %s", q.sql[:min(len(q.sql), 100)]+"...")
		r := runBenchQuery(t, q.name, q.sql)
		results = append(results, r)

		if r.err != "" {
			t.Logf("  FAILED: %s", r.err)
		} else {
			t.Logf("  Rows:  %d", r.rows)
			t.Logf("  Cold:  %v", r.cold.Round(time.Millisecond))
			t.Logf("  Hot1:  %v", r.hot1.Round(time.Millisecond))
			t.Logf("  Hot2:  %v", r.hot2.Round(time.Millisecond))
			if r.sample != "" {
				t.Logf("  Row[0]: %s", r.sample)
			}
		}
	}

	// Print summary table
	t.Log("\n=== JSONBench 1M Results Summary ===")
	t.Logf("%-4s  %-10s  %-10s  %-10s  %-6s  %s", "Q", "Cold", "Hot1", "Hot2", "Rows", "Status")
	t.Logf("%-4s  %-10s  %-10s  %-10s  %-6s  %s", "----", "----------", "----------", "----------", "------", "------")
	for _, r := range results {
		if r.err != "" {
			t.Logf("%-4s  %-10s  %-10s  %-10s  %-6s  %s", r.name, "-", "-", "-", "-", "FAIL: "+r.err[:min(len(r.err), 60)])
		} else {
			t.Logf("%-4s  %-10v  %-10v  %-10v  %-6d  %s", r.name,
				r.cold.Round(time.Millisecond),
				r.hot1.Round(time.Millisecond),
				r.hot2.Round(time.Millisecond),
				r.rows, "OK")
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
