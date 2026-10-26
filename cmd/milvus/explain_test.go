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

package milvus

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// explainServer stands in for the Milvus RESTful API: it records the last
// request and answers with whatever the test set.
type explainServer struct {
	*httptest.Server
	answer   string
	status   int // 0 answers 200
	requests int
	path     string
	auth     string
	body     map[string]json.RawMessage
}

func newExplainServer(t *testing.T) *explainServer {
	s := &explainServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests++
		s.path = r.URL.Path
		s.auth = r.Header.Get("Authorization")
		raw, _ := io.ReadAll(r.Body)
		// a body the server cannot decode leaves nil behind, which no
		// assertion below accepts
		s.body = nil
		_ = json.Unmarshal(raw, &s.body)
		w.Header().Set("Content-Type", "application/json")
		if s.status != 0 {
			w.WriteHeader(s.status)
		}
		_, _ = io.WriteString(w, s.answer)
	}))
	t.Cleanup(s.Close)
	return s
}

const explainTestBody = `{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in {list}", "exprParams": {"list": [9007199254740993]}, "limit": 4}`

func TestExplainCommand(t *testing.T) {
	server := newExplainServer(t)

	t.Run("json", func(t *testing.T) {
		server.answer = `{"code":0,"data":{"version":1,"plan_type":"vector_search","predicates":null}}`
		c := &explain{address: server.URL, token: "root:Milvus", format: "json", file: "-"} //nolint:gosec // G101: the documented default login, a test value
		var stdout bytes.Buffer
		require.NoError(t, c.run(strings.NewReader(explainTestBody), &stdout))

		assert.Equal(t, "/v2/vectordb/entities/explain", server.path)
		assert.Equal(t, "Bearer root:Milvus", server.auth)
		// the body is forwarded with the format added and its values untouched:
		// an integer above 2^53 would not survive a float64 round trip
		assert.JSONEq(t, `"json"`, string(server.body["format"]))
		assert.JSONEq(t, `"book"`, string(server.body["collectionName"]))
		assert.Equal(t, `{"list":[9007199254740993]}`, string(server.body["exprParams"]))
		assert.Equal(t, `[[0.1,0.2]]`, string(server.body["data"]))

		assert.Equal(t, "{\n  \"version\": 1,\n  \"plan_type\": \"vector_search\",\n  \"predicates\": null\n}\n", stdout.String())
	})

	t.Run("text", func(t *testing.T) {
		server.answer = `{"code":0,"data":"plan: vector_search\nsearch: book_intro (FloatVector) metric=COSINE topk=4\n"}`
		// a trailing slash on the address must not double the path's slash
		c := &explain{address: server.URL + "/", format: "text", file: "-"}
		var stdout bytes.Buffer
		// the flag replaces a format key already in the body
		body := strings.TrimSuffix(explainTestBody, "}") + `, "format": "json"}`
		require.NoError(t, c.run(strings.NewReader(body), &stdout))
		assert.Equal(t, "/v2/vectordb/entities/explain", server.path)
		assert.JSONEq(t, `"text"`, string(server.body["format"]))
		assert.Empty(t, server.auth)
		assert.Equal(t, "plan: vector_search\nsearch: book_intro (FloatVector) metric=COSINE topk=4\n", stdout.String())
	})

	t.Run("body from a file", func(t *testing.T) {
		server.answer = `{"code":0,"data":{}}`
		path := filepath.Join(t.TempDir(), "search.json")
		require.NoError(t, os.WriteFile(path, []byte(explainTestBody), 0o600))
		c := &explain{address: server.URL, format: "json", file: path}
		var stdout bytes.Buffer
		require.NoError(t, c.run(strings.NewReader("not read"), &stdout))
		assert.JSONEq(t, `"book"`, string(server.body["collectionName"]))
		assert.Equal(t, "{}\n", stdout.String())
	})

	t.Run("the server's error is reported with its code", func(t *testing.T) {
		server.answer = `{"code":1100,"message":"invalid parameter[expected=valid expression][actual=book_id >]"}`
		c := &explain{address: server.URL, format: "json", file: "-"}
		err := c.run(strings.NewReader(explainTestBody), io.Discard)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "code 1100")
		assert.Contains(t, err.Error(), "actual=book_id >")
	})

	t.Run("a body that is not a JSON object is refused before any request", func(t *testing.T) {
		before := server.requests
		for _, body := range []string{`[1, 2]`, `null`} {
			c := &explain{address: server.URL, format: "json", file: "-"}
			err := c.run(strings.NewReader(body), io.Discard)
			require.Error(t, err, body)
			assert.Contains(t, err.Error(), "must be a JSON object")
			assert.Equal(t, before, server.requests)
		}
	})

	t.Run("a response that is not the API envelope is reported as is", func(t *testing.T) {
		server.answer = `<html>not milvus</html>`
		c := &explain{address: server.URL, format: "json", file: "-"}
		err := c.run(strings.NewReader(explainTestBody), io.Discard)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected response (HTTP 200)")
		assert.Contains(t, err.Error(), "<html>not milvus</html>")
	})

	t.Run("a gateway's JSON error is not taken for a success", func(t *testing.T) {
		server.status = http.StatusBadGateway
		t.Cleanup(func() { server.status = 0 })
		server.answer = `{"error":"upstream unavailable"}`
		c := &explain{address: server.URL, format: "json", file: "-"}
		err := c.run(strings.NewReader(explainTestBody), io.Discard)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected response (HTTP 502)")
		assert.Contains(t, err.Error(), "upstream unavailable")
	})

	t.Run("a success without data is reported", func(t *testing.T) {
		server.answer = `{"code":0}`
		c := &explain{address: server.URL, format: "text", file: "-"}
		err := c.run(strings.NewReader(explainTestBody), io.Discard)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected response (HTTP 200)")
	})
}

func TestExplainUsage(t *testing.T) {
	assert.Contains(t, usageLine, "milvus explain [flags]")
}
