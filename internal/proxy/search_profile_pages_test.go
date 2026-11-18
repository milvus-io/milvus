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

package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSearchProfileStoreKeepsLatest(t *testing.T) {
	var s searchProfileStore
	for i := int64(1); i <= maxSearchProfiles+5; i++ {
		s.add(&searchProfile{ID: i, Text: "\x1b[1m\x1b[31mhot\x1b[0m"})
	}
	list := s.list()
	require.Len(t, list, maxSearchProfiles)
	assert.EqualValues(t, maxSearchProfiles+5, list[0].ID)
	assert.EqualValues(t, 6, list[len(list)-1].ID)
	assert.Nil(t, s.get(5))
	assert.Equal(t, "hot", s.get(6).Text)
}

func TestRecordSearchProfileSkipsSearchesWithoutOne(t *testing.T) {
	node := &Proxy{}
	request := &milvuspb.SearchRequest{DbName: "default", CollectionName: "c", Dsl: "cat < 50"}
	node.recordSearchProfile(1, "root", request, time.Millisecond, merr.Success())
	assert.Empty(t, node.searchProfiles.list())

	status := merr.Success()
	status.ExtraInfo = map[string]string{SearchProfileTextKey: "Summary"}
	node.recordSearchProfile(2, "root", request, time.Millisecond, status)
	list := node.searchProfiles.list()
	require.Len(t, list, 1)
	assert.Equal(t, "Search", list[0].Type)
	assert.Equal(t, "cat < 50", list[0].Expr)
}

func TestSearchProfilePages(t *testing.T) {
	node := &Proxy{}
	node.searchProfiles.add(&searchProfile{
		ID: 42, User: "root", Database: "default", Collection: "pc_vec", Expr: "cat < 50", Type: "Search",
		Start: time.Unix(0, 0), End: time.Unix(0, 0), Total: 1652 * time.Microsecond, State: "Finished",
		Text: "Summary\n    TotalTime: 1.652ms\nStages\n└──proxy.execute [Execution]",
	})
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET(mhttp.SearchProfilesPath, getSearchProfiles(node))
	router.GET(mhttp.SearchProfilePath, getSearchProfile(node))
	get := func(url string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
		return w
	}

	w := get(mhttp.SearchProfilesPath)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "Finished Searches")
	assert.Contains(t, w.Body.String(), "<td>pc_vec</td>")
	assert.Contains(t, w.Body.String(), "cat &lt; 50")
	assert.Contains(t, w.Body.String(), "<td>1.652ms</td>")
	assert.Contains(t, w.Body.String(), `<a href="profile?id=42">42</a>`)

	w = get(mhttp.SearchProfilePath + "?id=42")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "└──proxy.execute [Execution]")

	assert.Equal(t, http.StatusNotFound, get(mhttp.SearchProfilePath+"?id=7").Code)
	assert.Equal(t, http.StatusNotFound, get(mhttp.SearchProfilePath+"?id=x").Code)
}
