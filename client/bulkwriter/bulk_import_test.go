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

package bulkwriter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type BulkImportSuite struct {
	suite.Suite
}

func (s *BulkImportSuite) TestBulkImport() {
	s.Run("normal_case", func() {
		svr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			authHeader := req.Header.Get("Authorization")
			s.Equal("Bearer root:Milvus", authHeader)
			s.True(strings.Contains(req.URL.Path, "/v2/vectordb/jobs/import/create"))
			rw.Write([]byte(`{"status":0, "data":{"jobId": "123"}}`))
		}))
		defer svr.Close()

		resp, err := BulkImport(context.Background(),
			NewBulkImportOption(svr.URL, "hello_milvus", [][]string{{"files/a.json", "files/b.json"}}).
				WithPartition("_default").
				WithOption("backup", "true").
				WithAPIKey("root:Milvus").
				WithDBName("db1"),
		)
		s.NoError(err)
		s.EqualValues(0, resp.Status)
		s.Equal("123", resp.Data.JobID)
	})

	s.Run("svr_error", func() {
		svr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			// rw.
			rw.WriteHeader(http.StatusInternalServerError)
			rw.Write([]byte(`interal server error`))
		}))
		defer svr.Close()

		_, err := BulkImport(context.Background(), NewBulkImportOption(svr.URL, "hello_milvus", [][]string{{"files/a.json", "files/b.json"}}))
		s.Error(err)
	})

	s.Run("status_error", func() {
		svr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			s.True(strings.Contains(req.URL.Path, "/v2/vectordb/jobs/import/create"))
			rw.Write([]byte(`{"status":1100, "message": "import job failed"}`))
		}))
		defer svr.Close()

		_, err := BulkImport(context.Background(), NewBulkImportOption(svr.URL, "hello_milvus", [][]string{{"files/a.json", "files/b.json"}}))
		s.Error(err)
	})

	s.Run("server_closed", func() {
		svr2 := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {}))
		svr2.Close()
		_, err := BulkImport(context.Background(), NewBulkImportOption(svr2.URL, "hello_milvus", [][]string{{"files/a.json", "files/b.json"}}))
		s.Error(err)
	})
}

func (s *BulkImportSuite) TestListImportJobs() {
	s.Run("normal_case", func() {
		svr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			authHeader := req.Header.Get("Authorization")
			s.Equal("Bearer root:Milvus", authHeader)
			s.True(strings.Contains(req.URL.Path, "/v2/vectordb/jobs/import/list"))
			rw.Write([]byte(`{"status":0, "data":{"records": [{"jobID": "abc", "collectionName": "hello_milvus", "state":"Importing", "progress": 50}]}}`))
		}))
		defer svr.Close()

		resp, err := ListImportJobs(context.Background(),
			NewListImportJobsOption(svr.URL, "hello_milvus").
				WithPageSize(10).
				WithCurrentPage(1).
				WithAPIKey("root:Milvus"),
		)
		s.NoError(err)
		s.EqualValues(0, resp.Status)
		if s.Len(resp.Data.Records, 1) {
			record := resp.Data.Records[0]
			s.Equal("abc", record.JobID)
			s.Equal("hello_milvus", record.CollectionName)
			s.Equal("Importing", record.State)
			s.EqualValues(50, record.Progress)
		}
	})

	s.Run("svr_error", func() {
		svr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusInternalServerError)
		}))
		defer svr.Close()

		_, err := ListImportJobs(context.Background(), NewListImportJobsOption(svr.URL, "hello_milvus"))
		s.Error(err)
	})
}

func (s *BulkImportSuite) TestGetImportProgress() {
	s.Run("normal_case", func() {
		svr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			authHeader := req.Header.Get("Authorization")
			s.Equal("Bearer root:Milvus", authHeader)
			s.True(strings.Contains(req.URL.Path, "/v2/vectordb/jobs/import/describe"))
			rw.Write([]byte(`{"status":0, "data":{"collectionName": "hello_milvus","jobId":"abc", "state":"Importing", "progress": 50, "importedRows": 20000,"totalRows": 40000, "details":[{"fileName": "files/a.json", "fileSize": 64312, "progress": 100, "state": "Completed"}, {"fileName":"files/b.json", "fileSize":52912, "progress":0, "state":"Importing"}]}}`))
		}))
		defer svr.Close()

		resp, err := GetImportProgress(context.Background(),
			NewGetImportProgressOption(svr.URL, "abc").
				WithAPIKey("root:Milvus"),
		)
		s.NoError(err)
		s.EqualValues(0, resp.Status)
		s.Equal("hello_milvus", resp.Data.CollectionName)
		s.Equal("abc", resp.Data.JobID)
		s.Equal("Importing", resp.Data.State)
		s.EqualValues(50, resp.Data.Progress)
		if s.Len(resp.Data.Details, 2) {
			detail1 := resp.Data.Details[0]
			s.Equal("files/a.json", detail1.FileName)
			s.Equal("Completed", detail1.State)
			s.EqualValues(100, detail1.Progress)
			detail2 := resp.Data.Details[1]
			s.Equal("files/b.json", detail2.FileName)
			s.Equal("Importing", detail2.State)
			s.EqualValues(0, detail2.Progress)
		}
	})

	s.Run("svr_error", func() {
		svr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusInternalServerError)
		}))
		defer svr.Close()

		_, err := GetImportProgress(context.Background(), NewGetImportProgressOption(svr.URL, "abc"))
		s.Error(err)
	})
}

func TestBulkImportAPIs(t *testing.T) {
	suite.Run(t, new(BulkImportSuite))
}
