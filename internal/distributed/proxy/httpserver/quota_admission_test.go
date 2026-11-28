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
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type restQuotaScope struct {
	dbID       int64
	partitions map[int64][]int64
	rateType   internalpb.RateType
	tokens     int
}

type rejectingRESTAdmissionLimiter struct {
	checks []restQuotaScope
}

func (l *rejectingRESTAdmissionLimiter) Check(dbID int64, partitions map[int64][]int64, rateType internalpb.RateType, tokens int) error {
	l.checks = append(l.checks, restQuotaScope{dbID: dbID, partitions: partitions, rateType: rateType, tokens: tokens})
	return merr.ErrServiceQuotaExceeded
}

func (l *rejectingRESTAdmissionLimiter) Alloc(_ context.Context, dbID int64, partitions map[int64][]int64, rateType internalpb.RateType, tokens int) error {
	return l.Check(dbID, partitions, rateType, tokens)
}

func TestRESTV2DatabaseAlterAndImportCheckQuota(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	quota := &params.QuotaConfig.QuotaAndLimitsEnabled
	auth := &proxy.Params.CommonCfg.AuthorizationEnabled
	originalQuota, originalAuth := quota.GetValue(), auth.GetValue()
	t.Cleanup(func() {
		assert.NoError(t, params.Save(quota.Key, originalQuota))
		assert.NoError(t, params.Save(auth.Key, originalAuth))
	})
	require.NoError(t, params.Save(quota.Key, "true"))
	require.NoError(t, params.Save(auth.Key, "false"))

	dbQuota := restQuotaScope{
		dbID: util.InvalidDBID, partitions: map[int64][]int64{},
		rateType: internalpb.RateType_DDLDB, tokens: 1,
	}
	importRequest := &internalpb.ImportRequest{
		DbName: "target", CollectionName: "source", PartitionName: "p1",
		Files: []*internalpb.ImportFile{
			{Paths: []string{"a.json"}},
			{Paths: []string{"b.json", "c.json"}},
		},
	}
	for _, tc := range []struct {
		path string
		body string
		want restQuotaScope
	}{
		{
			path: versionalV2(DataBaseCategory, AlterAction),
			body: `{"dbName":"target","properties":{"database.replica.number":2}}`,
			want: dbQuota,
		},
		{
			path: versionalV2(DataBaseCategory, AlterPropertiesAction),
			body: `{"dbName":"target","properties":{"database.replica.number":2}}`,
			want: dbQuota,
		},
		{
			path: versionalV2(ImportJobCategory, CreateAction),
			body: `{"dbName":"target","collectionName":"source","partitionName":"p1","files":[["a.json"],["b.json","c.json"]]}`,
			want: restQuotaScope{
				dbID: 100, partitions: map[int64][]int64{10: {20}},
				rateType: internalpb.RateType_DMLBulkLoad, tokens: proto.Size(importRequest),
			},
		},
	} {
		t.Run(tc.path, func(t *testing.T) {
			cache := proxy.NewMockCache(t)
			if tc.want.rateType == internalpb.RateType_DMLBulkLoad {
				cache.EXPECT().GetDatabaseInfo(mock.Anything, "target").Return(&metacache.DatabaseInfo{DBID: 100}, nil).Once()
				cache.EXPECT().GetCollectionID(mock.Anything, "target", "source").Return(int64(10), nil).Once()
				cache.EXPECT().GetPartitionInfo(mock.Anything, "target", "source", "p1").Return(&metacache.PartitionInfo{PartitionID: 20}, nil).Once()
			}

			backend := mocks.NewMockProxy(t)
			limiter := &rejectingRESTAdmissionLimiter{}
			backend.EXPECT().GetRateLimiter().Return(limiter, nil).Once()
			backend.EXPECT().AlterDatabase(mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
			backend.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(&internalpb.ImportResponse{Status: merr.Success(), JobID: "123"}, nil).Maybe()
			server := initHTTPServerV2(proxyComponentWithMetaCache{ProxyComponent: backend, metaCache: cache}, false)

			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			server.ServeHTTP(recorder, req)

			require.Equal(t, http.StatusOK, recorder.Code)
			var response ReturnErrMsg
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			assert.Equal(t, merr.Code(merr.ErrHTTPRateLimit), response.Code)
			assert.Contains(t, response.Message, merr.ErrServiceQuotaExceeded.Error())
			assert.Equal(t, []restQuotaScope{tc.want}, limiter.checks)
			backend.AssertNumberOfCalls(t, "AlterDatabase", 0)
			backend.AssertNumberOfCalls(t, "ImportV2", 0)
		})
	}
}
