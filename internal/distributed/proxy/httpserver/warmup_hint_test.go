package httpserver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRESTLoadForwardsWarmupHintIncludingRefreshValidation(t *testing.T) {
	paramtable.Init()
	quota := paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key
	require.NoError(t, paramtable.Get().Save(quota, "false"))
	t.Cleanup(func() { _ = paramtable.Get().Reset(quota) })
	for _, tc := range []struct {
		action  string
		body    string
		warmup  string
		present bool
		refresh bool
	}{
		{LoadAction, `{"collectionName":"warmup_test"}`, "", false, false},
		{LoadAction, `{"collectionName":"warmup_test","warmupHint":true}`, "true", true, false},
		{LoadAction, `{"collectionName":"warmup_test","warmupHint":false}`, "false", true, false},
		{RefreshLoadAction, `{"collectionName":"warmup_test","warmupHint":true}`, "true", true, true},
	} {
		t.Run(tc.action+tc.body, func(t *testing.T) {
			proxy := mocks.NewMockProxy(t)
			proxy.EXPECT().LoadCollection(mock.Anything, mock.MatchedBy(func(req *milvuspb.LoadCollectionRequest) bool {
				value, present := req.GetLoadParams()["warmup_hint"]
				return value == tc.warmup && present == tc.present && req.GetRefresh() == tc.refresh
			})).Return(commonSuccessStatus, nil).Once()
			engine := initHTTPServerV2(proxy, false)
			req := httptest.NewRequest(http.MethodPost, versionalV2(CollectionCategory, tc.action), bytes.NewBufferString(tc.body))
			response := httptest.NewRecorder()
			engine.ServeHTTP(response, req)
			require.Equal(t, http.StatusOK, response.Code)
		})
	}
}
