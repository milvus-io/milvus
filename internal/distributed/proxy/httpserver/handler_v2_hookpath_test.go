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
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type recordingHook struct {
	mu      sync.Mutex
	methods []string
}

func (r *recordingHook) Init(params map[string]string) error { return nil }
func (r *recordingHook) VerifyAPIKey(string) (string, error) { return "", nil }
func (r *recordingHook) Release()                            {}

func (r *recordingHook) Mock(ctx context.Context, req interface{}, fullMethod string) (bool, interface{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.methods = append(r.methods, fullMethod)
	return false, nil, nil
}

func (r *recordingHook) Before(ctx context.Context, req interface{}, fullMethod string) (context.Context, error) {
	return ctx, nil
}

func (r *recordingHook) After(ctx context.Context, result interface{}, err error, fullMethod string) error {
	return nil
}

// Regression for #48115: the REST V2 grant endpoint must pass OperatePrivilegeV2
// (not the legacy OperatePrivilege) to the hook interceptor, otherwise denyAPI
// entries for OperatePrivilege silently block REST V2 grants on Cloud.
func TestGrantPrivilegeV2_FullMethodMismatch(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	// Trigger sync.Once init first; the next GetHook() inside HookInterceptor
	// would otherwise lazily re-init and overwrite our recording hook.
	hookutil.InitOnceHook()
	rec := &recordingHook{}
	hookutil.SetTestHook(rec)
	defer hookutil.SetTestHook(hookutil.DefaultHook{})

	mp := mocks.NewMockProxy(t)
	mp.EXPECT().OperatePrivilegeV2(mock.Anything, mock.Anything).
		Return(commonSuccessStatus, nil).Once()
	engine := initHTTPServerV2(mp, false)

	body := []byte(`{"roleName":"r","dbName":"default","collectionName":"c","privilege":"Insert"}`)
	req := httptest.NewRequest(http.MethodPost,
		versionalV2(RoleCategory, GrantPrivilegeActionV2),
		bytes.NewReader(body))
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	rec.mu.Lock()
	captured := append([]string(nil), rec.methods...)
	rec.mu.Unlock()

	assert.Contains(t, captured,
		"/milvus.proto.milvus.MilvusService/OperatePrivilegeV2",
		"V2 REST grant must pass the V2 gRPC method name to the hook interceptor")
	assert.NotContains(t, captured,
		"/milvus.proto.milvus.MilvusService/OperatePrivilege",
		"V2 REST grant must NOT pass the legacy method name (would trigger denyAPI regressions)")
}
