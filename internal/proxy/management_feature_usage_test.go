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
	"context"
	"net/http"
	"net/http/httptest"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	management "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const testRootPassword = "FeatureUsageRootPwd"

// setupRootCredential points the privilege cache at a credential store that
// knows the root password, so PasswordVerify can succeed in the test.
func (s *ProxyManagementSuite) setupRootCredential() {
	privilege.ResetPrivilegeCacheForTest()
	s.T().Cleanup(privilege.ResetPrivilegeCacheForTest)
	encrypted, err := crypto.PasswordEncrypt(testRootPassword)
	s.Require().NoError(err)
	mockedCoord := NewMixCoordMock()
	mockedCoord.GetGetCredentialFunc = func(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
		if req.GetUsername() != util.UserRoot {
			return nil, errors.New("not found")
		}
		return &rootcoordpb.GetCredentialResponse{Status: merr.Success(), Username: util.UserRoot, Password: encrypted}, nil
	}
	privilege.InitPrivilegeCache(context.Background(), mockedCoord)
}

func (s *ProxyManagementSuite) TestFeatureUsage() {
	newReq := func(method string) *http.Request {
		req, err := http.NewRequest(method, management.RouteFeatureUsage, nil)
		s.Require().NoError(err)
		return req
	}

	s.Run("no credentials is 401 and does not reach mixcoord", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.setupRootCredential()

		recorder := httptest.NewRecorder()
		s.proxy.FeatureUsage(recorder, newReq(http.MethodGet))
		s.Equal(http.StatusUnauthorized, recorder.Code)
		s.NotEmpty(recorder.Header().Get("WWW-Authenticate"))
	})

	s.Run("non-root user is 401", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.setupRootCredential()

		req := newReq(http.MethodGet)
		req.SetBasicAuth("alice", testRootPassword)
		recorder := httptest.NewRecorder()
		s.proxy.FeatureUsage(recorder, req)
		s.Equal(http.StatusUnauthorized, recorder.Code)
	})

	s.Run("root with wrong password is 401", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.setupRootCredential()

		req := newReq(http.MethodGet)
		req.SetBasicAuth(util.UserRoot, "wrong")
		recorder := httptest.NewRecorder()
		s.proxy.FeatureUsage(recorder, req)
		s.Equal(http.StatusUnauthorized, recorder.Code)
	})

	s.Run("wrong method is 405", func() {
		s.SetupTest()
		defer s.TearDownTest()

		recorder := httptest.NewRecorder()
		s.proxy.FeatureUsage(recorder, newReq(http.MethodPost))
		s.Equal(http.StatusMethodNotAllowed, recorder.Code)
	})

	s.Run("root returns the report", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.setupRootCredential()

		s.mixcoord.EXPECT().GetFeatureUsage(mock.Anything, mock.Anything).Return(&internalpb.FeatureUsageReport{
			Status:       merr.Success(),
			CollectedAt:  1234,
			BuildVersion: "v3.0.0-test",
			Nodes: []*internalpb.FeatureUsageNode{{
				Role: typeutil.ProxyRole, NodeId: 1, Reachable: true,
				Entries: []*internalpb.FeatureEntry{{Group: "request", Name: "iterator", Value: 2, LastUsedAt: 1000}},
			}},
		}, nil)

		req := newReq(http.MethodGet)
		req.SetBasicAuth(util.UserRoot, testRootPassword)
		recorder := httptest.NewRecorder()
		s.proxy.FeatureUsage(recorder, req)
		s.Equal(http.StatusOK, recorder.Code)
		s.Equal("application/json", recorder.Header().Get("Content-Type"))
		body := recorder.Body.String()
		s.Contains(body, `"build_version":"v3.0.0-test"`)
		s.Contains(body, `"name":"iterator"`)
		s.Contains(body, `"last_used_at":1000`)
		s.NotContains(body, `"status"`, "status is stripped from the HTTP body")
	})

	s.Run("mixcoord error is 500", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.setupRootCredential()

		s.mixcoord.EXPECT().GetFeatureUsage(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error"))
		req := newReq(http.MethodGet)
		req.SetBasicAuth(util.UserRoot, testRootPassword)
		recorder := httptest.NewRecorder()
		s.proxy.FeatureUsage(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})

	s.Run("mixcoord failed status is 500", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.setupRootCredential()

		s.mixcoord.EXPECT().GetFeatureUsage(mock.Anything, mock.Anything).Return(&internalpb.FeatureUsageReport{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mocked"},
		}, nil)
		req := newReq(http.MethodGet)
		req.SetBasicAuth(util.UserRoot, testRootPassword)
		recorder := httptest.NewRecorder()
		s.proxy.FeatureUsage(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})
}
