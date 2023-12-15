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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type ProxyManagementSuite struct {
	suite.Suite

	datacoord *mocks.MockDataCoordClient
	proxy     *Proxy
}

func (s *ProxyManagementSuite) SetupTest() {
	s.datacoord = mocks.NewMockDataCoordClient(s.T())
	s.proxy = &Proxy{
		dataCoord: s.datacoord,
	}
}

func (s *ProxyManagementSuite) TearDownTest() {
	s.datacoord.AssertExpectations(s.T())
}

func (s *ProxyManagementSuite) TestPauseDataCoordGC() {
	s.Run("normal", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.datacoord.EXPECT().GcControl(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *datapb.GcControlRequest, options ...grpc.CallOption) (*commonpb.Status, error) {
			s.Equal(datapb.GcCommand_Pause, req.GetCommand())
			return &commonpb.Status{}, nil
		})

		req, err := http.NewRequest(http.MethodGet, mgrRouteGcPause+"?pause_seconds=60", nil)
		s.Require().NoError(err)

		recorder := httptest.NewRecorder()
		s.proxy.PauseDatacoordGC(recorder, req)

		s.Equal(http.StatusOK, recorder.Code)
	})

	s.Run("return_error", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.datacoord.EXPECT().GcControl(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *datapb.GcControlRequest, options ...grpc.CallOption) (*commonpb.Status, error) {
			return &commonpb.Status{}, errors.New("mock")
		})

		req, err := http.NewRequest(http.MethodGet, mgrRouteGcPause+"?pause_seconds=60", nil)
		s.Require().NoError(err)

		recorder := httptest.NewRecorder()
		s.proxy.PauseDatacoordGC(recorder, req)

		s.Equal(http.StatusInternalServerError, recorder.Code)
	})

	s.Run("return_failure", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.datacoord.EXPECT().GcControl(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *datapb.GcControlRequest, options ...grpc.CallOption) (*commonpb.Status, error) {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "mocked",
			}, nil
		})

		req, err := http.NewRequest(http.MethodGet, mgrRouteGcPause+"?pause_seconds=60", nil)
		s.Require().NoError(err)

		recorder := httptest.NewRecorder()
		s.proxy.PauseDatacoordGC(recorder, req)

		s.Equal(http.StatusInternalServerError, recorder.Code)
	})
}

func (s *ProxyManagementSuite) TestResumeDatacoordGC() {
	s.Run("normal", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.datacoord.EXPECT().GcControl(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *datapb.GcControlRequest, options ...grpc.CallOption) (*commonpb.Status, error) {
			s.Equal(datapb.GcCommand_Resume, req.GetCommand())
			return &commonpb.Status{}, nil
		})

		req, err := http.NewRequest(http.MethodGet, mgrRouteGcResume, nil)
		s.Require().NoError(err)

		recorder := httptest.NewRecorder()
		s.proxy.ResumeDatacoordGC(recorder, req)

		s.Equal(http.StatusOK, recorder.Code)
	})

	s.Run("return_error", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.datacoord.EXPECT().GcControl(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *datapb.GcControlRequest, options ...grpc.CallOption) (*commonpb.Status, error) {
			return &commonpb.Status{}, errors.New("mock")
		})

		req, err := http.NewRequest(http.MethodGet, mgrRouteGcResume, nil)
		s.Require().NoError(err)

		recorder := httptest.NewRecorder()
		s.proxy.ResumeDatacoordGC(recorder, req)

		s.Equal(http.StatusInternalServerError, recorder.Code)
	})

	s.Run("return_failure", func() {
		s.SetupTest()
		defer s.TearDownTest()
		s.datacoord.EXPECT().GcControl(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *datapb.GcControlRequest, options ...grpc.CallOption) (*commonpb.Status, error) {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "mocked",
			}, nil
		})

		req, err := http.NewRequest(http.MethodGet, mgrRouteGcResume, nil)
		s.Require().NoError(err)

		recorder := httptest.NewRecorder()
		s.proxy.ResumeDatacoordGC(recorder, req)

		s.Equal(http.StatusInternalServerError, recorder.Code)
	})
}

func TestProxyManagement(t *testing.T) {
	suite.Run(t, new(ProxyManagementSuite))
}
