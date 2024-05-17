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
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type ProxyManagementSuite struct {
	suite.Suite

	querycoord *mocks.MockQueryCoordClient
	datacoord  *mocks.MockDataCoordClient
	proxy      *Proxy
}

func (s *ProxyManagementSuite) SetupTest() {
	s.datacoord = mocks.NewMockDataCoordClient(s.T())
	s.querycoord = mocks.NewMockQueryCoordClient(s.T())

	s.proxy = &Proxy{
		dataCoord:  s.datacoord,
		queryCoord: s.querycoord,
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

func (s *ProxyManagementSuite) TestListQueryNode() {
	s.Run("normal", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().ListQueryNode(mock.Anything, mock.Anything).Return(&querypb.ListQueryNodeResponse{
			Status: merr.Success(),
			NodeInfos: []*querypb.NodeInfo{
				{
					ID:      1,
					Address: "localhost",
					State:   "Healthy",
				},
			},
		}, nil)

		req, err := http.NewRequest(http.MethodPost, mgrListQueryNode, nil)
		s.Require().NoError(err)

		recorder := httptest.NewRecorder()
		s.proxy.ListQueryNode(recorder, req)
		s.Equal(http.StatusOK, recorder.Code)
		s.Equal(`{"nodeInfos":[{"ID":1,"address":"localhost","state":"Healthy"}]}`, recorder.Body.String())
	})

	s.Run("return_error", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().ListQueryNode(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error"))
		req, err := http.NewRequest(http.MethodPost, mgrListQueryNode, nil)
		s.Require().NoError(err)

		recorder := httptest.NewRecorder()
		s.proxy.ListQueryNode(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})

	s.Run("return_failure", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().ListQueryNode(mock.Anything, mock.Anything).Return(&querypb.ListQueryNodeResponse{
			Status: merr.Status(merr.ErrServiceNotReady),
		}, nil)

		req, err := http.NewRequest(http.MethodPost, mgrListQueryNode, nil)
		s.Require().NoError(err)

		recorder := httptest.NewRecorder()
		s.proxy.ListQueryNode(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})
}

func (s *ProxyManagementSuite) TestGetQueryNodeDistribution() {
	s.Run("normal", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().GetQueryNodeDistribution(mock.Anything, mock.Anything).Return(&querypb.GetQueryNodeDistributionResponse{
			Status:           merr.Success(),
			ID:               1,
			ChannelNames:     []string{"channel-1"},
			SealedSegmentIDs: []int64{1, 2, 3},
		}, nil)

		req, err := http.NewRequest(http.MethodPost, mgrGetQueryNodeDistribution, strings.NewReader("node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()
		s.proxy.GetQueryNodeDistribution(recorder, req)
		s.Equal(http.StatusOK, recorder.Code)
		s.Equal(`{"ID":1,"channel_names":["channel-1"],"sealed_segmentIDs":[1,2,3]}`, recorder.Body.String())
	})

	s.Run("return_error", func() {
		s.SetupTest()
		defer s.TearDownTest()

		// test invalid request body
		req, err := http.NewRequest(http.MethodPost, mgrGetQueryNodeDistribution, nil)
		s.Require().NoError(err)
		recorder := httptest.NewRecorder()
		s.proxy.GetQueryNodeDistribution(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test miss requested param
		req, err = http.NewRequest(http.MethodPost, mgrGetQueryNodeDistribution, strings.NewReader(""))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.GetQueryNodeDistribution(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test rpc return error
		s.querycoord.EXPECT().GetQueryNodeDistribution(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error"))
		req, err = http.NewRequest(http.MethodPost, mgrGetQueryNodeDistribution, strings.NewReader("node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.GetQueryNodeDistribution(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})

	s.Run("return_failure", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().GetQueryNodeDistribution(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error"))
		req, err := http.NewRequest(http.MethodPost, mgrGetQueryNodeDistribution, strings.NewReader("node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder := httptest.NewRecorder()
		s.proxy.GetQueryNodeDistribution(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})
}

	})
}

func (s *ProxyManagementSuite) TestSuspendQueryNode() {
	s.Run("normal", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().SuspendNode(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		req, err := http.NewRequest(http.MethodPost, mgrSuspendQueryNode, strings.NewReader("node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()
		s.proxy.SuspendQueryNode(recorder, req)
		s.Equal(http.StatusOK, recorder.Code)
		s.Equal(`{"msg": "OK"}`, recorder.Body.String())
	})

	s.Run("return_error", func() {
		s.SetupTest()
		defer s.TearDownTest()

		// test invalid request body
		req, err := http.NewRequest(http.MethodPost, mgrSuspendQueryNode, nil)
		s.Require().NoError(err)
		recorder := httptest.NewRecorder()
		s.proxy.SuspendQueryNode(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test miss requested param
		req, err = http.NewRequest(http.MethodPost, mgrSuspendQueryNode, strings.NewReader(""))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.SuspendQueryNode(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test rpc return error
		s.querycoord.EXPECT().SuspendNode(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error"))
		req, err = http.NewRequest(http.MethodPost, mgrSuspendQueryNode, strings.NewReader("node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.SuspendQueryNode(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})

	s.Run("return_failure", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().SuspendNode(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)
		req, err := http.NewRequest(http.MethodPost, mgrSuspendQueryNode, strings.NewReader("node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder := httptest.NewRecorder()
		s.proxy.SuspendQueryNode(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})
}

func (s *ProxyManagementSuite) TestResumeQueryNode() {
	s.Run("normal", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().ResumeNode(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		req, err := http.NewRequest(http.MethodPost, mgrResumeQueryNode, strings.NewReader("node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()
		s.proxy.ResumeQueryNode(recorder, req)
		s.Equal(http.StatusOK, recorder.Code)
		s.Equal(`{"msg": "OK"}`, recorder.Body.String())
	})

	s.Run("return_error", func() {
		s.SetupTest()
		defer s.TearDownTest()

		// test invalid request body
		req, err := http.NewRequest(http.MethodPost, mgrResumeQueryNode, nil)
		s.Require().NoError(err)
		recorder := httptest.NewRecorder()
		s.proxy.ResumeQueryNode(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test miss requested param
		req, err = http.NewRequest(http.MethodPost, mgrResumeQueryNode, strings.NewReader(""))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.ResumeQueryNode(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test rpc return error
		s.querycoord.EXPECT().ResumeNode(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error"))
		req, err = http.NewRequest(http.MethodPost, mgrResumeQueryNode, strings.NewReader("node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.ResumeQueryNode(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})

	s.Run("return_failure", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().ResumeNode(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error"))
		req, err := http.NewRequest(http.MethodPost, mgrResumeQueryNode, strings.NewReader("node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder := httptest.NewRecorder()
		s.proxy.ResumeQueryNode(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})
}

func (s *ProxyManagementSuite) TestTransferSegment() {
	s.Run("normal", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().TransferSegment(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		req, err := http.NewRequest(http.MethodPost, mgrTransferSegment, strings.NewReader("source_node_id=1&target_node_id=1&segment_id=1&copy_mode=false"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder := httptest.NewRecorder()
		s.proxy.TransferSegment(recorder, req)
		s.Equal(http.StatusOK, recorder.Code)
		s.Equal(`{"msg": "OK"}`, recorder.Body.String())

		// test use default param
		req, err = http.NewRequest(http.MethodPost, mgrTransferSegment, strings.NewReader("source_node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.TransferSegment(recorder, req)
		s.Equal(http.StatusOK, recorder.Code)
		s.Equal(`{"msg": "OK"}`, recorder.Body.String())
	})

	s.Run("return_error", func() {
		s.SetupTest()
		defer s.TearDownTest()

		// test invalid request body
		req, err := http.NewRequest(http.MethodPost, mgrTransferSegment, nil)
		s.Require().NoError(err)
		recorder := httptest.NewRecorder()
		s.proxy.TransferSegment(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test miss requested param
		req, err = http.NewRequest(http.MethodPost, mgrTransferSegment, strings.NewReader(""))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.TransferSegment(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test rpc return error
		s.querycoord.EXPECT().TransferSegment(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error"))
		req, err = http.NewRequest(http.MethodPost, mgrTransferSegment, strings.NewReader("source_node_id=1&target_node_id=1&segment_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.TransferSegment(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})

	s.Run("return_failure", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().TransferSegment(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)
		req, err := http.NewRequest(http.MethodPost, mgrTransferSegment, strings.NewReader("source_node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder := httptest.NewRecorder()
		s.proxy.TransferSegment(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})
}

func (s *ProxyManagementSuite) TestTransferChannel() {
	s.Run("normal", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().TransferChannel(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		req, err := http.NewRequest(http.MethodPost, mgrTransferChannel, strings.NewReader("source_node_id=1&target_node_id=1&segment_id=1&copy_mode=false"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder := httptest.NewRecorder()
		s.proxy.TransferChannel(recorder, req)
		s.Equal(http.StatusOK, recorder.Code)
		s.Equal(`{"msg": "OK"}`, recorder.Body.String())

		// test use default param
		req, err = http.NewRequest(http.MethodPost, mgrTransferChannel, strings.NewReader("source_node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.TransferChannel(recorder, req)
		s.Equal(http.StatusOK, recorder.Code)
		s.Equal(`{"msg": "OK"}`, recorder.Body.String())
	})

	s.Run("return_error", func() {
		s.SetupTest()
		defer s.TearDownTest()

		// test invalid request body
		req, err := http.NewRequest(http.MethodPost, mgrTransferChannel, nil)
		s.Require().NoError(err)
		recorder := httptest.NewRecorder()
		s.proxy.TransferChannel(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test miss requested param
		req, err = http.NewRequest(http.MethodPost, mgrTransferChannel, strings.NewReader(""))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.TransferChannel(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test rpc return error
		s.querycoord.EXPECT().TransferChannel(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error"))
		req, err = http.NewRequest(http.MethodPost, mgrTransferChannel, strings.NewReader("source_node_id=1&target_node_id=1&segment_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.TransferChannel(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})

	s.Run("return_failure", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().TransferChannel(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)
		req, err := http.NewRequest(http.MethodPost, mgrTransferChannel, strings.NewReader("source_node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder := httptest.NewRecorder()
		s.proxy.TransferChannel(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})
}

func (s *ProxyManagementSuite) TestCheckQueryNodeDistribution() {
	s.Run("normal", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().CheckQueryNodeDistribution(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		req, err := http.NewRequest(http.MethodPost, mgrCheckQueryNodeDistribution, strings.NewReader("source_node_id=1&target_node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder := httptest.NewRecorder()
		s.proxy.CheckQueryNodeDistribution(recorder, req)
		s.Equal(http.StatusOK, recorder.Code)
		s.Equal(`{"msg": "OK"}`, recorder.Body.String())
	})

	s.Run("return_error", func() {
		s.SetupTest()
		defer s.TearDownTest()

		// test invalid request body
		req, err := http.NewRequest(http.MethodPost, mgrCheckQueryNodeDistribution, nil)
		s.Require().NoError(err)
		recorder := httptest.NewRecorder()
		s.proxy.CheckQueryNodeDistribution(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test miss requested param
		req, err = http.NewRequest(http.MethodPost, mgrCheckQueryNodeDistribution, strings.NewReader(""))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.CheckQueryNodeDistribution(recorder, req)
		s.Equal(http.StatusBadRequest, recorder.Code)

		// test rpc return error
		s.querycoord.EXPECT().CheckQueryNodeDistribution(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error"))
		req, err = http.NewRequest(http.MethodPost, mgrCheckQueryNodeDistribution, strings.NewReader("source_node_id=1&target_node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder = httptest.NewRecorder()
		s.proxy.CheckQueryNodeDistribution(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})

	s.Run("return_failure", func() {
		s.SetupTest()
		defer s.TearDownTest()

		s.querycoord.EXPECT().CheckQueryNodeDistribution(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)
		req, err := http.NewRequest(http.MethodPost, mgrCheckQueryNodeDistribution, strings.NewReader("source_node_id=1&target_node_id=1"))
		s.Require().NoError(err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder := httptest.NewRecorder()
		s.proxy.CheckQueryNodeDistribution(recorder, req)
		s.Equal(http.StatusInternalServerError, recorder.Code)
	})
}

func TestProxyManagement(t *testing.T) {
	suite.Run(t, new(ProxyManagementSuite))
}
