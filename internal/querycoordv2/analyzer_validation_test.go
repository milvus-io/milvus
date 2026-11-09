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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type validationWAL struct{ streaming.WALAccesser }

func (*validationWAL) AnalyzerClient() streaming.AnalyzerClient { panic("unpatched") }

type validationClient struct{ streaming.AnalyzerClient }

func (*validationClient) ValidateAnalyzer(context.Context, *streamingpb.StreamingNodeValidateAnalyzerRequest) (*streamingpb.StreamingNodeValidateAnalyzerResponse, error) {
	panic("unpatched")
}

func TestValidateAnalyzerCompatibility(t *testing.T) {
	mockey.PatchConvey("legacy coordinator RPC forwards to streaming", t, func() {
		mockey.Mock(streaming.WAL).Return(&validationWAL{}).Build()
		mockey.Mock((*validationWAL).AnalyzerClient).Return(&validationClient{}).Build()
		var validationErr error
		call := mockey.Mock((*validationClient).ValidateAnalyzer).To(func(_ *validationClient, _ context.Context, req *streamingpb.StreamingNodeValidateAnalyzerRequest) (*streamingpb.StreamingNodeValidateAnalyzerResponse, error) {
			require.Len(t, req.AnalyzerInfos, 1)
			require.Equal(t, "body", req.AnalyzerInfos[0].Field)
			require.Equal(t, "english", req.AnalyzerInfos[0].Name)
			require.Equal(t, "{}", req.AnalyzerInfos[0].Params)
			if validationErr != nil {
				return nil, validationErr
			}
			return &streamingpb.StreamingNodeValidateAnalyzerResponse{ResourceIds: []int64{11}}, nil
		}).Build()
		server := &Server{}
		server.UpdateStateCode(commonpb.StateCode_Healthy)
		req := &querypb.ValidateAnalyzerRequest{AnalyzerInfos: []*querypb.AnalyzerInfo{{Field: "body", Name: "english", Params: "{}"}}}
		resp, err := server.ValidateAnalyzer(context.Background(), req)
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.Status))
		require.Equal(t, []int64{11}, resp.ResourceIds)
		validationErr = merr.ErrParameterInvalid
		resp, err = server.ValidateAnalyzer(context.Background(), req)
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(resp.Status), validationErr)
		server.UpdateStateCode(commonpb.StateCode_Initializing)
		resp, err = server.ValidateAnalyzer(context.Background(), req)
		require.NoError(t, err)
		require.Error(t, merr.Error(resp.Status))
		require.Equal(t, 2, call.Times())
	})
}
