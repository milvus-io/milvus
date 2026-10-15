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

package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Resource-lifecycle tests replace only native validation; reservation and DDL
// publication continue through their real implementation.
func mockAnalyzerValidation(t *testing.T) func(func(context.Context, []*querypb.AnalyzerInfo) ([]int64, error)) {
	var validate func(context.Context, []*querypb.AnalyzerInfo) ([]int64, error)
	patch := mockey.Mock((*Core).validateAnalyzerInfos).To(func(_ *Core, ctx context.Context, infos []*querypb.AnalyzerInfo) ([]int64, error) {
		if len(infos) == 0 {
			return nil, nil
		}
		return validate(ctx, infos)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return func(f func(context.Context, []*querypb.AnalyzerInfo) ([]int64, error)) { validate = f }
}

type validationWAL struct{ streaming.WALAccesser }

func (*validationWAL) AnalyzerClient() streaming.AnalyzerClient { panic("unpatched") }

type validationClient struct{ streaming.AnalyzerClient }

func (*validationClient) ValidateAnalyzer(context.Context, *streamingpb.StreamingNodeValidateAnalyzerRequest) (*streamingpb.StreamingNodeValidateAnalyzerResponse, error) {
	panic("unpatched")
}

func TestValidateAnalyzerInfosUsesStreaming(t *testing.T) {
	mockey.PatchConvey("validation does not require QN inventory or a collection", t, func() {
		wal := mockey.Mock(streaming.WAL).Return(&validationWAL{}).Build()
		mockey.Mock((*validationWAL).AnalyzerClient).Return(&validationClient{}).Build()
		var validationErr error
		mockey.Mock((*validationClient).ValidateAnalyzer).To(func(_ *validationClient, _ context.Context, req *streamingpb.StreamingNodeValidateAnalyzerRequest) (*streamingpb.StreamingNodeValidateAnalyzerResponse, error) {
			require.Len(t, req.AnalyzerInfos, 1)
			info := req.AnalyzerInfos[0]
			require.Equal(t, "body", info.Field)
			require.Equal(t, "english", info.Name)
			require.Equal(t, "{}", info.Params)
			if validationErr != nil {
				return nil, validationErr
			}
			return &streamingpb.StreamingNodeValidateAnalyzerResponse{ResourceIds: []int64{11}}, nil
		}).Build()
		core := &Core{} // No coordinator client or file-resource observer.
		ids, err := core.validateAnalyzerInfos(context.Background(), nil)
		require.NoError(t, err)
		require.Empty(t, ids)
		require.Zero(t, wal.Times())
		infos := []*querypb.AnalyzerInfo{{Field: "body", Name: "english", Params: "{}"}}
		ids, err = core.validateAnalyzerInfos(context.Background(), infos)
		require.NoError(t, err)
		require.Equal(t, []int64{11}, ids)
		validationErr = merr.ErrServiceUnavailable
		_, err = core.validateAnalyzerInfos(context.Background(), infos)
		require.ErrorIs(t, err, validationErr)
	})
}
