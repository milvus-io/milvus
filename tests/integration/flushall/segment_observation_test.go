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

package flushall

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type segmentObservationClient struct {
	milvuspb.MilvusServiceClient
	read func() (*milvuspb.GetPersistentSegmentInfoResponse, error)
}

func (c *segmentObservationClient) GetPersistentSegmentInfo(context.Context, *milvuspb.GetPersistentSegmentInfoRequest, ...grpc.CallOption) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	return c.read()
}

func TestPersistentSegmentObservationRecoversEmptyCompactionView(t *testing.T) {
	calls := 0
	expected := &milvuspb.GetPersistentSegmentInfoResponse{
		Status: merr.Success(),
		Infos: []*milvuspb.PersistentSegmentInfo{{
			SegmentID: 123,
			State:     commonpb.SegmentState_Flushed,
			NumRows:   100,
		}},
	}
	client := &segmentObservationClient{read: func() (*milvuspb.GetPersistentSegmentInfoResponse, error) {
		calls++
		if calls == 1 {
			return &milvuspb.GetPersistentSegmentInfoResponse{Status: merr.Success()}, nil
		}
		return expected, nil
	}}
	resp, err := getPersistentSegmentInfoWithRetry(context.Background(), client, &milvuspb.GetPersistentSegmentInfoRequest{})
	require.NoError(t, err)
	require.Same(t, expected, resp)
	require.Equal(t, 2, calls)
}

func TestPersistentSegmentObservationRejectsPersistentEmptyView(t *testing.T) {
	calls := 0
	client := &segmentObservationClient{read: func() (*milvuspb.GetPersistentSegmentInfoResponse, error) {
		calls++
		return &milvuspb.GetPersistentSegmentInfoResponse{Status: merr.Success()}, nil
	}}
	_, err := getPersistentSegmentInfoWithRetry(context.Background(), client, &milvuspb.GetPersistentSegmentInfoRequest{})
	require.ErrorContains(t, err, "empty persistent segment observation")
	require.Equal(t, 5, calls)
}

func TestPersistentSegmentObservationPreservesRPCErrorPolicy(t *testing.T) {
	tests := []struct {
		name      string
		status    *commonpb.Status
		err       error
		wantErr   error
		wantCalls int
	}{
		{
			name: "current SegmentNotFound", status: merr.Status(merr.ErrSegmentNotFound), wantCalls: 2,
		},
		{
			name: "legacy SegmentNotFound", status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_SegmentNotFound}, wantCalls: 2,
		},
		{
			name: "unrelated status", status: merr.Status(merr.ErrServiceInternal), wantErr: merr.ErrServiceInternal, wantCalls: 1,
		},
		{
			name: "transport error", err: context.DeadlineExceeded, wantErr: context.DeadlineExceeded, wantCalls: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			expected := &milvuspb.GetPersistentSegmentInfoResponse{
				Status: merr.Success(),
				Infos:  []*milvuspb.PersistentSegmentInfo{{SegmentID: 123}},
			}
			client := &segmentObservationClient{read: func() (*milvuspb.GetPersistentSegmentInfoResponse, error) {
				calls++
				if calls == 1 {
					return &milvuspb.GetPersistentSegmentInfoResponse{Status: test.status}, test.err
				}
				return expected, nil
			}}
			resp, err := getPersistentSegmentInfoWithRetry(context.Background(), client, &milvuspb.GetPersistentSegmentInfoRequest{})
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
				require.Same(t, expected, resp)
			}
			require.Equal(t, test.wantCalls, calls)
		})
	}
}
