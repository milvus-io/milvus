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

package dql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestHighlightSerialOverloadPublishesOnlySuccess(t *testing.T) {
	task := &HighlightTask{GetHighlightRequest: &querypb.GetHighlightRequest{}}
	client := mocks.NewMockQueryNodeClient(t)
	refused := &querypb.GetHighlightResponse{Status: merr.Status(merr.WrapErrTooManyRequests(200))}
	success := &querypb.GetHighlightResponse{Status: merr.Success()}
	client.EXPECT().GetHighlight(mock.Anything, mock.Anything).Return(refused, nil).Once()
	client.EXPECT().GetHighlight(mock.Anything, mock.Anything).Return(success, nil).Once()
	lb := shardclient.NewMockLBPolicy(t)
	lb.EXPECT().ExecuteOneChannel(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, w shardclient.CollectionWorkLoad) error {
		require.True(t, w.OverloadRetryable)
		require.False(t, w.Hedgeable, "this callback is only safe for serial failover")
		err := w.Exec(ctx, 1, client, "ch")
		require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
		require.Nil(t, task.result, "a refused attempt must not publish its response")
		return w.Exec(ctx, 2, client, "ch")
	})
	task.lb = lb
	require.NoError(t, task.Execute(context.Background()))
	require.Same(t, success, task.result)
}
