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

package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRootCoordTransferRPCClientChecksStatus(t *testing.T) {
	mockClient := mocks.NewMockRootCoordClient(t)
	mockClient.EXPECT().
		CatalogTransferPrepare(mock.Anything, mock.Anything).
		Return(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "prepare rejected",
		}, nil)

	client := NewRootCoordTransferRPCClient(mockClient)
	err := client.CatalogTransferPrepare(context.Background(), &rootcoordpb.CatalogTransferPrepareRequest{})
	require.Error(t, err)
	require.ErrorContains(t, err, "prepare rejected")
}

func TestRootCoordTransferRPCClientAcceptsSuccess(t *testing.T) {
	mockClient := mocks.NewMockRootCoordClient(t)
	mockClient.EXPECT().
		CatalogTransferAbort(mock.Anything, mock.Anything).
		Return(merr.Success(), nil)

	client := NewRootCoordTransferRPCClient(mockClient)
	require.NoError(t, client.CatalogTransferAbort(context.Background(), &rootcoordpb.CatalogTransferAbortRequest{}))
}
