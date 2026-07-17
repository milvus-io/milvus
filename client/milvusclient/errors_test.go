// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

func TestRPCErrorPublicAPI(t *testing.T) {
	err := merr.Error(&commonpb.Status{
		Code:      100,
		Reason:    "collection missing",
		Detail:    "collection missing from database",
		Retriable: true,
	})

	require.ErrorIs(t, err, ErrCollectionNotFound)
	require.Equal(t, int32(100), ErrorCode(err))
	require.True(t, IsRetryableError(err))

	var rpcErr *RPCError
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, "collection missing from database", rpcErr.Detail())
	require.Equal(t, commonpb.ErrorCode_CollectionNotExists, rpcErr.LegacyCode())
}
