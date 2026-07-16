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

package merr

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

func TestModernStatusConversion(t *testing.T) {
	status := &commonpb.Status{
		Code:      ErrCollectionNotFound.Code(),
		Reason:    "collection missing",
		Detail:    "collection missing from database",
		Retriable: true,
	}

	err := Error(status)
	require.ErrorIs(t, err, ErrCollectionNotFound)
	require.Equal(t, int32(100), Code(err))
	require.True(t, IsRetryableErr(err))

	var rpcErr *RPCError
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, status.GetDetail(), rpcErr.Detail())
	require.Equal(t, commonpb.ErrorCode_CollectionNotExists, Status(err).GetErrorCode())
}

func TestStatusMetadataConversion(t *testing.T) {
	status := &commonpb.Status{
		Code:      ErrParameterInvalid.Code(),
		Reason:    "bad parameter",
		Detail:    "dimension must be positive",
		Retriable: false,
		ExtraInfo: map[string]string{inputErrorFlagKey: "true"},
	}

	err := Error(status)
	require.ErrorIs(t, err, ErrParameterInvalid)
	var rpcErr *RPCError
	require.ErrorAs(t, err, &rpcErr)
	require.True(t, rpcErr.IsInputError())
	require.Equal(t, status.GetDetail(), rpcErr.Detail())
	require.Equal(t, status.GetExtraInfo(), Status(err).GetExtraInfo())
}

func TestUnknownModernCodeIsPreserved(t *testing.T) {
	status := &commonpb.Status{
		Code:      2400,
		Reason:    "function pipeline failed",
		Detail:    "embedding provider timed out",
		Retriable: true,
		ExtraInfo: map[string]string{inputErrorFlagKey: "true"},
	}

	err := Error(status)
	require.Equal(t, status.GetCode(), Code(err))
	require.True(t, IsRetryableErr(err))

	var rpcErr *RPCError
	require.ErrorAs(t, err, &rpcErr)
	require.True(t, rpcErr.IsInputError())
	require.Equal(t, status.GetDetail(), rpcErr.Detail())
}

func TestLegacyStatusConversion(t *testing.T) {
	tests := []struct {
		legacy commonpb.ErrorCode
		code   int32
		target error
	}{
		{commonpb.ErrorCode_NotReadyServe, 1, ErrServiceNotReady},
		{commonpb.ErrorCode_CollectionNotExists, 100, ErrCollectionNotFound},
		{commonpb.ErrorCode_IllegalArgument, 1100, ErrParameterInvalid},
		{commonpb.ErrorCode_MemoryQuotaExhausted, 3, nil},
		{commonpb.ErrorCode_DiskQuotaExhausted, 7, nil},
		{commonpb.ErrorCode_RateLimit, 8, nil},
		{commonpb.ErrorCode_ForceDeny, 9, nil},
		{commonpb.ErrorCode_TimeTickLongDelay, 11, nil},
		{commonpb.ErrorCode_MetaFailed, 500, nil},
		{commonpb.ErrorCode_IndexNotExist, 700, ErrIndexNotFound},
	}

	for _, test := range tests {
		t.Run(test.legacy.String(), func(t *testing.T) {
			err := Error(&commonpb.Status{ErrorCode: test.legacy, Reason: "legacy failure"})
			require.Equal(t, test.code, Code(err))
			if test.target != nil {
				require.ErrorIs(t, err, test.target)
			}
		})
	}
}

func TestStatusRoundTrip(t *testing.T) {
	original := WrapErrCollectionSchemaMisMatch("collection")
	status := Status(original)
	require.Equal(t, int32(109), status.GetCode())
	require.Equal(t, commonpb.ErrorCode_SchemaMismatch, status.GetErrorCode())
	require.Equal(t, "true", status.GetExtraInfo()[inputErrorFlagKey])

	roundTrip := Error(status)
	require.True(t, errors.Is(roundTrip, ErrCollectionSchemaMismatch))
}

func TestContextCodes(t *testing.T) {
	require.Equal(t, CanceledCode, Code(context.Canceled))
	require.Equal(t, TimeoutCode, Code(context.DeadlineExceeded))
}

func TestCheckRPCCall(t *testing.T) {
	transportErr := errors.New("transport")
	require.ErrorIs(t, CheckRPCCall(nil, transportErr), transportErr)
	require.Equal(t, unexpectedCode, Code(CheckRPCCall(nil, nil)))
	require.NoError(t, CheckRPCCall(&commonpb.Status{}, nil))
}
