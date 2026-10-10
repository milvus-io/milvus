// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pyudf

import (
	"context"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/proto/pyudfpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func executionError(failure *pyudfpb.ExecuteError) error {
	if failure == nil || len(failure.Message) > MaxErrorMessageBytes || !utf8.ValidString(failure.Message) {
		return merr.WrapErrServiceInternalMsg("py_udf: invalid ExecuteError")
	}
	var category error
	switch failure.Code {
	case pyudfpb.ErrorCode_INVALID_ARGUMENT:
		category = merr.ErrParameterInvalid
	case pyudfpb.ErrorCode_UDF_FAILED:
		category = merr.ErrFunctionFailed
	case pyudfpb.ErrorCode_RESOURCE_NOT_FOUND:
		category = merr.ErrIoKeyNotFound
	case pyudfpb.ErrorCode_RESOURCE_PERMISSION_DENIED:
		category = merr.ErrIoPermissionDenied
	case pyudfpb.ErrorCode_RESOURCE_IO_FAILED:
		category = merr.ErrIoFailed
	case pyudfpb.ErrorCode_OUT_OF_MEMORY:
		category = merr.ErrServiceMemoryLimitExceeded
	case pyudfpb.ErrorCode_UNSUPPORTED:
		category = merr.ErrServiceUnimplemented
	default:
		category = merr.ErrServiceInternal
	}
	return merr.Wrapf(category, "py_udf: worker %s: %s", failure.Code, failure.Message)
}

func transportError(err error) error {
	switch status.Code(err) {
	case codes.Canceled:
		return merr.Wrap(merr.Combine(err, context.Canceled), "py_udf: RPC canceled")
	case codes.DeadlineExceeded:
		return merr.Wrap(merr.Combine(err, context.DeadlineExceeded), "py_udf: RPC deadline")
	case codes.Unavailable:
		return merr.WrapErrServiceUnavailableErr(err, "py_udf: RPC unavailable")
	case codes.ResourceExhausted:
		return merr.Wrap(merr.Combine(err, merr.ErrServiceResourceInsufficient), "py_udf: TRANSPORT_RESOURCE_LIMIT_UNKNOWN")
	case codes.Unimplemented:
		return merr.Wrap(merr.Combine(err, merr.ErrServiceUnimplemented), "py_udf: RPC unsupported")
	default:
		return merr.WrapErrServiceInternalErr(err, "py_udf: RPC failed")
	}
}
