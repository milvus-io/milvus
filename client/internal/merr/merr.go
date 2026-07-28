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

// Package merr contains the client-side subset of Milvus RPC error handling.
// It deliberately depends only on the public API protobuf module so the Go SDK
// does not need to pull the server-side milvus/pkg module into its dependency
// graph.
package merr

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

const (
	CanceledCode   int32 = 10000
	TimeoutCode    int32 = 10001
	unexpectedCode int32 = (1 << 16) - 1

	inputErrorFlagKey = "is_input_error"
)

// RPCError is the client representation of an error returned in a Milvus
// commonpb.Status. Errors with the same Milvus code match through errors.Is.
type RPCError struct {
	message    string
	detail     string
	code       int32
	legacyCode commonpb.ErrorCode
	retriable  bool
	input      bool
}

func newRPCError(message string, code int32, legacyCode commonpb.ErrorCode, retriable, input bool) *RPCError {
	return &RPCError{
		message:    message,
		detail:     message,
		code:       code,
		legacyCode: legacyCode,
		retriable:  retriable,
		input:      input,
	}
}

func (e *RPCError) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

// Code returns the Milvus numeric error code.
func (e *RPCError) Code() int32 { return e.code }

// LegacyCode returns the deprecated commonpb.ErrorCode value.
func (e *RPCError) LegacyCode() commonpb.ErrorCode { return e.legacyCode }

// Detail returns the detailed error message sent by the server.
func (e *RPCError) Detail() string { return e.detail }

// Retriable reports whether the server marked the error as retriable.
func (e *RPCError) Retriable() bool { return e.retriable }

// IsInputError reports whether the server classified the error as an input
// error.
func (e *RPCError) IsInputError() bool { return e.input }

func (e *RPCError) Is(target error) bool {
	var other *RPCError
	return errors.As(target, &other) && e.code == other.code
}

var (
	ErrServiceNotReady      = newRPCError("service not ready", 1, commonpb.ErrorCode_NotReadyServe, true, false)
	ErrServiceInternal      = newRPCError("service internal error", 5, commonpb.ErrorCode_UnexpectedError, false, false)
	ErrServiceUnimplemented = newRPCError("service unimplemented", 10, commonpb.ErrorCode_UnexpectedError, false, false)

	ErrCollectionNotFound       = newRPCError("collection not found", 100, commonpb.ErrorCode_CollectionNotExists, false, false)
	ErrCollectionSchemaMismatch = newRPCError("collection schema mismatch", 109, commonpb.ErrorCode_SchemaMismatch, false, true)
	ErrIndexNotFound            = newRPCError("index not found", 700, commonpb.ErrorCode_IndexNotExist, false, false)
	ErrParameterInvalid         = newRPCError("invalid parameter", 1100, commonpb.ErrorCode_IllegalArgument, false, true)
	ErrParameterMissing         = newRPCError("missing required parameters", 1101, commonpb.ErrorCode_IllegalArgument, false, true)

	errUnexpected = newRPCError("unexpected error", unexpectedCode, commonpb.ErrorCode_UnexpectedError, false, false)
)

func cloneWithMessage(base *RPCError, message string) *RPCError {
	return &RPCError{
		message:    message,
		detail:     message,
		code:       base.code,
		legacyCode: base.legacyCode,
		retriable:  base.retriable,
		input:      base.input,
	}
}

func withDetails(base *RPCError, field string, value any, messages ...string) error {
	message := fmt.Sprintf("%s[%s=%v]", base.message, field, value)
	if len(messages) > 0 {
		message = strings.Join(messages, "->") + ": " + message
	}
	return cloneWithMessage(base, message)
}

// Code returns the Milvus code carried by err.
func Code(err error) int32 {
	if err == nil {
		return 0
	}
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr.code
	}
	if errors.Is(err, context.Canceled) {
		return CanceledCode
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return TimeoutCode
	}
	return unexpectedCode
}

// IsRetryableErr reports the retriable bit carried by an RPC error.
func IsRetryableErr(err error) bool {
	var rpcErr *RPCError
	return errors.As(err, &rpcErr) && rpcErr.retriable
}

func modernCodeFromLegacy(code commonpb.ErrorCode) int32 {
	switch code {
	case commonpb.ErrorCode_NotReadyServe:
		return 1
	case commonpb.ErrorCode_CollectionNotExists:
		return 100
	case commonpb.ErrorCode_IllegalArgument:
		return 1100
	case commonpb.ErrorCode_NodeIDNotMatch:
		return 904
	case commonpb.ErrorCode_InsufficientMemoryToLoad, commonpb.ErrorCode_MemoryQuotaExhausted:
		return 3
	case commonpb.ErrorCode_DiskQuotaExhausted:
		return 7
	case commonpb.ErrorCode_TimeTickLongDelay:
		return 11
	case commonpb.ErrorCode_RateLimit:
		return 8
	case commonpb.ErrorCode_ForceDeny:
		return 9
	case commonpb.ErrorCode_IndexNotExist:
		return 700
	case commonpb.ErrorCode_SegmentNotFound:
		return 600
	case commonpb.ErrorCode_MetaFailed:
		return 500
	default:
		return unexpectedCode
	}
}

func legacyCodeFromModern(code int32) commonpb.ErrorCode {
	switch code {
	case 1, 110:
		return commonpb.ErrorCode_NotReadyServe
	case 100:
		return commonpb.ErrorCode_CollectionNotExists
	case 1100, 1101, 1102:
		return commonpb.ErrorCode_IllegalArgument
	case 904:
		return commonpb.ErrorCode_NodeIDNotMatch
	case 200, 400, 501:
		return commonpb.ErrorCode_MetaFailed
	case 401, 503, 506, 905:
		return commonpb.ErrorCode_NoReplicaAvailable
	case 3:
		return commonpb.ErrorCode_InsufficientMemoryToLoad
	case 7:
		return commonpb.ErrorCode_DiskQuotaExhausted
	case 11, 505:
		return commonpb.ErrorCode_TimeTickLongDelay
	case 8:
		return commonpb.ErrorCode_RateLimit
	case 9:
		return commonpb.ErrorCode_ForceDeny
	case 700:
		return commonpb.ErrorCode_IndexNotExist
	case 600:
		return commonpb.ErrorCode_SegmentNotFound
	case 109:
		return commonpb.ErrorCode_SchemaMismatch
	default:
		return commonpb.ErrorCode_UnexpectedError
	}
}

// Ok reports whether status represents a successful RPC response.
func Ok(status *commonpb.Status) bool {
	return status.GetErrorCode() == commonpb.ErrorCode_Success && status.GetCode() == 0
}

// Error converts a protobuf status into a client RPC error.
func Error(status *commonpb.Status) error {
	if Ok(status) {
		return nil
	}

	code := status.GetCode()
	if code == 0 {
		code = modernCodeFromLegacy(status.GetErrorCode())
	}
	legacyCode := status.GetErrorCode()
	if legacyCode == commonpb.ErrorCode_Success {
		legacyCode = legacyCodeFromModern(code)
	}

	result := newRPCError(
		status.GetReason(),
		code,
		legacyCode,
		status.GetRetriable(),
		status.GetExtraInfo()[inputErrorFlagKey] == "true",
	)
	result.detail = status.GetDetail()
	return result
}

// CheckRPCCall combines a transport error and the Status embedded in an RPC
// response. Responses without a Status field are considered successful.
func CheckRPCCall(resp any, err error) error {
	if err != nil {
		return err
	}
	if resp == nil {
		return errUnexpected
	}

	switch value := resp.(type) {
	case interface{ GetStatus() *commonpb.Status }:
		return Error(value.GetStatus())
	case *commonpb.Status:
		return Error(value)
	default:
		return nil
	}
}

// Status converts an error into commonpb.Status. It is mainly useful for
// client-side mocks and tests.
func Status(err error) *commonpb.Status {
	if err == nil {
		return &commonpb.Status{}
	}

	code := Code(err)
	status := &commonpb.Status{
		Code:      code,
		ErrorCode: legacyCodeFromModern(code),
		Reason:    err.Error(),
		Detail:    err.Error(),
		Retriable: IsRetryableErr(err),
	}

	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		status.ErrorCode = rpcErr.legacyCode
		status.Detail = rpcErr.detail
		if rpcErr.input {
			status.ExtraInfo = map[string]string{inputErrorFlagKey: "true"}
		}
	}
	return status
}

// Success creates a successful Status, optionally with a reason.
func Success(reason ...string) *commonpb.Status {
	return &commonpb.Status{Reason: strings.Join(reason, " ")}
}

func WrapErrServiceNotReady(role string, sessionID int64, state string, messages ...string) error {
	message := fmt.Sprintf("%s[%s=%d]: %s", ErrServiceNotReady.message, role, sessionID, state)
	if len(messages) > 0 {
		message = strings.Join(messages, "->") + ": " + message
	}
	return cloneWithMessage(ErrServiceNotReady, message)
}

func WrapErrServiceInternal(reason string, messages ...string) error {
	message := ErrServiceInternal.message + ": " + reason
	if len(messages) > 0 {
		message = strings.Join(messages, "->") + ": " + message
	}
	return cloneWithMessage(ErrServiceInternal, message)
}

func WrapErrCollectionNotFound(collection any, messages ...string) error {
	return withDetails(ErrCollectionNotFound, "collection", collection, messages...)
}

func WrapErrCollectionSchemaMisMatch(collection any, messages ...string) error {
	return withDetails(ErrCollectionSchemaMismatch, "collection", collection, messages...)
}

func WrapErrIndexNotFound(indexName string, messages ...string) error {
	return withDetails(ErrIndexNotFound, "indexName", indexName, messages...)
}

func WrapErrParameterInvalid[T any](expected, actual T, messages ...string) error {
	message := fmt.Sprintf("%s[expected=%v][actual=%v]", ErrParameterInvalid.message, expected, actual)
	if len(messages) > 0 {
		message = strings.Join(messages, "->") + ": " + message
	}
	return cloneWithMessage(ErrParameterInvalid, message)
}

func WrapErrParameterInvalidMsg(format string, args ...any) error {
	return cloneWithMessage(ErrParameterInvalid, fmt.Sprintf(format, args...))
}

// WrapErrParameterInvalidErr reclassifies err as ErrParameterInvalid and only
// retains the original error text, not its identity or code.
func WrapErrParameterInvalidErr(err error, message string) error {
	return cloneWithMessage(ErrParameterInvalid, message+": "+err.Error())
}

func WrapErrParameterMissingMsg(format string, args ...any) error {
	return cloneWithMessage(ErrParameterMissing, fmt.Sprintf(format, args...))
}
