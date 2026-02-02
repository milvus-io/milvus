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

package merr

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/errors"
	"golang.org/x/exp/constraints"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

const InputErrorFlagKey string = "is_input_error"

// Code returns the error code of the given error,
// WARN: DO NOT use this for now
func Code(err error) int32 {
	if err == nil {
		return 0
	}

	cause := errors.Cause(err)
	switch specificErr := cause.(type) {
	case milvusError:
		return specificErr.code()

	default:
		if errors.Is(specificErr, context.Canceled) {
			return CanceledCode
		} else if errors.Is(specificErr, context.DeadlineExceeded) {
			return TimeoutCode
		} else {
			return errUnexpected.code()
		}
	}
}

func IsRetryableErr(err error) bool {
	if err, ok := err.(milvusError); ok {
		return err.retriable
	}

	return false
}

func IsCanceledOrTimeout(err error) bool {
	return errors.IsAny(err, context.Canceled, context.DeadlineExceeded)
}

// Status returns a status according to the given err,
// returns Success status if err is nil
func Status(err error) *commonpb.Status {
	if err == nil {
		return &commonpb.Status{}
	}

	code := Code(err)

	status := &commonpb.Status{
		Code:   code,
		Reason: previousLastError(err).Error(),
		// Deprecated, for compatibility
		ErrorCode: oldCode(code),
		Retriable: IsRetryableErr(err),
		Detail:    err.Error(),
	}

	if GetErrorType(err) == InputError {
		status.ExtraInfo = map[string]string{InputErrorFlagKey: "true"}
	}
	return status
}

func previousLastError(err error) error {
	lastErr := err
	for {
		nextErr := errors.Unwrap(err)
		if nextErr == nil {
			break
		}
		lastErr = err
		err = nextErr
	}
	return lastErr
}

func CheckRPCCall(resp any, err error) error {
	if err != nil {
		return err
	}
	if resp == nil {
		return errUnexpected
	}
	switch resp := resp.(type) {
	case interface{ GetStatus() *commonpb.Status }:
		return Error(resp.GetStatus())
	case *commonpb.Status:
		return Error(resp)
	}
	return nil
}

func Success(reason ...string) *commonpb.Status {
	status := Status(nil)
	// NOLINT
	status.Reason = strings.Join(reason, " ")
	return status
}

// Deprecated
func StatusWithErrorCode(err error, code commonpb.ErrorCode) *commonpb.Status {
	if err == nil {
		return &commonpb.Status{}
	}

	return &commonpb.Status{
		Code:      Code(err),
		Reason:    err.Error(),
		ErrorCode: code,
	}
}

func oldCode(code int32) commonpb.ErrorCode {
	switch code {
	case ErrServiceNotReady.code():
		return commonpb.ErrorCode_NotReadyServe

	case ErrCollectionNotFound.code():
		return commonpb.ErrorCode_CollectionNotExists

	case ErrParameterInvalid.code():
		return commonpb.ErrorCode_IllegalArgument

	case ErrNodeNotMatch.code():
		return commonpb.ErrorCode_NodeIDNotMatch

	case ErrPartitionNotFound.code(), ErrReplicaNotFound.code():
		return commonpb.ErrorCode_MetaFailed

	case ErrReplicaNotAvailable.code(), ErrChannelNotAvailable.code(), ErrNodeNotAvailable.code():
		return commonpb.ErrorCode_NoReplicaAvailable

	case ErrServiceMemoryLimitExceeded.code():
		return commonpb.ErrorCode_InsufficientMemoryToLoad

	case ErrServiceDiskLimitExceeded.code():
		return commonpb.ErrorCode_DiskQuotaExhausted

	case ErrServiceTimeTickLongDelay.code():
		return commonpb.ErrorCode_TimeTickLongDelay

	case ErrServiceRateLimit.code():
		return commonpb.ErrorCode_RateLimit

	case ErrServiceQuotaExceeded.code():
		return commonpb.ErrorCode_ForceDeny

	case ErrIndexNotFound.code():
		return commonpb.ErrorCode_IndexNotExist

	case ErrSegmentNotFound.code():
		return commonpb.ErrorCode_SegmentNotFound

	case ErrChannelLack.code():
		return commonpb.ErrorCode_MetaFailed

	case ErrCollectionSchemaMismatch.code():
		return commonpb.ErrorCode_SchemaMismatch

	default:
		return commonpb.ErrorCode_UnexpectedError
	}
}

func OldCodeToMerr(code commonpb.ErrorCode) error {
	switch code {
	case commonpb.ErrorCode_NotReadyServe:
		return ErrServiceNotReady

	case commonpb.ErrorCode_CollectionNotExists:
		return ErrCollectionNotFound

	case commonpb.ErrorCode_IllegalArgument:
		return ErrParameterInvalid

	case commonpb.ErrorCode_NodeIDNotMatch:
		return ErrNodeNotMatch

	case commonpb.ErrorCode_InsufficientMemoryToLoad, commonpb.ErrorCode_MemoryQuotaExhausted:
		return ErrServiceMemoryLimitExceeded

	case commonpb.ErrorCode_DiskQuotaExhausted:
		return ErrServiceDiskLimitExceeded

	case commonpb.ErrorCode_TimeTickLongDelay:
		return ErrServiceTimeTickLongDelay

	case commonpb.ErrorCode_RateLimit:
		return ErrServiceRateLimit

	case commonpb.ErrorCode_ForceDeny:
		return ErrServiceQuotaExceeded

	case commonpb.ErrorCode_IndexNotExist:
		return ErrIndexNotFound

	case commonpb.ErrorCode_SegmentNotFound:
		return ErrSegmentNotFound

	case commonpb.ErrorCode_MetaFailed:
		return ErrChannelNotFound

	default:
		return errUnexpected
	}
}

func Ok(status *commonpb.Status) bool {
	return status.GetErrorCode() == commonpb.ErrorCode_Success && status.GetCode() == 0
}

// Error returns a error according to the given status,
// returns nil if the status is a success status
func Error(status *commonpb.Status) error {
	if Ok(status) {
		return nil
	}

	var eType ErrorType
	_, ok := status.GetExtraInfo()[InputErrorFlagKey]
	if ok {
		eType = InputError
	}

	// use code first
	code := status.GetCode()
	if code == 0 {
		return newMilvusError(status.GetReason(), Code(OldCodeToMerr(status.GetErrorCode())), false, WithDetail(status.GetDetail()), WithErrorType(eType))
	}
	return newMilvusError(status.GetReason(), code, status.GetRetriable(), WithDetail(status.GetDetail()), WithErrorType(eType))
}

// SegcoreError returns a merr according to the given segcore error code and message
func SegcoreError(code int32, msg string) error {
	return newMilvusError(msg, code, false)
}

func IsHealthy(stateCode commonpb.StateCode) error {
	if stateCode == commonpb.StateCode_Healthy {
		return nil
	}
	return CheckHealthy(stateCode)
}

func IsHealthyOrStopping(stateCode commonpb.StateCode) error {
	if stateCode == commonpb.StateCode_Healthy || stateCode == commonpb.StateCode_Stopping {
		return nil
	}
	return CheckHealthy(stateCode)
}

func WrapErrAsInputError(err error) error {
	if merr, ok := err.(milvusError); ok {
		WithErrorType(InputError)(&merr)
		return merr
	}
	return err
}

func WrapErrAsSysError(err error) error {
	if merr, ok := err.(milvusError); ok {
		WithErrorType(SystemError)(&merr)
		return merr
	}
	return err
}

func WrapErrAsInputErrorWhen(err error, targets ...milvusError) error {
	if merr, ok := err.(milvusError); ok {
		for _, target := range targets {
			if target.errCode == merr.errCode {
				WithErrorType(InputError)(&merr)
				return merr
			}
		}
	}
	return err
}

func WrapErrCollectionReplicateMode(operation string) error {
	return wrapFields(ErrCollectionReplicateMode, value("operation", operation))
}

func GetErrorType(err error) ErrorType {
	if merr, ok := err.(milvusError); ok {
		return merr.errType
	}

	return SystemError
}

// keeps only 2 decimal places
func toMB[T constraints.Integer | constraints.Float](mem T) T {
	return T(math.Round(float64(mem)/1024/1024*100) / 100)
}

// CheckHealthy checks whether the state is healthy,
// returns nil if healthy,
// otherwise returns ErrServiceNotReady wrapped with current state
func CheckHealthy(stateCode commonpb.StateCode) error {
	if stateCode != commonpb.StateCode_Healthy {
		return ErrServiceNotReady
	}
	return nil
}

func AnalyzeState(role string, nodeID int64, state *milvuspb.ComponentStates) error {
	if err := Error(state.GetStatus()); err != nil {
		return WrapErrServiceNotReady(role, nodeID, err.Error())
	} else if stateCode := state.GetState().GetStateCode(); stateCode != commonpb.StateCode_Healthy {
		return WrapErrServiceNotReady(role, nodeID, stateCode.String())
	}
	return nil
}

func WrapErrServiceNotReady(role string, sessionID int64, state string, msg ...string) error {
	err := wrapFieldsWithDesc(ErrServiceNotReady,
		state,
		value(role, sessionID),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrServiceUnavailable(reason string, msg ...string) error {
	err := wrapFieldsWithDesc(ErrServiceUnavailable, reason)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrServiceMemoryLimitExceeded(predict, limit float32, msg ...string) error {
	err := wrapFields(ErrServiceMemoryLimitExceeded,
		value("predict(MB)", toMB(float64(predict))),
		value("limit(MB)", toMB(float64(limit))),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrTooManyRequests(limit int32, msg ...string) error {
	err := wrapFields(ErrServiceTooManyRequests,
		value("limit", limit),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrServiceInternal(reason string, msg ...string) error {
	err := wrapFieldsWithDesc(ErrServiceInternal, reason)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrServiceInternalErr(err error, format string, args ...any) error {
	if err == nil {
		// If the underlying error is nil, we fall back to the message-only version.
		return WrapErrServiceInternalMsg(format, args...)
	}

	// 1. Create the contextual message chain: "Context message" -> "Original error message"
	contextualMsg := fmt.Sprintf(format, args...)

	// 2. Wrap the underlying error first to include the context.
	errWithContext := errors.Wrap(err, contextualMsg)

	// 3. Wrap the resulting error chain with the Milvus constant (ErrParameterInvalid)
	// to ensure 'merr.Is(err, merr.ErrParameterInvalid)' works correctly and the code (1005) is preserved.
	// This makes ErrParameterInvalid the topmost error in the Milvus system for parameter issues.
	return errors.Wrap(ErrServiceInternal, errWithContext.Error())
}

func WrapErrServiceInternalMsg(fmt string, args ...any) error {
	return errors.Wrapf(ErrServiceInternal, fmt, args...)
}

func WrapErrServiceCrossClusterRouting(expectedCluster, actualCluster string, msg ...string) error {
	err := wrapFields(ErrServiceCrossClusterRouting,
		value("expectedCluster", expectedCluster),
		value("actualCluster", actualCluster),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrServiceDiskLimitExceeded(predict, limit float32, msg ...string) error {
	err := wrapFields(ErrServiceDiskLimitExceeded,
		value("predict(MB)", toMB(float64(predict))),
		value("limit(MB)", toMB(float64(limit))),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrServiceRateLimit(rate float64, msg ...string) error {
	err := wrapFields(ErrServiceRateLimit, value("rate", rate))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrServiceQuotaExceeded(reason string, msg ...string) error {
	err := wrapFields(ErrServiceQuotaExceeded, value("reason", reason))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrServiceUnimplemented(grpcErr error) error {
	return wrapFieldsWithDesc(ErrServiceUnimplemented, grpcErr.Error())
}

// database related
func WrapErrDatabaseNotFound(database any, msg ...string) error {
	err := wrapFields(ErrDatabaseNotFound, value("database", database))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrDatabaseNumLimitExceeded(limit int, msg ...string) error {
	err := wrapFields(ErrDatabaseNumLimitExceeded, value("limit", limit))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrDatabaseNameInvalid(database any, msg ...string) error {
	err := wrapFields(ErrDatabaseInvalidName, value("database", database))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrPrivilegeGroupNameInvalid(privilegeGroup any, msg ...string) error {
	err := wrapFields(ErrPrivilegeGroupInvalidName, value("privilegeGroup", privilegeGroup))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// Collection related
func WrapErrCollectionNotFound(collection any, msg ...string) error {
	err := wrapFields(ErrCollectionNotFound, value("collection", collection))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrCollectionIDNotFound(collection any, msg ...string) error {
	err := wrapFields(ErrCollectionIDNotFound, value("collection", collection))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrCollectionExistConflictMsg(fmt string, args ...any) error {
	return errors.Wrapf(ErrCollectionExistConflict, fmt, args...)
}

func WrapErrCollectionNotFoundWithDB(db any, collection any, msg ...string) error {
	err := wrapFields(ErrCollectionNotFound,
		value("database", db),
		value("collection", collection),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrCollectionNotLoaded(collection any, msg ...string) error {
	err := wrapFields(ErrCollectionNotLoaded, value("collection", collection))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrCollectionNumLimitExceeded(db string, limit int, msg ...string) error {
	err := wrapFields(ErrCollectionNumLimitExceeded, value("dbName", db), value("limit", limit))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrCollectionIDOfAliasNotFound(collectionID int64, msg ...string) error {
	err := wrapFields(ErrCollectionIDOfAliasNotFound, value("collectionID", collectionID))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrCollectionNotFullyLoaded(collection any, msg ...string) error {
	err := wrapFields(ErrCollectionNotFullyLoaded, value("collection", collection))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrCollectionLoaded(collection string, msgAndArgs ...any) error {
	err := wrapFields(ErrCollectionLoaded, value("collection", collection))
	if len(msgAndArgs) > 0 {
		msg := msgAndArgs[0].(string)
		err = errors.Wrapf(err, msg, msgAndArgs[1:]...)
	}
	return err
}

func WrapErrCollectionIllegalSchema(collection string, msgAndArgs ...any) error {
	err := wrapFields(ErrCollectionIllegalSchema, value("collection", collection))
	if len(msgAndArgs) > 0 {
		msg := msgAndArgs[0].(string)
		err = errors.Wrapf(err, msg, msgAndArgs[1:]...)
	}
	return err
}

// WrapErrCollectionOnRecovering wraps ErrCollectionOnRecovering with collection
func WrapErrCollectionOnRecovering(collection any, msgAndArgs ...any) error {
	err := wrapFields(ErrCollectionOnRecovering, value("collection", collection))
	if len(msgAndArgs) > 0 {
		msg := msgAndArgs[0].(string)
		err = errors.Wrapf(err, msg, msgAndArgs[1:]...)
	}
	return err
}

// WrapErrCollectionVectorClusteringKeyNotAllowed wraps ErrCollectionVectorClusteringKeyNotAllowed with collection
func WrapErrCollectionVectorClusteringKeyNotAllowed(collection any, msgAndArgs ...any) error {
	err := wrapFields(ErrCollectionVectorClusteringKeyNotAllowed, value("collection", collection))
	if len(msgAndArgs) > 0 {
		msg := msgAndArgs[0].(string)
		err = errors.Wrapf(err, msg, msgAndArgs[1:]...)
	}
	return err
}

func WrapErrCollectionSchemaMisMatch(collection any, msg ...string) error {
	err := wrapFields(ErrCollectionSchemaMismatch, value("collection", collection))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrAliasNotFound(db any, alias any, msg ...string) error {
	err := wrapFields(ErrAliasNotFound,
		value("database", db),
		value("alias", alias),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrAliasCollectionNameConflict(db any, alias any, msg ...string) error {
	err := wrapFields(ErrAliasCollectionNameConfilct,
		value("database", db),
		value("alias", alias),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrAliasAlreadyExist(db any, alias any, msg ...string) error {
	err := wrapFields(ErrAliasAlreadyExist,
		value("database", db),
		value("alias", alias),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// Partition related
func WrapErrPartitionNotFound(partition any, msg ...string) error {
	err := wrapFields(ErrPartitionNotFound, value("partition", partition))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrPartitionIDNotFound(partition any, msg ...string) error {
	err := wrapFields(ErrPartitionIDNotFound, value("partition", partition))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrPartitionNotLoaded(partition any, msg ...string) error {
	err := wrapFields(ErrPartitionNotLoaded, value("partition", partition))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrPartitionNotFullyLoaded(partition any, msg ...string) error {
	err := wrapFields(ErrPartitionNotFullyLoaded, value("partition", partition))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapGeneralCapacityExceed(newGeneralSize any, generalCapacity any, msg ...string) error {
	err := wrapFields(ErrGeneralCapacityExceeded, value("newGeneralSize", newGeneralSize),
		value("generalCapacity", generalCapacity))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// ResourceGroup related
func WrapErrResourceGroupNotFound(rg any, msg ...string) error {
	err := wrapFields(ErrResourceGroupNotFound, value("rg", rg))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// WrapErrResourceGroupAlreadyExist wraps ErrResourceGroupNotFound with resource group
func WrapErrResourceGroupAlreadyExist(rg any, msg ...string) error {
	err := wrapFields(ErrResourceGroupAlreadyExist, value("rg", rg))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// WrapErrResourceGroupReachLimit wraps ErrResourceGroupReachLimit with resource group and limit
func WrapErrResourceGroupReachLimit(rg any, limit any, msg ...string) error {
	err := wrapFields(ErrResourceGroupReachLimit, value("rg", rg), value("limit", limit))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// WrapErrResourceGroupIllegalConfig wraps ErrResourceGroupIllegalConfig with resource group
func WrapErrResourceGroupIllegalConfig(rg any, cfg any, msg ...string) error {
	err := wrapFields(ErrResourceGroupIllegalConfig, value("rg", rg), value("config", cfg))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// WrapErrStreamingNodeNotEnough make a streaming node is not enough error
func WrapErrStreamingNodeNotEnough(current int, expected int, msg ...string) error {
	err := wrapFields(ErrServiceResourceInsufficient, value("currentStreamingNode", current), value("expectedStreamingNode", expected))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// go:deprecated
// WrapErrResourceGroupNodeNotEnough wraps ErrResourceGroupNodeNotEnough with resource group
func WrapErrResourceGroupNodeNotEnough(rg any, current any, expected any, msg ...string) error {
	err := wrapFields(ErrResourceGroupNodeNotEnough, value("rg", rg), value("currentNodeNum", current), value("expectedNodeNum", expected))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// WrapErrResourceGroupServiceUnAvailable wraps ErrResourceGroupServiceUnAvailable with resource group
func WrapErrResourceGroupServiceUnAvailable(msg ...string) error {
	err := wrapFields(ErrResourceGroupServiceUnAvailable)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// Replica related
func WrapErrReplicaNotFound(id int64, msg ...string) error {
	err := wrapFields(ErrReplicaNotFound, value("replica", id))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrReplicaNotAvailable(id int64, msg ...string) error {
	err := wrapFields(ErrReplicaNotAvailable, value("replica", id))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// Channel related

func warpChannelErr(mErr milvusError, name string, msg ...string) error {
	err := wrapFields(mErr, value("channel", name))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrChannelNotFound(name string, msg ...string) error {
	return warpChannelErr(ErrChannelNotFound, name, msg...)
}

func WrapErrChannelCPExceededMaxLag(name string, msg ...string) error {
	return warpChannelErr(ErrChannelCPExceededMaxLag, name, msg...)
}

func WrapErrChannelLack(name string, msg ...string) error {
	return warpChannelErr(ErrChannelLack, name, msg...)
}

func WrapErrChannelReduplicate(name string, msg ...string) error {
	return warpChannelErr(ErrChannelReduplicate, name, msg...)
}

func WrapErrChannelNotAvailable(name string, msg ...string) error {
	return warpChannelErr(ErrChannelNotAvailable, name, msg...)
}

// Segment related
func WrapErrSegmentNotFound(id int64, msg ...string) error {
	err := wrapFields(ErrSegmentNotFound, value("segment", id))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrSegmentsNotFound(ids []int64, msg ...string) error {
	err := wrapFields(ErrSegmentNotFound, value("segments", ids))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrSegmentLoadFailed(id int64, msg ...string) error {
	err := wrapFields(ErrSegmentLoadFailed, value("segment", id))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// WrapErrSegmentRequestResourceFailed creates a resource exhaustion error for segment loading.
// resourceType should be one of: "Memory", "Disk", "GPU".
// This error triggers the query coordinator to mark the node as resource exhausted,
// applying a penalty period controlled by queryCoord.resourceExhaustionPenaltyDuration.
func WrapErrSegmentRequestResourceFailed(
	resourceType string,
	msg ...string,
) error {
	err := wrapFields(ErrSegmentRequestResourceFailed,
		value("resourceType", resourceType),
	)

	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrSegmentNotLoaded(id int64, msg ...string) error {
	err := wrapFields(ErrSegmentNotLoaded, value("segment", id))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrSegmentLack(id int64, msg ...string) error {
	err := wrapFields(ErrSegmentLack, value("segment", id))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrSegmentReduplicate(id int64, msg ...string) error {
	err := wrapFields(ErrSegmentReduplicate, value("segment", id))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// Index related
func WrapErrIndexNotFound(indexName string, msg ...string) error {
	err := wrapFields(ErrIndexNotFound, value("indexName", indexName))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrIndexNotFoundForSegments(segmentIDs []int64, msg ...string) error {
	err := wrapFields(ErrIndexNotFound, value("segmentIDs", segmentIDs))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrIndexNotFoundForCollection(collection string, msg ...string) error {
	err := wrapFields(ErrIndexNotFound, value("collection", collection))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrIndexNotSupported(indexType string, msg ...string) error {
	err := wrapFields(ErrIndexNotSupported, value("indexType", indexType))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrIndexDuplicate(indexName string, msg ...string) error {
	err := wrapFields(ErrIndexDuplicate, value("indexName", indexName))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrTaskDuplicate(taskType string, msg ...string) error {
	err := wrapFields(ErrTaskDuplicate, value("taskType", taskType))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// Node related
func WrapErrNodeNotFound(id int64, msg ...string) error {
	err := wrapFields(ErrNodeNotFound, value("node", id))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrNodeOffline(id int64, msg ...string) error {
	err := wrapFields(ErrNodeOffline, value("node", id))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrNodeLack(expectedNum, actualNum int64, msg ...string) error {
	err := wrapFields(ErrNodeLack,
		value("expectedNum", expectedNum),
		value("actualNum", actualNum),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrNodeLackAny(msg ...string) error {
	err := error(ErrNodeLack)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrNodeNotAvailable(id int64, msg ...string) error {
	err := wrapFields(ErrNodeNotAvailable, value("node", id))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrNodeStateUnexpected(id int64, state string, msg ...string) error {
	err := wrapFields(ErrNodeStateUnexpected, value("node", id), value("state", state))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrNodeNotMatch(expectedNodeID, actualNodeID int64, msg ...string) error {
	err := wrapFields(ErrNodeNotMatch,
		value("expectedNodeID", expectedNodeID),
		value("actualNodeID", actualNodeID),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// IO related
func WrapErrIoKeyNotFound(key string, msg ...string) error {
	err := wrapFields(ErrIoKeyNotFound, value("key", key))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrIoFailed(key string, err error) error {
	if err == nil {
		return nil
	}
	return wrapFieldsWithDesc(ErrIoFailed, err.Error(), value("key", key))
}

func WrapErrIoFailedReason(reason string, msg ...string) error {
	err := wrapFieldsWithDesc(ErrIoFailed, reason)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrIoUnexpectEOF(key string, err error) error {
	if err == nil {
		return nil
	}
	return wrapFieldsWithDesc(ErrIoUnexpectEOF, err.Error(), value("key", key))
}

func WrapErrIoTooManyRequests(key string, err error) error {
	if err == nil {
		return nil
	}
	return wrapFieldsWithDesc(ErrIoTooManyRequests, err.Error(), value("key", key))
}

// Parameter related
func WrapErrParameterInvalid[T any](expected, actual T, msg ...string) error {
	err := wrapFields(ErrParameterInvalid,
		value("expected", expected),
		value("actual", actual),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// WrapErrParameterInvalidErr wraps an existing error 'err' with ErrParameterInvalid (Code 1005).
// This is used when an underlying error (e.g., from parsing, validation utility, or dependency)
// causes a parameter check to fail, and you need to provide extra context
// in 'format' and 'args'.
func WrapErrParameterInvalidErr(err error, format string, args ...any) error {
	if err == nil {
		// If the underlying error is nil, we fall back to the message-only version.
		return WrapErrParameterInvalidMsg(format, args...)
	}

	// 1. Create the contextual message chain: "Context message" -> "Original error message"
	contextualMsg := fmt.Sprintf(format, args...)

	// 2. Wrap the underlying error first to include the context.
	errWithContext := errors.Wrap(err, contextualMsg)

	// 3. Wrap the resulting error chain with the Milvus constant (ErrParameterInvalid)
	// to ensure 'merr.Is(err, merr.ErrParameterInvalid)' works correctly and the code (1005) is preserved.
	// This makes ErrParameterInvalid the topmost error in the Milvus system for parameter issues.
	return errors.Wrap(ErrParameterInvalid, errWithContext.Error())
}

func WrapErrParameterInvalidRange[T any](lower, upper, actual T, msg ...string) error {
	err := wrapFields(ErrParameterInvalid,
		bound("value", actual, lower, upper),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrParameterInvalidMsg(fmt string, args ...any) error {
	return errors.Wrapf(ErrParameterInvalid, fmt, args...)
}

func WrapErrParameterMissing[T any](param T, msg ...string) error {
	err := wrapFields(ErrParameterMissing,
		value("missing_param", param),
	)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrParameterMissingMsg(fmt string, args ...any) error {
	return errors.Wrapf(ErrParameterMissing, fmt, args...)
}

func WrapErrParameterTooLarge(name string, msg ...string) error {
	err := wrapFields(ErrParameterTooLarge, value("message", name))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// Metrics related
func WrapErrMetricNotFound(name string, msg ...string) error {
	err := wrapFields(ErrMetricNotFound, value("metric", name))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// Message queue related
func WrapErrMqTopicNotFound(name string, msg ...string) error {
	err := wrapFields(ErrMqTopicNotFound, value("topic", name))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrMqTopicNotEmpty(name string, msg ...string) error {
	err := wrapFields(ErrMqTopicNotEmpty, value("topic", name))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrMqInternal(err error, msg ...string) error {
	err = wrapFieldsWithDesc(ErrMqInternal, err.Error())
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrPrivilegeNotAuthenticated(fmt string, args ...any) error {
	err := errors.Wrapf(ErrPrivilegeNotAuthenticated, fmt, args...)
	return err
}

func WrapErrPrivilegeNotPermitted(fmt string, args ...any) error {
	err := errors.Wrapf(ErrPrivilegeNotPermitted, fmt, args...)
	return err
}

// Segcore related
func WrapErrSegcore(code int32, msg ...string) error {
	err := wrapFields(ErrSegcore, value("segcoreCode", code))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrSegcoreUnsupported(code int32, msg ...string) error {
	err := wrapFields(ErrSegcoreUnsupported, value("segcoreCode", code))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// field related
func WrapErrFieldNotFound[T any](field T, msg ...string) error {
	err := wrapFields(ErrFieldNotFound, value("field", field))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrFieldNameInvalid(field any, msg ...string) error {
	err := wrapFields(ErrFieldInvalidName, value("field", field))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func wrapFields(err milvusError, fields ...errorField) error {
	for i := range fields {
		err.msg += fmt.Sprintf("[%s]", fields[i].String())
	}
	err.detail = err.msg
	return err
}

func wrapFieldsWithDesc(err milvusError, desc string, fields ...errorField) error {
	for i := range fields {
		err.msg += fmt.Sprintf("[%s]", fields[i].String())
	}
	err.msg += ": " + desc
	err.detail = err.msg
	return err
}

type errorField interface {
	String() string
}

type valueField struct {
	name  string
	value any
}

func value(name string, value any) valueField {
	return valueField{
		name,
		value,
	}
}

func (f valueField) String() string {
	return fmt.Sprintf("%s=%v", f.name, f.value)
}

type boundField struct {
	name  string
	value any
	lower any
	upper any
}

func bound(name string, value, lower, upper any) boundField {
	return boundField{
		name,
		value,
		lower,
		upper,
	}
}

func (f boundField) String() string {
	return fmt.Sprintf("%v out of range %v <= %s <= %v", f.value, f.lower, f.name, f.upper)
}

func WrapErrImportFailed(msg ...string) error {
	err := error(ErrImportFailed)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrImportFailedErr(err error, format string, args ...any) error {
	if err == nil {
		// If the underlying error is nil, we fall back to the message-only version.
		return WrapErrImportFailedMsg(format, args...)
	}

	// 1. Create the contextual message chain: "Context message" -> "Original error message"
	contextualMsg := fmt.Sprintf(format, args...)

	// 2. Wrap the underlying error first to include the context.
	errWithContext := errors.Wrap(err, contextualMsg)

	// 3. Wrap the resulting error chain with the Milvus constant (ErrImportFailed)
	// to ensure 'merr.Is(err, merr.ErrImportFailed)' works correctly.
	// This makes ErrImportFailed the topmost error for import-related failures.
	return errors.Wrap(ErrImportFailed, errWithContext.Error())
}

func WrapErrImportFailedMsg(format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	// Since this is the message-only path, we wrap the constant error with the formatted message.
	return errors.Wrap(ErrImportFailed, detail)
}

func WrapErrImportSysFailed(msg ...string) error {
	err := error(ErrImportSysFailed)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrImportSysFailedErr(err error, format string, args ...any) error {
	if err == nil {
		// If the underlying error is nil, we fall back to the message-only version.
		return WrapErrImportSysFailedMsg(format, args...)
	}

	// 1. Create the contextual message chain: "Context message" -> "Original error message"
	contextualMsg := fmt.Sprintf(format, args...)

	// 2. Wrap the underlying error first to include the context.
	errWithContext := errors.Wrap(err, contextualMsg)

	// 3. Wrap the resulting error chain with the Milvus constant (ErrImportSysFailed)
	// to ensure 'merr.Is(err, merr.ErrImportSysFailed)' works correctly and the specific
	// system error code is preserved as the root cause in the Milvus system.
	return errors.Wrap(ErrImportSysFailed, errWithContext.Error())
}

func WrapErrImportSysFailedMsg(format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	// Since this is the message-only path, we wrap the constant error with the formatted message.
	return errors.Wrap(ErrImportSysFailed, detail)
}

func WrapErrInconsistentRequery(msg ...string) error {
	err := error(ErrInconsistentRequery)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrKMSKeyRevoked(dbID int64, reason string) error {
	return wrapFields(ErrKMSKeyRevoked,
		value("dbID", dbID),
		value("reason", reason))
}

func WrapErrCompactionReadDeltaLogErr(msg ...string) error {
	err := error(ErrCompactionReadDeltaLogErr)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrIllegalCompactionPlan(msg ...string) error {
	err := error(ErrIllegalCompactionPlan)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrIllegalCompactionPlanMsg(format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	// Since this is the message-only path, we wrap the constant error with the formatted message.
	return errors.Wrap(ErrIllegalCompactionPlan, detail)
}

func WrapErrCompactionPlanConflict(msg ...string) error {
	err := error(ErrCompactionPlanConflict)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrCompactionResultNotFound(msg ...string) error {
	err := error(ErrCompactionResultNotFound)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrClusteringCompactionGetCollectionFail(collectionID int64, err error) error {
	return wrapFieldsWithDesc(ErrClusteringCompactionGetCollectionFail, err.Error(), value("collectionID", collectionID))
}

func WrapErrClusteringCompactionClusterNotSupport(msg ...string) error {
	err := error(ErrClusteringCompactionClusterNotSupport)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrClusteringCompactionCollectionNotSupport(msg ...string) error {
	err := error(ErrClusteringCompactionCollectionNotSupport)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrClusteringCompactionNotSupportVector(msg ...string) error {
	err := error(ErrClusteringCompactionNotSupportVector)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrClusteringCompactionSubmitTaskFail(taskType string, err error) error {
	if err == nil {
		return nil
	}
	return wrapFieldsWithDesc(ErrClusteringCompactionSubmitTaskFail, err.Error(), value("taskType", taskType))
}

func WrapErrClusteringCompactionMetaError(operation string, err error) error {
	return wrapFieldsWithDesc(ErrClusteringCompactionMetaError, err.Error(), value("operation", operation))
}

func WrapErrCleanPartitionStatsFail(msg ...string) error {
	err := error(ErrCleanPartitionStatsFail)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrAnalyzeTaskNotFound(id int64) error {
	return wrapFields(ErrAnalyzeTaskNotFound, value("analyzeId", id))
}

func WrapErrBuildCompactionRequestFail(err error) error {
	return wrapFieldsWithDesc(ErrBuildCompactionRequestFail, err.Error())
}

func WrapErrGetCompactionPlanResultFail(err error) error {
	return wrapFieldsWithDesc(ErrGetCompactionPlanResultFail, err.Error())
}

func WrapErrCompactionResult(msg ...string) error {
	err := error(ErrCompactionResult)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrDataNodeSlotExhausted(msg ...string) error {
	err := error(ErrDataNodeSlotExhausted)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrDuplicatedCompactionTask(msg ...string) error {
	err := error(ErrDuplicatedCompactionTask)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrOldSessionExists(msg ...string) error {
	err := error(ErrOldSessionExists)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// WrapErrSerializationFailedMsg creates a new ErrSerializationFailed with a detail message (Code 1003).
// This is used when the serialization fails directly without an underlying Go error to wrap.
func WrapErrSerializationFailedMsg(format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	// Since this is the message-only path, we wrap the constant error with the formatted message.
	return errors.Wrap(ErrSerializationFailed, detail)
}

// WrapErrSerializationFailed wraps an existing error 'err' with ErrSerializationFailed (Code 1003).
// This is used when an underlying error (e.g., from io or a library) caused the serialization/deserialization to fail,
// and you need to provide extra context in 'format' and 'args'.
func WrapErrSerializationFailed(err error, format string, args ...any) error {
	if err == nil {
		// If the underlying error is nil, we fall back to the message-only version.
		return WrapErrSerializationFailedMsg(format, args...)
	}

	// 1. Create the contextual message chain: "Context message" -> "Original error message"
	contextualMsg := fmt.Sprintf(format, args...)

	// 2. Wrap the underlying error first to include the context.
	errWithContext := errors.Wrap(err, contextualMsg)

	// 3. Wrap the resulting error chain with the Milvus constant (ErrSerializationFailed)
	// to ensure 'merr.Is(err, merr.ErrSerializationFailed)' works correctly.
	// This makes ErrSerializationFailed the topmost error in the Milvus system.
	return errors.Wrap(ErrSerializationFailed, errWithContext.Error())
}

// WrapErrFunctionFailedMsg creates a new ErrFunctionFailed with a detail message (Code 4000).
// This is used when the function execution fails directly without an underlying Go error to wrap.
func WrapErrFunctionFailedMsg(format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	// Since this is the message-only path, we wrap the constant error with the formatted message.
	return errors.Wrap(ErrFunctionFailed, detail)
}

// WrapErrFunctionFailed wraps an existing error 'err' with ErrFunctionFailed (Code 4000).
// This is used when an underlying error (e.g., from an external API call or a dependency)
// caused the function execution to fail, and you need to provide extra context in 'format' and 'args'.
func WrapErrFunctionFailed(err error, format string, args ...any) error {
	if err == nil {
		// If the underlying error is nil, we fall back to the message-only version.
		return WrapErrFunctionFailedMsg(format, args...)
	}

	// 1. Create the contextual message chain: "Context message" -> "Original error message"
	contextualMsg := fmt.Sprintf(format, args...)

	// 2. Wrap the underlying error first to include the context.
	errWithContext := errors.Wrap(err, contextualMsg)

	// 3. Wrap the resulting error chain with the Milvus constant (ErrFunctionFailed)
	// to ensure 'merr.Is(err, merr.ErrFunctionFailed)' works correctly.
	// This makes ErrFunctionFailed the topmost error in the Milvus system.
	return errors.Wrap(ErrFunctionFailed, errWithContext.Error())
}

// WrapErrQueryPlanMsg creates a new ErrQueryPlan with a detail message (Code 4100).
// This is used when the query plan parsing/validation fails directly
// without an underlying Go error to wrap.
func WrapErrQueryPlanMsg(format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	// Since this is the message-only path, we wrap the constant error with the formatted message.
	return errors.Wrap(ErrQueryPlan, detail)
}

// WrapErrQueryPlan wraps an existing error 'err' with ErrQueryPlan (Code 4100).
// This is used when an underlying error (e.g., from a lower-level component or dependency)
// caused the query plan process to fail, and you need to provide extra context
// in 'format' and 'args'.
func WrapErrQueryPlan(err error, format string, args ...any) error {
	if err == nil {
		// If the underlying error is nil, we fall back to the message-only version.
		return WrapErrQueryPlanMsg(format, args...)
	}

	// 1. Create the contextual message chain: "Context message" -> "Original error message"
	contextualMsg := fmt.Sprintf(format, args...)

	// 2. Wrap the underlying error first to include the context.
	errWithContext := errors.Wrap(err, contextualMsg)

	// 3. Wrap the resulting error chain with the Milvus constant (ErrQueryPlan)
	// to ensure 'merr.Is(err, merr.ErrQueryPlan)' works correctly.
	// This makes ErrQueryPlan the topmost error in the Milvus system.
	return errors.Wrap(ErrQueryPlan, errWithContext.Error())
}

// WrapErrStorageMsg creates a new ErrStorage with a detail message (Code 4200).
// This is used when a logical internal error occurs in the storage layer (e.g., invalid state,
// corrupted data structure check, nil data) and there is no underlying Go error to wrap.
func WrapErrStorageMsg(format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	// Since this is the message-only path, we wrap the constant error with the formatted message.
	return errors.Wrap(ErrStorage, detail)
}

// WrapErrStorage wraps an existing underlying error 'err' with ErrStorage (Code 4200).
// This is used for I/O errors, underlying file system failures, Arrow library errors,
// or chunk manager communication failures, preserving the error chain and adding context.
func WrapErrStorage(err error, format string, args ...any) error {
	if err == nil {
		// If the underlying error is nil, we fall back to the message-only version.
		return WrapErrStorageMsg(format, args...)
	}

	// 1. Create the contextual message chain: "Context message" -> "Original error message"
	contextualMsg := fmt.Sprintf(format, args...)

	// 2. Wrap the underlying error first to include the context.
	errWithContext := errors.Wrap(err, contextualMsg)

	// 3. Wrap the resulting error chain with the Milvus constant (ErrStorage)
	// to ensure 'merr.Is(err, merr.ErrStorage)' works correctly and the code (4200) is preserved.
	// This makes ErrStorage the topmost error in the Milvus system for storage issues.
	return errors.Wrap(ErrStorage, errWithContext.Error())
}

// WrapErrRbacMsg creates a new ErrRbac with a detail message (Code 1800).
// This is used for RBAC validation errors (e.g., role not found, invalid username)
// where no underlying Go error exists.
func WrapErrRbacMsg(format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	return errors.Wrap(ErrRbac, detail)
}

// WrapErrRbac wraps an existing error 'err' with ErrRbac (Code 1800).
func WrapErrRbac(err error, format string, args ...any) error {
	if err == nil {
		return WrapErrRbacMsg(format, args...)
	}
	contextualMsg := fmt.Sprintf(format, args...)
	errWithContext := errors.Wrap(err, contextualMsg)
	return errors.Wrap(ErrRbac, errWithContext.Error())
}

func WrapErrConnectComponentMsg(fmt string, args ...any) error {
	return errors.Wrapf(ErrConnectComponent, fmt, args...)
}

func WrapErrConnectComponent(err error, format string, args ...any) error {
	if err == nil {
		// If the underlying error is nil, we fall back to the message-only version.
		return WrapErrConnectComponentMsg(format, args...)
	}

	// 1. Create the contextual message chain: "Context message" -> "Original error message"
	contextualMsg := fmt.Sprintf(format, args...)

	// 2. Wrap the underlying error first to include the context.
	errWithContext := errors.Wrap(err, contextualMsg)

	// 3. Wrap the resulting error chain with the Milvus constant (ErrParameterInvalid)
	// to ensure 'merr.Is(err, merr.ErrParameterInvalid)' works correctly and the code (1005) is preserved.
	// This makes ErrParameterInvalid the topmost error in the Milvus system for parameter issues.
	return errors.Wrap(ErrConnectComponent, errWithContext.Error())
}

func WrapErrHTTPBadRequestMsg(fmt string, args ...any) error {
	return errors.Wrapf(ErrHTTPBadRequest, fmt, args...)
}

func WrapErrHTTPBadRequest(err error, format string, args ...any) error {
	if err == nil {
		// If the underlying error is nil, we fall back to the message-only version.
		return WrapErrHTTPBadRequestMsg(format, args...)
	}

	// 1. Create the contextual message chain: "Context message" -> "Original error message"
	contextualMsg := fmt.Sprintf(format, args...)

	// 2. Wrap the underlying error first to include the context.
	errWithContext := errors.Wrap(err, contextualMsg)

	// 3. Wrap the resulting error chain with the Milvus constant (ErrParameterInvalid)
	// to ensure 'merr.Is(err, merr.ErrParameterInvalid)' works correctly and the code (1005) is preserved.
	// This makes ErrParameterInvalid the topmost error in the Milvus system for parameter issues.
	return errors.Wrap(ErrHTTPBadRequest, errWithContext.Error())
}

// WrapErrOperationNotSupportedMsg creates a new ErrOperationNotSupported with a detail message (Code 14).
// This is the primary replacement for fmt.Errorf/errors.New for operations that are
// currently not supported by the system.
func WrapErrOperationNotSupportedMsg(format string, args ...any) error {
	return errors.Wrapf(ErrOperationNotSupported, format, args...)
}

// WrapErrOperationNotSupported wraps an existing error 'err' with ErrOperationNotSupported (Code 14).
func WrapErrOperationNotSupported(err error, format string, args ...any) error {
	if err == nil {
		return WrapErrOperationNotSupportedMsg(format, args...)
	}

	contextualMsg := fmt.Sprintf(format, args...)
	// Wrap the original error with the new context
	errWithContext := errors.Wrap(err, contextualMsg)
	// Chain it to the base ErrOperationNotSupported
	return errors.Wrap(ErrOperationNotSupported, errWithContext.Error())
}
