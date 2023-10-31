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
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// Code returns the error code of the given error,
// WARN: DO NOT use this for now
func Code(err error) int32 {
	if err == nil {
		return 0
	}

	cause := errors.Cause(err)
	switch cause := cause.(type) {
	case milvusError:
		return cause.code()

	default:
		if errors.Is(cause, context.Canceled) {
			return CanceledCode
		} else if errors.Is(cause, context.DeadlineExceeded) {
			return TimeoutCode
		} else {
			return errUnexpected.code()
		}
	}
}

func IsRetryableErr(err error) bool {
	return IsRetryableCode(Code(err))
}

func IsRetryableCode(code int32) bool {
	return code&retryableFlag != 0
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
	return &commonpb.Status{
		Code:   code,
		Reason: err.Error(),
		// Deprecated, for compatibility
		ErrorCode: oldCode(code),
	}
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

	case ErrCollectionNotFound.code(), ErrPartitionNotFound.code(), ErrReplicaNotFound.code():
		return commonpb.ErrorCode_MetaFailed

	case ErrReplicaNotAvailable.code(), ErrChannelNotAvailable.code(), ErrNodeNotAvailable.code():
		return commonpb.ErrorCode_NoReplicaAvailable

	case ErrServiceMemoryLimitExceeded.code():
		return commonpb.ErrorCode_InsufficientMemoryToLoad

	case ErrServiceRateLimit.code():
		return commonpb.ErrorCode_RateLimit

	case ErrServiceForceDeny.code():
		return commonpb.ErrorCode_ForceDeny

	case ErrIndexNotFound.code():
		return commonpb.ErrorCode_IndexNotExist

	case ErrSegmentNotFound.code():
		return commonpb.ErrorCode_SegmentNotFound

	case ErrChannelLack.code():
		return commonpb.ErrorCode_MetaFailed

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

	case commonpb.ErrorCode_RateLimit:
		return ErrServiceRateLimit

	case commonpb.ErrorCode_ForceDeny:
		return ErrServiceForceDeny

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

	// use code first
	code := status.GetCode()
	if code == 0 {
		return newMilvusError(status.GetReason(), Code(OldCodeToMerr(status.GetErrorCode())), false)
	}
	return newMilvusError(status.GetReason(), code, code&retryableFlag != 0)
}

// CheckHealthy checks whether the state is healthy,
// returns nil if healthy,
// otherwise returns ErrServiceNotReady wrapped with current state
func CheckHealthy(state commonpb.StateCode) error {
	if state != commonpb.StateCode_Healthy {
		return WrapErrServiceNotReady(paramtable.GetRole(), paramtable.GetNodeID(), state.String())
	}

	return nil
}

// CheckHealthyStandby checks whether the state is healthy or standby,
// returns nil if healthy or standby
// otherwise returns ErrServiceNotReady wrapped with current state
// this method only used in GetMetrics
func CheckHealthyStandby(state commonpb.StateCode) error {
	if state != commonpb.StateCode_Healthy && state != commonpb.StateCode_StandBy {
		return WrapErrServiceNotReady(paramtable.GetRole(), paramtable.GetNodeID(), state.String())
	}

	return nil
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

func AnalyzeState(role string, nodeID int64, state *milvuspb.ComponentStates) error {
	if err := Error(state.GetStatus()); err != nil {
		return errors.Wrapf(err, "%s=%d not healthy", role, nodeID)
	} else if state := state.GetState().GetStateCode(); state != commonpb.StateCode_Healthy {
		return WrapErrServiceNotReady(role, nodeID, state.String())
	}

	return nil
}

func CheckTargetID(msg *commonpb.MsgBase) error {
	if msg.GetTargetID() != paramtable.GetNodeID() {
		return WrapErrNodeNotMatch(paramtable.GetNodeID(), msg.GetTargetID())
	}

	return nil
}

// Service related
func WrapErrServiceNotReady(role string, sessionID int64, state string, msg ...string) error {
	err := errors.Wrapf(ErrServiceNotReady, "%s=%d stage=%s", role, sessionID, state)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrServiceUnavailable(reason string, msg ...string) error {
	err := errors.Wrap(ErrServiceUnavailable, reason)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrServiceMemoryLimitExceeded(predict, limit float32, msg ...string) error {
	err := errors.Wrapf(ErrServiceMemoryLimitExceeded, "predict=%v, limit=%v", predict, limit)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrServiceRequestLimitExceeded(limit int32, msg ...string) error {
	err := errors.Wrapf(ErrServiceRequestLimitExceeded, "limit=%v", limit)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrServiceInternal(msg string, others ...string) error {
	msg = strings.Join(append([]string{msg}, others...), "; ")
	err := errors.Wrap(ErrServiceInternal, msg)

	return err
}

func WrapErrServiceCrossClusterRouting(expectedCluster, actualCluster string, msg ...string) error {
	err := errors.Wrapf(ErrServiceCrossClusterRouting, "expectedCluster=%s, actualCluster=%s", expectedCluster, actualCluster)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrServiceDiskLimitExceeded(predict, limit float32, msg ...string) error {
	err := errors.Wrapf(ErrServiceDiskLimitExceeded, "predict=%v, limit=%v", predict, limit)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrServiceRateLimit(rate float64) error {
	err := errors.Wrapf(ErrServiceRateLimit, "rate=%v", rate)
	return err
}

func WrapErrServiceForceDeny(op string, reason error, method string) error {
	err := errors.Wrapf(ErrServiceForceDeny, "deny to %s, reason: %s, req: %s", op, reason.Error(), method)
	return err
}

func WrapErrServiceUnimplemented(grpcErr error) error {
	err := errors.Wrapf(ErrServiceUnimplemented, "err: %s", grpcErr.Error())
	return err
}

// database related
func WrapErrDatabaseNotFound(database any, msg ...string) error {
	err := wrapWithField(ErrDatabaseNotFound, "database", database)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrDatabaseResourceLimitExceeded(msg ...string) error {
	var err error = ErrDatabaseNumLimitExceeded
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrDatabaseNameInvalid(database any, msg ...string) error {
	err := wrapWithField(ErrDatabaseInvalidName, "database", database)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// Collection related
func WrapErrCollectionNotFound(collection any, msg ...string) error {
	err := wrapWithField(ErrCollectionNotFound, "collection", collection)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrCollectionNotFoundWithDB(db any, collection any, msg ...string) error {
	err := errors.Wrapf(ErrCollectionNotFound, "collection %v:%v", db, collection)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrCollectionNotLoaded(collection any, msg ...string) error {
	err := wrapWithField(ErrCollectionNotLoaded, "collection", collection)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrCollectionResourceLimitExceeded(msg ...string) error {
	var err error = ErrCollectionNumLimitExceeded
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrCollectionNotFullyLoaded(collection any, msg ...string) error {
	err := wrapWithField(ErrCollectionNotFullyLoaded, "collection", collection)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrAliasNotFound(db any, alias any, msg ...string) error {
	err := errors.Wrapf(ErrAliasNotFound, "alias %v:%v", db, alias)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrAliasCollectionNameConflict(db any, alias any, msg ...string) error {
	err := errors.Wrapf(ErrAliasCollectionNameConfilct, "alias %v:%v", db, alias)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrAliasAlreadyExist(db any, alias any, msg ...string) error {
	err := errors.Wrapf(ErrAliasAlreadyExist, "alias %v:%v already exist", db, alias)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// Partition related
func WrapErrPartitionNotFound(partition any, msg ...string) error {
	err := wrapWithField(ErrPartitionNotFound, "partition", partition)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrPartitionNotLoaded(partition any, msg ...string) error {
	err := wrapWithField(ErrPartitionNotLoaded, "partition", partition)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrPartitionNotFullyLoaded(partition any, msg ...string) error {
	err := wrapWithField(ErrPartitionNotFullyLoaded, "partition", partition)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// ResourceGroup related
func WrapErrResourceGroupNotFound(rg any, msg ...string) error {
	err := wrapWithField(ErrResourceGroupNotFound, "rg", rg)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// Replica related
func WrapErrReplicaNotFound(id int64, msg ...string) error {
	err := wrapWithField(ErrReplicaNotFound, "replica", id)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrReplicaNotAvailable(id int64, msg ...string) error {
	err := wrapWithField(ErrReplicaNotAvailable, "replica", id)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// Channel related
func WrapErrChannelNotFound(name string, msg ...string) error {
	err := wrapWithField(ErrChannelNotFound, "channel", name)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrChannelLack(name string, msg ...string) error {
	err := wrapWithField(ErrChannelLack, "channel", name)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrChannelReduplicate(name string, msg ...string) error {
	err := wrapWithField(ErrChannelReduplicate, "channel", name)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrChannelNotAvailable(name string, msg ...string) error {
	err := wrapWithField(ErrChannelNotAvailable, "channel", name)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// Segment related
func WrapErrSegmentNotFound(id int64, msg ...string) error {
	err := wrapWithField(ErrSegmentNotFound, "segment", id)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrSegmentNotLoaded(id int64, msg ...string) error {
	err := wrapWithField(ErrSegmentNotLoaded, "segment", id)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrSegmentLack(id int64, msg ...string) error {
	err := wrapWithField(ErrSegmentLack, "segment", id)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrSegmentReduplicate(id int64, msg ...string) error {
	err := wrapWithField(ErrSegmentReduplicate, "segment", id)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// Index related
func WrapErrIndexNotFound(indexName string, msg ...string) error {
	err := wrapWithField(ErrIndexNotFound, "indexName", indexName)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrIndexNotFoundForSegment(segmentID int64, msg ...string) error {
	err := wrapWithField(ErrIndexNotFound, "segmentID", segmentID)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrIndexNotFoundForCollection(collection string, msg ...string) error {
	err := wrapWithField(ErrIndexNotFound, "collection", collection)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrIndexNotSupported(indexType string, msg ...string) error {
	err := wrapWithField(ErrIndexNotSupported, "indexType", indexType)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrIndexDuplicate(indexName string, msg ...string) error {
	err := wrapWithField(ErrIndexDuplicate, "indexName", indexName)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// Node related
func WrapErrNodeNotFound(id int64, msg ...string) error {
	err := wrapWithField(ErrNodeNotFound, "node", id)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrNodeOffline(id int64, msg ...string) error {
	err := wrapWithField(ErrNodeOffline, "node", id)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrNodeLack(expectedNum, actualNum int64, msg ...string) error {
	err := errors.Wrapf(ErrNodeLack, "expectedNum=%d, actualNum=%d", expectedNum, actualNum)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrNodeLackAny(msg ...string) error {
	err := error(ErrNodeLack)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrNodeNotAvailable(id int64, msg ...string) error {
	err := wrapWithField(ErrNodeNotAvailable, "node", id)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrNodeNotMatch(expectedNodeID, actualNodeID int64, msg ...string) error {
	err := errors.Wrapf(ErrNodeNotMatch, "expectedNodeID=%d, actualNodeID=%d", expectedNodeID, actualNodeID)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// IO related
func WrapErrIoKeyNotFound(key string, msg ...string) error {
	err := errors.Wrapf(ErrIoKeyNotFound, "key=%s", key)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrIoFailed(key string, err error) error {
	if err == nil {
		return nil
	}
	err = errors.Wrapf(ErrIoFailed, "key=%s: %v", key, err)
	return err
}

func WrapErrIoFailedReason(reason string, msg ...string) error {
	err := errors.Wrapf(ErrIoFailed, reason)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// Parameter related
func WrapErrParameterInvalid[T any](expected, actual T, msg ...string) error {
	err := errors.Wrapf(ErrParameterInvalid, "expected=%v, actual=%v", expected, actual)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrParameterInvalidRange[T any](lower, upper, actual T, msg ...string) error {
	err := errors.Wrapf(ErrParameterInvalid, "expected in [%v, %v], actual=%v", lower, upper, actual)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrParameterInvalidMsg(fmt string, args ...any) error {
	err := errors.Wrapf(ErrParameterInvalid, fmt, args...)
	return err
}

// Metrics related
func WrapErrMetricNotFound(name string, msg ...string) error {
	err := errors.Wrapf(ErrMetricNotFound, "metric=%s", name)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// Message queue related
func WrapErrMqTopicNotFound(name string, msg ...string) error {
	err := errors.Wrapf(ErrMqTopicNotFound, "topic=%s", name)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrMqTopicNotEmpty(name string, msg ...string) error {
	err := errors.Wrapf(ErrMqTopicNotEmpty, "topic=%s", name)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrMqInternal(err error, msg ...string) error {
	err = errors.Wrapf(ErrMqInternal, "internal=%v", err)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
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
	err := errors.Wrapf(ErrSegcore, "internal code=%v", code)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// field related
func WrapErrFieldNotFound[T any](field T, msg ...string) error {
	err := errors.Wrapf(ErrFieldNotFound, "field=%v", field)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrFieldNameInvalid(field any, msg ...string) error {
	err := wrapWithField(ErrFieldInvalidName, "field", field)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func wrapWithField(err error, name string, value any) error {
	return errors.Wrapf(err, "%s=%v", name, value)
}
