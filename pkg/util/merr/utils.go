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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

const InputErrorFlagKey string = "is_input_error"

// Code returns the error code of the given error,
// WARN: DO NOT use this for now
func Code(err error) int32 {
	if err == nil {
		return 0
	}

	// Walk the chain for the first milvusError (a sentinel, a wrapFields value,
	// or a relabeling milvusError-with-inner). Stopping at the first match means
	// a relabel reports its own code, not the inner's.
	for cur := err; cur != nil; cur = errors.Unwrap(cur) {
		if me, ok := cur.(milvusError); ok {
			return me.code()
		}
	}

	cause := errors.Cause(err)
	if errors.Is(cause, context.Canceled) {
		return CanceledCode
	} else if errors.Is(cause, context.DeadlineExceeded) {
		return TimeoutCode
	}
	return errUnexpected.code()
}

func IsRetryableErr(err error) bool {
	var milvusErr milvusError
	if errors.As(err, &milvusErr) {
		return milvusErr.retriable
	}

	return false
}

// IsNonRetryableErr checks if an error is non-retryable (denylist approach).
// Returns true for permanent errors (resource not found, permission denied)
// and client validation errors (invalid argument, invalid range).
// All other errors are considered retryable (including nil).
func IsNonRetryableErr(err error) bool {
	if err == nil {
		return false
	}

	// Permanent errors - resource doesn't exist or access denied
	if errors.Is(err, ErrIoKeyNotFound) ||
		errors.Is(err, ErrIoPermissionDenied) ||
		errors.Is(err, ErrIoBucketNotFound) ||
		errors.Is(err, ErrIoInvalidCredentials) {
		return true
	}

	// Client validation errors - request is malformed
	if errors.Is(err, ErrIoInvalidArgument) ||
		errors.Is(err, ErrIoInvalidRange) ||
		errors.Is(err, ErrIoEntityTooLarge) {
		return true
	}

	return false
}

func IsMilvusError(err error) bool {
	var me milvusError
	return errors.As(err, &me)
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
		Code: code,
		// Reason is the SDK/REST-visible message: the full composed chain,
		// outermost context first ("outer context: ...: root cause"). The
		// previous leaf-oriented heuristic (previousLastError) dropped the
		// actionable outer context whenever the root cause was itself a
		// nested error (e.g. strconv.NumError wrapping ErrSyntax), leaving
		// clients with only the raw low-level cause.
		Reason: err.Error(),
		// Deprecated, for compatibility
		ErrorCode: oldCode(code),
		Retriable: IsRetryableErr(err),
		Detail:    err.Error(),
	}

	if GetErrorType(err) == InputError {
		status.ExtraInfo = map[string]string{InputErrorFlagKey: "true"}
		// Invariant enforced at the proxy boundary: an input error means the
		// request is malformed, so retrying it unchanged can never succeed.
		// Force Retriable=false even if the underlying sentinel is retriable,
		// so clients never receive the self-contradictory "your input is wrong
		// but you may retry" signal.
		status.Retriable = false
	}
	return status
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

	// Reuse Status so Retriable / Detail / the is_input_error flag are populated
	// (and Retriable forced false for input errors). Otherwise the ~31 rootcoord
	// RBAC/credential callers would round-trip genuine input errors as system.
	// Only override the explicit wire ErrorCode the caller asked for.
	st := Status(err)
	st.ErrorCode = code
	return st
}

func oldCode(code int32) commonpb.ErrorCode {
	switch code {
	case ErrServiceNotReady.code(), ErrCollectionSchemaVersionNotReady.code():
		return commonpb.ErrorCode_NotReadyServe

	case ErrCollectionNotFound.code():
		return commonpb.ErrorCode_CollectionNotExists

	case ErrParameterInvalid.code(), ErrParameterMissing.code(), ErrParameterTooLarge.code():
		// The legacy contract is that every parameter-class error surfaces as
		// IllegalArgument, so the finer-grained 1101/1102 codes must not regress
		// old SDKs (which still read the deprecated ErrorCode) to UnexpectedError.
		return commonpb.ErrorCode_IllegalArgument

	case ErrNodeNotMatch.code():
		return commonpb.ErrorCode_NodeIDNotMatch

	case ErrPartitionNotFound.code(), ErrReplicaNotFound.code():
		return commonpb.ErrorCode_MetaFailed

	case ErrReplicaNotAvailable.code(), ErrChannelNotAvailable.code(), ErrChannelDroppedSentinel.code(), ErrNodeNotAvailable.code():
		// ErrChannelDroppedSentinel is an internal-only signal that is currently
		// swallowed by the alter-load-config ack callback and never serialized to a
		// client Status. It is mapped alongside its ErrChannelNotAvailable sibling so
		// that, if it is ever surfaced to a client, an old SDK keeps seeing
		// NoReplicaAvailable instead of falling through to UnexpectedError.
		return commonpb.ErrorCode_NoReplicaAvailable

	case ErrServiceMemoryLimitExceeded.code():
		return commonpb.ErrorCode_InsufficientMemoryToLoad

	case ErrServiceDiskLimitExceeded.code():
		return commonpb.ErrorCode_DiskQuotaExhausted

	case ErrServiceTimeTickLongDelay.code(), ErrChannelTSafeStalled.code():
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
	if status.GetExtraInfo()[InputErrorFlagKey] == "true" {
		eType = InputError
	}

	// use code first
	code := status.GetCode()
	if code == 0 {
		return makeMilvusError(status.GetReason(), Code(OldCodeToMerr(status.GetErrorCode())), false, WithDetail(status.GetDetail()), WithErrorType(eType))
	}
	return makeMilvusError(status.GetReason(), code, status.GetRetriable(), WithDetail(status.GetDetail()), WithErrorType(eType))
}

// SegcoreError returns a merr according to the given segcore error code and
// message. Classification (sentinel identity, input-vs-system error type) is
// delegated to the shared segcore code table; see classifySegcoreError.
func SegcoreError(code int32, msg string) error {
	return classifySegcoreError(code, msg)
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

// errorTypeMarker overrides the broad classification (Input/System) of the
// error it wraps, no matter how deep the milvus sentinel sits in the chain.
// GetErrorType finds it via the ErrorClassifier interface, so the mark works on
// bare milvusError values, *Msg (errors.Wrapf) results, and further-wrapped
// errors alike. It keeps the underlying error reachable via Unwrap, so Code /
// IsRetryableErr / errors.Is are unaffected.
type errorTypeMarker struct {
	error
	etype ErrorType
}

func (m errorTypeMarker) Unwrap() error           { return m.error }
func (m errorTypeMarker) GetErrorType() ErrorType { return m.etype }

func WrapErrAsInputError(err error) error {
	if err == nil {
		return nil
	}
	return errorTypeMarker{error: err, etype: InputError}
}

func WrapErrAsSysError(err error) error {
	if err == nil {
		return nil
	}
	return errorTypeMarker{error: err, etype: SystemError}
}

func WrapErrAsInputErrorWhen(err error, targets ...milvusError) error {
	if err == nil {
		return nil
	}
	code := Code(err)
	for _, target := range targets {
		if target.errCode == code {
			return errorTypeMarker{error: err, etype: InputError}
		}
	}
	return err
}

func WrapErrCollectionReplicateMode(operation string) error {
	return wrapFields(ErrCollectionReplicateMode, value("operation", operation))
}

func GetErrorType(err error) ErrorType {
	// Find the outermost classifier in the chain: an explicit errorTypeMarker
	// (from WrapErrAsInputError/SysError) takes precedence over the underlying
	// milvusError's baked-in errType, so the mark works through any wrapping.
	var ec ErrorClassifier
	if errors.As(err, &ec) {
		return ec.GetErrorType()
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
		return Wrapf(ErrServiceNotReady, "state code: %s", stateCode.String())
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

// formatMsg renders a WrapErr* message. When no args are supplied the format is
// used verbatim (no Sprintf), so a '%' in dynamic content — e.g.
// WrapErrServiceInternalMsg(err.Error()) where the message contains "50%" — is
// not misinterpreted as a printf verb and rendered as "%!s(MISSING)" garbage.
// With args it formats as usual.
func formatMsg(format string, args ...any) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}

// wrapMsg attaches a (safely rendered) message to a sentinel for the
// WrapErr*Msg factories. When no args are supplied the format is used verbatim
// (errors.Wrap, no Sprintf), so a '%' in dynamic content — e.g.
// WrapErrServiceInternalMsg(err.Error()) where the message contains "50%" — is
// not misinterpreted as a printf verb. The args path uses errors.Wrapf rather
// than fmt.Sprintf so `go vet` does not classify the WrapErr*Msg helpers as
// printf wrappers and flag every non-constant-format callsite.
func wrapMsg(err error, format string, args ...any) error {
	if len(args) == 0 {
		return errors.Wrap(err, format)
	}
	return errors.Wrapf(err, format, args...)
}

func WrapErrServiceNotReadyMsg(fmt string, args ...any) error {
	return wrapMsg(ErrServiceNotReady, fmt, args...)
}

func WrapErrServiceUnavailable(reason string, msg ...string) error {
	err := wrapFieldsWithDesc(ErrServiceUnavailable, reason)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrServiceUnavailableMsg(fmt string, args ...any) error {
	return wrapMsg(ErrServiceUnavailable, fmt, args...)
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
		return WrapErrServiceInternalMsg(format, args...)
	}
	return wrapInner(ErrServiceInternal, formatMsg(format, args...), err)
}

func WrapErrServiceInternalMsg(fmt string, args ...any) error {
	return wrapMsg(ErrServiceInternal, fmt, args...)
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

func WrapErrServiceQuotaExceededMsg(fmt string, args ...any) error {
	return wrapMsg(ErrServiceQuotaExceeded, fmt, args...)
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

func WrapErrCollectionSchemaVersionNotReady(collection any, consistentSegments, totalSegments int) error {
	return wrapFieldsWithDesc(
		ErrCollectionSchemaVersionNotReady,
		fmt.Sprintf("%d/%d segments are consistent, required 100%%", consistentSegments, totalSegments),
		value("collection", collection),
	)
}

func WrapErrCollectionSchemaChangeInProgress(collection any, msgAndArgs ...any) error {
	err := wrapFields(ErrCollectionSchemaChangeInProgress, value("collection", collection))
	// Guard the first-arg-is-format-string assumption: a non-string first arg is ignored rather than
	// panicking (callers that pass no message, e.g. WrapErrCollectionSchemaChangeInProgress(collID), stay ok).
	if len(msgAndArgs) > 0 {
		if msg, ok := msgAndArgs[0].(string); ok {
			err = errors.Wrapf(err, msg, msgAndArgs[1:]...)
		}
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

// Deprecated: misspelled historical name kept for backward compatibility of the
// exported symbol; use WrapErrResourceGroupServiceUnAvailable.
func WrapErrResourceGroupServiceAvailable(msg ...string) error {
	return WrapErrResourceGroupServiceUnAvailable(msg...)
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

func WrapErrChannelTSafeStalled(name string, msg ...string) error {
	return warpChannelErr(ErrChannelTSafeStalled, name, msg...)
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

func WrapErrChannelDroppedSentinel(name string, msg ...string) error {
	return warpChannelErr(ErrChannelDroppedSentinel, name, msg...)
}

// WrapErrChannelMisrouted is used by a delegator/querynode when it receives a
// request for a channel it does not own. Encodes the requested channel name
// in the structured field; callers can put additional context (e.g. the list
// of channels the node actually owns) into msg.
func WrapErrChannelMisrouted(name string, msg ...string) error {
	return warpChannelErr(ErrChannelMisrouted, name, msg...)
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

// WrapErrSerializationFailedMsg creates a new ErrSerializationFailed (code 1004)
// with a detail message. Used when stored bytes cannot be decoded into the
// expected shape and there is no underlying error to wrap (e.g. valuesRead vs
// rows mismatch in payload reader, type mismatch when reading a column from
// the wrong DataType).
func WrapErrSerializationFailedMsg(format string, args ...any) error {
	return wrapMsg(ErrSerializationFailed, format, args...)
}

// WrapErrSerializationFailed wraps an existing underlying error 'err' with
// ErrSerializationFailed (code 1004). Used when a decode / unmarshal /
// schema-conversion step fails with a non-typed inner error (json, proto,
// arrow). If 'err' is already a typed merr, use merr.Wrap(err, msg) instead.
func WrapErrSerializationFailed(err error, format string, args ...any) error {
	if err == nil {
		return WrapErrSerializationFailedMsg(format, args...)
	}
	return wrapInner(ErrSerializationFailed, formatMsg(format, args...), err)
}

// WrapErrDataIntegrityMsg creates a new ErrDataIntegrity (code 1009) with a
// detail message. Use when on-disk bytes don't conform to the expected schema
// (binlog type mismatch, valuesRead vs rows mismatch, malformed event header,
// unparseable stats buffer) — i.e. the stored data itself is corrupt, not the
// (de)serialization step.
func WrapErrDataIntegrityMsg(format string, args ...any) error {
	return wrapMsg(ErrDataIntegrity, format, args...)
}

// WrapErrDataIntegrity wraps an existing underlying error with ErrDataIntegrity
// (code 1009). Use when stored-byte parsing surfaces a non-typed inner error
// (json unmarshal of stats buffer, type assertion of decoded header extras).
// If 'err' is already a typed merr, use merr.Wrap(err, msg) instead.
func WrapErrDataIntegrity(err error, format string, args ...any) error {
	if err == nil {
		return WrapErrDataIntegrityMsg(format, args...)
	}
	return wrapInner(ErrDataIntegrity, formatMsg(format, args...), err)
}

// WrapErrStorageMsg creates a new ErrStorage (code 1008) with a detail message.
// Used when a logical internal error occurs in the storage layer (e.g. invalid
// state, corrupted data structure check, nil data) and there is no underlying
// Go error to wrap.
func WrapErrStorageMsg(format string, args ...any) error {
	return wrapMsg(ErrStorage, format, args...)
}

// WrapErrStorage wraps an existing underlying error 'err' with ErrStorage
// (code 1008). Used for storage subsystem failures (transaction state machine,
// writer/reader lifecycle, FFI internal failures) that are not physical I/O,
// not serialization, and not client-input errors.
//
// IMPORTANT: only use when 'err' is a raw error (FFI / fs / proto / arrow).
// If 'err' is already a typed merr, use merr.Wrap(err, msg) instead — wrapping
// a typed merr here would mask its inner code (defect#3 pattern).
//
// merr.Code(result) returns ErrStorage.code() = 1008; errors.Is(result, ErrStorage)
// succeeds; errors.Is(result, err) also succeeds (inner chain preserved via Unwrap).
func WrapErrStorage(err error, format string, args ...any) error {
	if err == nil {
		return WrapErrStorageMsg(format, args...)
	}
	return wrapInner(ErrStorage, formatMsg(format, args...), err)
}

// WrapErrFunctionFailedMsg creates a new ErrFunctionFailed (code 2400) with a
// detail message. Use when a function / BM25 / MinHash / analyzer runner
// returns a malformed output (wrong type, empty, unexpected shape) and there
// is no underlying Go error to wrap.
func WrapErrFunctionFailedMsg(format string, args ...any) error {
	return wrapMsg(ErrFunctionFailed, format, args...)
}

// WrapErrFunctionFailed wraps an existing underlying error 'err' with
// ErrFunctionFailed (code 2400). Use when a function-pipeline call surfaces
// a non-typed inner error (runner I/O, model invocation, dependency failure).
//
// IMPORTANT: only use when 'err' is a raw error. If 'err' is already a typed
// merr, use merr.Wrap(err, msg) instead — wrapping a typed merr here would
// mask its inner code (defect#3 pattern).
//
// merr.Code(result) returns ErrFunctionFailed.code() = 2400; errors.Is(result,
// ErrFunctionFailed) succeeds; errors.Is(result, err) also succeeds (inner
// chain preserved via Unwrap).
func WrapErrFunctionFailed(err error, format string, args ...any) error {
	if err == nil {
		return WrapErrFunctionFailedMsg(format, args...)
	}
	return wrapInner(ErrFunctionFailed, formatMsg(format, args...), err)
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

func WrapErrIoFailedMsg(fmt string, args ...any) error {
	return wrapMsg(ErrIoFailed, fmt, args...)
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

func WrapErrIoPermissionDenied(key string, err error) error {
	if err == nil {
		return nil
	}
	return wrapFieldsWithDesc(ErrIoPermissionDenied, err.Error(), value("key", key))
}

func WrapErrIoBucketNotFound(key string, err error) error {
	if err == nil {
		return nil
	}
	return wrapFieldsWithDesc(ErrIoBucketNotFound, err.Error(), value("key", key))
}

func WrapErrIoInvalidCredentials(key string, err error) error {
	if err == nil {
		return nil
	}
	return wrapFieldsWithDesc(ErrIoInvalidCredentials, err.Error(), value("key", key))
}

func WrapErrIoInvalidArgument(key string, err error) error {
	if err == nil {
		return nil
	}
	return wrapFieldsWithDesc(ErrIoInvalidArgument, err.Error(), value("key", key))
}

func WrapErrIoInvalidRange(key string, err error) error {
	if err == nil {
		return nil
	}
	return wrapFieldsWithDesc(ErrIoInvalidRange, err.Error(), value("key", key))
}

func WrapErrIoEntityTooLarge(key string, err error) error {
	if err == nil {
		return nil
	}
	return wrapFieldsWithDesc(ErrIoEntityTooLarge, err.Error(), value("key", key))
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

// WrapErrParameterInvalidErr wraps an existing error 'err' with ErrParameterInvalid (Code 1100).
// This is used when an underlying error (e.g., from parsing, validation utility, or dependency)
// causes a parameter check to fail, and you need to provide extra context
// in 'format' and 'args'.
func WrapErrParameterInvalidErr(err error, format string, args ...any) error {
	if err == nil {
		return WrapErrParameterInvalidMsg(format, args...)
	}
	return wrapInner(ErrParameterInvalid, formatMsg(format, args...), err)
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
	return wrapMsg(ErrParameterInvalid, fmt, args...)
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
	return wrapMsg(ErrParameterMissing, fmt, args...)
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
	if err == nil {
		return ErrMqInternal
	}
	ctx := ErrMqInternal.msg
	if len(msg) > 0 {
		ctx = strings.Join(msg, "->") + ": " + ctx
	}
	return wrapInner(ErrMqInternal, ctx, err)
}

// WrapErrMqInternalMsg creates a new ErrMqInternal (code 1302) with a detail
// message. Use this when there is no underlying Go error to wrap.
func WrapErrMqInternalMsg(format string, args ...any) error {
	return wrapMsg(ErrMqInternal, format, args...)
}

func WrapErrPrivilegeNotAuthenticated(fmt string, args ...any) error {
	err := wrapMsg(ErrPrivilegeNotAuthenticated, fmt, args...)
	return err
}

func WrapErrPrivilegeNotPermitted(fmt string, args ...any) error {
	err := wrapMsg(ErrPrivilegeNotPermitted, fmt, args...)
	return err
}

// WrapErrSegcoreMsg creates a new ErrSegcore (2000) with a Sprintf-formatted
// message. Use for Go-side segcore invariants where there's no C++ errorCode
// available. When a C++ errorCode is available (the CGO boundary), use
// SegcoreError(code, msg) instead, which classifies the code via the shared
// segcore code table (see segcore.go).
func WrapErrSegcoreMsg(format string, args ...any) error {
	return wrapMsg(ErrSegcore, format, args...)
}

// Deprecated: segcore error classification is now driven by the shared code
// table; use WrapErrSegcoreMsg. Kept for backward compatibility of the exported
// symbol.
func WrapErrSegcore(code int32, msg ...string) error {
	err := wrapFields(ErrSegcore, value("segcoreCode", code))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// Deprecated: segcore error classification is now driven by the shared code
// table; use WrapErrSegcoreMsg. Kept for backward compatibility of the exported
// symbol.
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

// WrapErrImportFailedMsg is the formatted variant of WrapErrImportFailed,
// matching the standard merr Msg-factory convention (errors.Wrapf) so callers
// pass a format string + args instead of an inline fmt.Sprintf.
func WrapErrImportFailedMsg(fmt string, args ...any) error {
	return wrapMsg(ErrImportFailed, fmt, args...)
}

// WrapErrImportSysFailed wraps ErrImportSysFailed: the server-side / object-IO
// import failures (job orchestration, backpressure, reader open/read) that are
// the operator's concern, not the caller's. Use it instead of
// WrapErrImportFailed wherever the failure is not caused by malformed user data.
func WrapErrImportSysFailed(msg ...string) error {
	err := error(ErrImportSysFailed)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// WrapErrImportSysFailedMsg is the formatted variant of WrapErrImportSysFailed.
func WrapErrImportSysFailedMsg(fmt string, args ...any) error {
	return wrapMsg(ErrImportSysFailed, fmt, args...)
}

func WrapErrInconsistentRequery(msg ...string) error {
	err := error(ErrInconsistentRequery)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// WrapErrQueryPlanMsg creates a new ErrQueryPlan (code 2201) with a detail
// message. Use this when query plan parsing/validation fails and there is no
// underlying Go error to wrap.
func WrapErrQueryPlanMsg(format string, args ...any) error {
	return wrapMsg(ErrQueryPlan, format, args...)
}

// WrapErrQueryPlan wraps an existing underlying error with ErrQueryPlan
// (code 2201), preserving the inner error chain so callers can still match
// upstream sentinels via errors.Is/As.
func WrapErrQueryPlan(err error, format string, args ...any) error {
	if err == nil {
		return WrapErrQueryPlanMsg(format, args...)
	}
	return wrapInner(ErrQueryPlan, formatMsg(format, args...), err)
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
	return wrapMsg(ErrIllegalCompactionPlan, format, args...)
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
	if err == nil {
		return ErrBuildCompactionRequestFail
	}
	return wrapInner(ErrBuildCompactionRequestFail, ErrBuildCompactionRequestFail.msg, err)
}

func WrapErrGetCompactionPlanResultFail(err error) error {
	if err == nil {
		return ErrGetCompactionPlanResultFail
	}
	return wrapInner(ErrGetCompactionPlanResultFail, ErrGetCompactionPlanResultFail.msg, err)
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

// WrapErrCompactionBlocked indicates that a compaction task is blocked by snapshot protection.
// This is a business-level rejection (not a service fault) that the scheduler should handle
// with low-frequency backoff, not with P0 alerting.
func WrapErrCompactionBlocked(reason string, msg ...string) error {
	err := wrapFieldsWithDesc(ErrCompactionBlocked, reason)
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

func WrapErrSnapshotNotFound(name any, msg ...string) error {
	err := wrapFields(ErrSnapshotNotFound, value("snapshot", name))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

func WrapErrSnapshotPinned(name any, msg ...string) error {
	err := wrapFields(ErrSnapshotPinned, value("snapshot", name))
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "->"))
	}
	return err
}

// WrapErrOperationNotSupportedMsg creates a new ErrOperationNotSupported with a detail message (Code 3000).
// This is the primary replacement for fmt.Errorf/errors.New for operations that are
// currently not supported by the system.
func WrapErrOperationNotSupportedMsg(format string, args ...any) error {
	return wrapMsg(ErrOperationNotSupported, format, args...)
}

// WrapErrOperationNotSupported wraps an existing error 'err' with ErrOperationNotSupported (Code 3000).
func WrapErrOperationNotSupported(err error, format string, args ...any) error {
	if err == nil {
		return WrapErrOperationNotSupportedMsg(format, args...)
	}
	return wrapInner(ErrOperationNotSupported, formatMsg(format, args...), err)
}
