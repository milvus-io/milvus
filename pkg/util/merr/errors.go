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
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/markers"
	"github.com/samber/lo"
)

const (
	CanceledCode int32 = 10000
	TimeoutCode  int32 = 10001
)

type ErrorType int32

const (
	SystemError ErrorType = 0
	InputError  ErrorType = 1
)

var ErrorTypeName = map[ErrorType]string{
	SystemError: "system_error",
	InputError:  "input_error",
}

// ErrorClassifier defines the method to get the broad classification of a Milvus error.
type ErrorClassifier interface {
	GetErrorType() ErrorType
}

func (err ErrorType) String() string {
	return ErrorTypeName[err]
}

// Define leaf errors here,
// WARN: take care to add new error,
// check whether you can use the errors below before adding a new one.
// Name: Err + related prefix + error name
var (
	// Service related
	ErrServiceNotReady             = newMilvusError("service not ready", 1, true) // This indicates the service is still in init
	ErrServiceUnavailable          = newMilvusError("service unavailable", 2, true)
	ErrServiceMemoryLimitExceeded  = newMilvusError("memory limit exceeded", 3, false)
	ErrServiceTooManyRequests      = newMilvusError("too many concurrent requests, queue is full", 4, true)
	ErrServiceInternal             = newMilvusError("service internal error", 5, false) // Never return this error out of Milvus
	ErrServiceCrossClusterRouting  = newMilvusError("cross cluster routing", 6, false)
	ErrServiceDiskLimitExceeded    = newMilvusError("disk limit exceeded", 7, false)
	ErrServiceRateLimit            = newMilvusError("rate limit exceeded", 8, true)
	ErrServiceQuotaExceeded        = newMilvusError("quota exceeded", 9, false)
	ErrServiceUnimplemented        = newMilvusError("service unimplemented", 10, false)
	ErrServiceTimeTickLongDelay    = newMilvusError("time tick long delay", 11, false)
	ErrServiceResourceInsufficient = newMilvusError("service resource insufficient", 12, true)

	// Collection related
	ErrCollectionNotFound                      = newMilvusError("collection not found", 100, false) // SystemError by default: internal retry.Do paths (datacoord handler/recovery) must keep retrying through a transient not-found; the proxy boundary stamps it InputError for users via WrapErrAsInputErrorWhen.
	ErrCollectionNotLoaded                     = newMilvusError("collection not loaded", 101, false)
	ErrCollectionNumLimitExceeded              = newMilvusError("exceeded the limit number of collections", 102, false, WithErrorType(InputError))
	ErrCollectionNotFullyLoaded                = newMilvusError("collection not fully loaded", 103, true)
	ErrCollectionLoaded                        = newMilvusError("collection already loaded", 104, false, WithErrorType(InputError))
	ErrCollectionIllegalSchema                 = newMilvusError("illegal collection schema", 105, false, WithErrorType(InputError))
	ErrCollectionOnRecovering                  = newMilvusError("collection on recovering", 106, true)
	ErrCollectionVectorClusteringKeyNotAllowed = newMilvusError("vector clustering key not allowed", 107, false, WithErrorType(InputError))
	// Deprecated, keep it only for reserving the error code
	ErrCollectionReplicateMode         = newMilvusError("can't operate on the collection under standby mode", 108, false, WithErrorType(InputError))
	ErrCollectionSchemaMismatch        = newMilvusError("collection schema mismatch", 109, false, WithErrorType(InputError))
	ErrCollectionSchemaVersionNotReady = newMilvusError("collection schema version not ready", 110, true)

	// Partition related
	ErrPartitionNotFound       = newMilvusError("partition not found", 200, false) // SystemError by default; the proxy GetPartitionInfo name chokepoint stamps InputError for user-supplied partition names, while id-based lookups stay system.
	ErrPartitionNotLoaded      = newMilvusError("partition not loaded", 201, false)
	ErrPartitionNotFullyLoaded = newMilvusError("partition not fully loaded", 202, true)

	// General capacity related
	ErrGeneralCapacityExceeded = newMilvusError("general capacity exceeded", 250, false)

	// ResourceGroup related
	ErrResourceGroupNotFound      = newMilvusError("resource group not found", 300, false, WithErrorType(InputError))
	ErrResourceGroupAlreadyExist  = newMilvusError("resource group already exist, but create with different config", 301, false, WithErrorType(InputError))
	ErrResourceGroupReachLimit    = newMilvusError("resource group num reach limit", 302, false, WithErrorType(InputError))
	ErrResourceGroupIllegalConfig = newMilvusError("resource group illegal config", 303, false, WithErrorType(InputError))
	// go:deprecated
	ErrResourceGroupNodeNotEnough      = newMilvusError("resource group node not enough", 304, false)
	ErrResourceGroupServiceUnAvailable = newMilvusError("resource group service unavailable", 305, true)
	// Deprecated: misspelled historical name kept as an alias for backward
	// compatibility of the exported symbol; use ErrResourceGroupServiceUnAvailable.
	ErrResourceGroupServiceAvailable = ErrResourceGroupServiceUnAvailable

	// Replica related
	ErrReplicaNotFound     = newMilvusError("replica not found", 400, false)
	ErrReplicaNotAvailable = newMilvusError("replica not available", 401, false)

	// Channel & Delegator related
	ErrChannelNotFound         = newMilvusError("channel not found", 500, false)
	ErrChannelLack             = newMilvusError("channel lacks", 501, false)
	ErrChannelReduplicate      = newMilvusError("channel reduplicates", 502, false)
	ErrChannelNotAvailable     = newMilvusError("channel not available", 503, false)
	ErrChannelCPExceededMaxLag = newMilvusError("channel checkpoint exceed max lag", 504, false)
	ErrChannelTSafeStalled     = newMilvusError("channel tsafe stalled", 505, true)
	ErrChannelDroppedSentinel  = newMilvusError("channel checkpoint is dropped sentinel", 506, false)
	// Channel routed to the wrong node — the requested channel is not owned by
	// the delegator/querynode that received the request. Typically caused by
	// stale shard-map at the caller (proxy). Retryable so the caller can
	// re-resolve the shard map and dispatch to the correct node.
	ErrChannelMisrouted = newMilvusError("channel misrouted: not owned by this node", 507, true)

	// Segment related
	ErrSegmentNotFound    = newMilvusError("segment not found", 600, false)
	ErrSegmentNotLoaded   = newMilvusError("segment not loaded", 601, false)
	ErrSegmentLack        = newMilvusError("segment lacks", 602, false)
	ErrSegmentReduplicate = newMilvusError("segment reduplicates", 603, false)
	ErrSegmentLoadFailed  = newMilvusError("segment load failed", 604, false)
	// ErrSegmentRequestResourceFailed indicates the query node cannot load the segment
	// due to resource exhaustion (Memory, Disk, or GPU). When this error is returned,
	// the query coordinator will mark the node as resource exhausted and apply a
	// penalty period during which the node won't receive new loading tasks.
	ErrSegmentRequestResourceFailed = newMilvusError("segment request resource failed", 605, false)

	// Index related
	ErrIndexNotFound     = newMilvusError("index not found", 700, false)
	ErrIndexNotSupported = newMilvusError("index type not supported", 701, false)
	ErrIndexDuplicate    = newMilvusError("index duplicates", 702, false, WithErrorType(InputError))
	ErrTaskDuplicate     = newMilvusError("task duplicates", 703, false)

	// Database related
	ErrDatabaseNotFound         = newMilvusError("database not found", 800, false) // SystemError by default (same reason as ErrCollectionNotFound); proxy boundary marks it InputError for users via WrapErrAsInputErrorWhen.
	ErrDatabaseNumLimitExceeded = newMilvusError("exceeded the limit number of database", 801, false, WithErrorType(InputError))
	ErrDatabaseInvalidName      = newMilvusError("invalid database name", 802, false, WithErrorType(InputError))

	// Node related
	ErrNodeNotFound        = newMilvusError("node not found", 901, false)
	ErrNodeOffline         = newMilvusError("node offline", 902, false)
	ErrNodeLack            = newMilvusError("node lacks", 903, false)
	ErrNodeNotMatch        = newMilvusError("node not match", 904, false)
	ErrNodeNotAvailable    = newMilvusError("node not available", 905, false)
	ErrNodeStateUnexpected = newMilvusError("node state unexpected", 906, false)

	// IO related
	ErrIoKeyNotFound     = newMilvusError("key not found", 1000, false)
	ErrIoFailed          = newMilvusError("IO failed", 1001, false)
	ErrIoUnexpectEOF     = newMilvusError("unexpected EOF", 1002, true)
	ErrIoTooManyRequests = newMilvusError("too many requests", 1003, true)

	// Serialization / deserialization step failed — a transformation pipeline
	// (proto Marshal/Unmarshal, json encode/decode, arrow schema conversion etc.)
	// could not complete. Use for "conversion process failed" semantics rather
	// than "stored bytes are corrupt" — for the latter use ErrDataIntegrity.
	ErrSerializationFailed = newMilvusError("data serialization or deserialization failed", 1004, false)

	// Data integrity check failed — bytes already on disk (binlog payload,
	// event header extras, stats buffer) don't match the layout we expect from
	// the schema (type mismatch, valuesRead vs rows mismatch, unknown column
	// type, malformed event header). Likely permanent corruption; retry won't
	// help. Distinct from ErrSerializationFailed (process step failed) and
	// ErrParameterInvalid (caller-input we control).
	ErrDataIntegrity = newMilvusError("data integrity check failed", 1009, false)

	// Storage subsystem fallback — used for non-IO / non-serde / non-client-input
	// failures inside the storage package (transaction state machine, writer/reader
	// lifecycle, FFI internal failures, ext-table metadata operations).
	// Prefer ErrIo*/ErrSerializationFailed/ErrDataIntegrity/ErrParameterInvalid
	// when they fit; reach for ErrStorage only when none of those describe the failure.
	ErrStorage = newMilvusError("storage internal error", 1008, false, WithErrorType(SystemError))

	// Permanent errors - resource doesn't exist or access denied
	ErrIoPermissionDenied   = newMilvusError("permission denied", 1005, false)
	ErrIoBucketNotFound     = newMilvusError("bucket not found", 1006, false)
	ErrIoInvalidCredentials = newMilvusError("invalid credentials", 1007, false)

	// Client validation errors - request is malformed
	ErrIoInvalidArgument = newMilvusError("invalid argument", 1010, false)
	ErrIoInvalidRange    = newMilvusError("invalid range", 1011, false)
	ErrIoEntityTooLarge  = newMilvusError("entity too large", 1012, false)

	// Parameter related
	ErrParameterInvalid  = newMilvusError("invalid parameter", 1100, false, WithErrorType(InputError))
	ErrParameterMissing  = newMilvusError("missing parameter", 1101, false, WithErrorType(InputError))
	ErrParameterTooLarge = newMilvusError("parameter too large", 1102, false, WithErrorType(InputError))

	// Metrics related
	ErrMetricNotFound = newMilvusError("metric not found", 1200, false)

	// Message queue related
	ErrMqTopicNotFound = newMilvusError("topic not found", 1300, false)
	ErrMqTopicNotEmpty = newMilvusError("topic not empty", 1301, false)
	ErrMqInternal      = newMilvusError("message queue internal error", 1302, false)
	ErrDenyProduceMsg  = newMilvusError("deny to write the message to mq", 1303, false)

	// Privilege related
	// this operation is denied because the user not authorized, user need to login in first
	ErrPrivilegeNotAuthenticated = newMilvusError("not authenticated", 1400, false, WithErrorType(InputError))
	// this operation is denied because the user has no permission to do this, user need higher privilege
	ErrPrivilegeNotPermitted     = newMilvusError("privilege not permitted", 1401, false, WithErrorType(InputError))
	ErrPrivilegeGroupInvalidName = newMilvusError("invalid privilege group name", 1402, false, WithErrorType(InputError))

	// Alias related
	ErrAliasNotFound               = newMilvusError("alias not found", 1600, false) // SystemError by default; the proxy alias tasks (Describe/Drop/Alter) stamp InputError for user-supplied alias names.
	ErrAliasCollectionNameConfilct = newMilvusError("alias and collection name conflict", 1601, false)
	ErrAliasAlreadyExist           = newMilvusError("alias already exist", 1602, false)
	ErrCollectionIDOfAliasNotFound = newMilvusError("collection id of alias not found", 1603, false) // Genuinely SystemError: an existing alias whose target collection id cannot be resolved is an internal mapping inconsistency, not user input — left unmarked.

	// field related
	ErrFieldNotFound    = newMilvusError("field not found", 1700, false) // SystemError by default; proxy boundaries stamp InputError where the field name is user-supplied (e.g. group_by/anns field), while result-assembly lookups stay system.
	ErrFieldInvalidName = newMilvusError("field name invalid", 1701, false)

	// high-level restful api related
	ErrNeedAuthenticate          = newMilvusError("user hasn't authenticated", 1800, false, WithErrorType(InputError))
	ErrIncorrectParameterFormat  = newMilvusError("can only accept json format request", 1801, false, WithErrorType(InputError))
	ErrMissingRequiredParameters = newMilvusError("missing required parameters", 1802, false, WithErrorType(InputError))
	ErrMarshalCollectionSchema   = newMilvusError("fail to marshal collection schema", 1803, false)
	ErrInvalidInsertData         = newMilvusError("fail to deal the insert data", 1804, false, WithErrorType(InputError))
	ErrInvalidSearchResult       = newMilvusError("fail to parse search result", 1805, false)
	ErrCheckPrimaryKey           = newMilvusError("please check the primary key and its' type can only in [int, string]", 1806, false)
	ErrHTTPRateLimit             = newMilvusError("request is rejected by limiter", 1807, true)

	// replicate related
	ErrDenyReplicateMessage = newMilvusError("deny to use the replicate message in the normal instance", 1900, false)
	ErrInvalidMsgBytes      = newMilvusError("invalid replicate msg bytes", 1901, false)
	ErrNoAssignSegmentID    = newMilvusError("no assign segment id", 1902, false)
	ErrInvalidStreamObj     = newMilvusError("invalid stream object", 1903, false)

	// Segcore related
	ErrSegcore                    = newMilvusError("segcore error", 2000, false)
	ErrSegcoreUnsupported         = newMilvusError("segcore unsupported error", 2001, false)
	ErrSegcorePretendFinished     = newMilvusError("segcore pretend finished", 2002, false)
	ErrSegcoreFollyOtherException = newMilvusError("segcore folly other exception", 2037, false) // throw from segcore.
	ErrSegcoreFollyCancel         = newMilvusError("segcore Future was canceled", 2038, false)   // throw from segcore.
	ErrSegcoreOutOfRange          = newMilvusError("segcore out of range", 2039, false)          // throw from segcore.
	ErrSegcoreGCPNativeError      = newMilvusError("segcore GCP native error", 2040, false)      // throw from segcore.
	KnowhereError                 = newMilvusError("knowhere error", 2099, false)                // throw from segcore.

	// Do NOT export this,
	// never allow programmer using this, keep only for converting unknown error to milvusError
	errUnexpected = newMilvusError("unexpected error", (1<<16)-1, false)

	// import
	// ErrImportFailed is InputError by default: the import data layer
	// (internal/util/importutilv2) parses the caller's files and the vast
	// majority of its failures are malformed user data (bad JSON/CSV/Parquet,
	// type/dim/schema mismatch). Server-side import orchestration and object-IO
	// failures use ErrImportSysFailed instead.
	ErrImportFailed = newMilvusError("importing data failed", 2100, false, WithErrorType(InputError))
	// ErrImportSysFailed is the server-side counterpart of ErrImportFailed:
	// import job orchestration (job not found / no vchannels / restore /
	// job-count backpressure) and object-storage IO failures in the readers.
	// These are the operator's concern, not the caller's, so they stay
	// SystemError and must not be bucketed as a user-caused failure.
	ErrImportSysFailed = newMilvusError("importing data failed on server side", 2101, false)

	// Search/Query related
	ErrInconsistentRequery = newMilvusError("inconsistent requery result", 2200, true)
	ErrQueryPlan           = newMilvusError("query plan failed", 2201, false, WithErrorType(InputError))

	// Compaction
	ErrCompactionReadDeltaLogErr                  = newMilvusError("fail to read delta log", 2300, false)
	ErrIllegalCompactionPlan                      = newMilvusError("compaction plan illegal", 2301, false)
	ErrCompactionPlanConflict                     = newMilvusError("compaction plan conflict", 2302, false)
	ErrClusteringCompactionClusterNotSupport      = newMilvusError("milvus cluster not support clustering compaction", 2303, false)
	ErrClusteringCompactionCollectionNotSupport   = newMilvusError("collection not support clustering compaction", 2304, false)
	ErrClusteringCompactionCollectionIsCompacting = newMilvusError("collection is compacting", 2305, false)
	ErrClusteringCompactionNotSupportVector       = newMilvusError("vector field clustering compaction is not supported", 2306, false)
	ErrClusteringCompactionSubmitTaskFail         = newMilvusError("fail to submit task", 2307, true)
	ErrClusteringCompactionMetaError              = newMilvusError("fail to update meta in clustering compaction", 2308, true)
	ErrClusteringCompactionGetCollectionFail      = newMilvusError("fail to get collection in compaction", 2309, true)
	ErrCompactionResultNotFound                   = newMilvusError("compaction result not found", 2310, false)
	ErrAnalyzeTaskNotFound                        = newMilvusError("analyze task not found", 2311, true)
	ErrBuildCompactionRequestFail                 = newMilvusError("fail to build CompactionRequest", 2312, true)
	ErrGetCompactionPlanResultFail                = newMilvusError("fail to get compaction plan", 2313, true)
	ErrCompactionResult                           = newMilvusError("illegal compaction results", 2314, false)
	ErrDuplicatedCompactionTask                   = newMilvusError("duplicated compaction task", 2315, false)
	ErrCleanPartitionStatsFail                    = newMilvusError("fail to clean partition Stats", 2316, true)

	ErrDataNodeSlotExhausted = newMilvusError("datanode slot exhausted", 2401, false)

	// Function / runner execution failed — BM25 / MinHash / embedding /
	// analyzer runner returned a malformed output (wrong type, empty,
	// unexpected shape). Distinct from generic ServiceInternal: callers can
	// pattern-match this code when they specifically care about function
	// pipeline failures vs other internal errors.
	ErrFunctionFailed = newMilvusError("function execution failed", 2400, false)

	// Cipher/Encryption related
	ErrKMSKeyRevoked = newMilvusError("KMS key has been revoked, access denied", 2500, false)

	// General
	ErrOperationNotSupported = newMilvusError("unsupported operation", 3000, false)

	ErrOldSessionExists = newMilvusError("old session exists", 3001, false)
)

type errorOption func(*milvusError)

func WithDetail(detail string) errorOption {
	return func(err *milvusError) {
		err.detail = detail
	}
}

func WithErrorType(etype ErrorType) errorOption {
	return func(err *milvusError) {
		err.errType = etype
	}
}

type milvusError struct {
	msg       string
	detail    string
	retriable bool
	errCode   int32
	errType   ErrorType
	// inner is an optional underlying error. When set, this milvusError relabels
	// the chain with its own errCode/errType/retriable (so Code/IsRetryableErr
	// report this sentinel, not the inner) while keeping the inner reachable via
	// Unwrap so errors.Is(result, inner) still holds. Replaces wrappedMilvusError.
	inner error
}

// registeredCodes maps every sentinel code to the sentinel that owns it.
// milvusError.Is matches by code alone, so two sentinels sharing a code would
// silently satisfy errors.Is against each other; the registry also lets a
// wire code be mapped back to its baked classification (ErrorTypeOfCode).
var registeredCodes = make(map[int32]milvusError)

// newMilvusError defines a package-level sentinel and registers its code,
// panicking at package init on a duplicate. Use makeMilvusError for
// non-sentinel values (e.g. reconstructing an error from a wire Status).
func newMilvusError(msg string, code int32, retriable bool, options ...errorOption) milvusError {
	if owner, dup := registeredCodes[code]; dup {
		panic(fmt.Sprintf("merr: duplicate sentinel error code %d: %q vs %q", code, owner.msg, msg))
	}
	err := makeMilvusError(msg, code, retriable, options...)
	registeredCodes[code] = err
	return err
}

// ErrorTypeOfCode returns the baked classification of the sentinel that owns
// the given wire code, or SystemError for unregistered codes. Note this is
// the sentinel default only — boundary InputError marks stamped at runtime
// (WrapErrAsInputError) are not recoverable from the code.
func ErrorTypeOfCode(code int32) ErrorType {
	if s, ok := registeredCodes[code]; ok {
		return s.errType
	}
	return SystemError
}

// makeMilvusError constructs a milvusError value without claiming its code in
// the sentinel registry.
func makeMilvusError(msg string, code int32, retriable bool, options ...errorOption) milvusError {
	err := milvusError{
		msg:       msg,
		detail:    msg,
		retriable: retriable,
		errCode:   code,
	}

	for _, option := range options {
		option(&err)
	}
	return err
}

func (e milvusError) code() int32 {
	return e.errCode
}

func (e milvusError) Error() string {
	if e.inner != nil {
		return e.msg + ": " + e.inner.Error()
	}
	return e.msg
}

func (e milvusError) Detail() string {
	return e.detail
}

// Unwrap exposes the optional inner error so errors.Is(result, inner) holds.
// Returns nil for plain sentinels / wrapFields values, leaving their behavior
// unchanged.
func (e milvusError) Unwrap() error {
	return e.inner
}

func (e milvusError) Is(err error) bool {
	cause := errors.Cause(err)
	if cause, ok := cause.(milvusError); ok {
		return e.errCode == cause.errCode
	}
	return false
}

// GetErrorType implements the ErrorClassifier interface.
// It returns the predefined classification of the error (SystemError or InputError).
func (e milvusError) GetErrorType() ErrorType {
	return e.errType
}

// wrapInner builds a relabeling milvusError from a sentinel: it copies the
// sentinel's code/errType/retriable, attaches a contextual message, and keeps
// the inner error reachable via Unwrap. This is the unified replacement for
// wrappedMilvusError used by the WrapErrXxxErr factories.
func wrapInner(sentinel milvusError, msg string, inner error) milvusError {
	sentinel.msg = msg
	sentinel.detail = msg
	sentinel.inner = inner
	return sentinel
}

type multiErrors struct {
	errs []error
}

func (e multiErrors) Unwrap() error {
	if len(e.errs) <= 1 {
		return nil
	}
	// To make merr work for multi errors,
	// we need cause of multi errors, which defined as the last error
	if len(e.errs) == 2 {
		return e.errs[1]
	}

	return multiErrors{
		errs: e.errs[1:],
	}
}

func (e multiErrors) Error() string {
	final := e.errs[0]
	for i := 1; i < len(e.errs); i++ {
		final = errors.Wrap(e.errs[i], final.Error())
	}
	return final.Error()
}

func (e multiErrors) Is(err error) bool {
	for _, item := range e.errs {
		if errors.Is(item, err) {
			return true
		}
	}
	return false
}

func Combine(errs ...error) error {
	errs = lo.Filter(errs, func(err error, _ int) bool { return err != nil })
	if len(errs) == 0 {
		return nil
	}
	// A single error must keep its own identity/code: wrapping it in
	// multiErrors would make Unwrap return nil and degrade Code() to 65535.
	if len(errs) == 1 {
		return errs[0]
	}
	return multiErrors{
		errs,
	}
}

func Wrap(err error, msg string) error {
	return errors.Wrap(err, msg)
}

func Wrapf(err error, format string, args ...interface{}) error {
	return errors.Wrapf(err, format, args...)
}

func Mark(err error, reference error) error {
	return markers.Mark(err, reference)
}
