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
	"github.com/cockroachdb/errors"
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
	ErrCollectionNotFound                      = newMilvusError("collection not found", 100, false)
	ErrCollectionNotLoaded                     = newMilvusError("collection not loaded", 101, false)
	ErrCollectionNumLimitExceeded              = newMilvusError("exceeded the limit number of collections", 102, false)
	ErrCollectionNotFullyLoaded                = newMilvusError("collection not fully loaded", 103, true)
	ErrCollectionLoaded                        = newMilvusError("collection already loaded", 104, false)
	ErrCollectionIllegalSchema                 = newMilvusError("illegal collection schema", 105, false)
	ErrCollectionOnRecovering                  = newMilvusError("collection on recovering", 106, true)
	ErrCollectionVectorClusteringKeyNotAllowed = newMilvusError("vector clustering key not allowed", 107, false)
	ErrCollectionReplicateMode                 = newMilvusError("can't operate on the collection under standby mode", 108, false)
	ErrCollectionSchemaMismatch                = newMilvusError("collection schema mismatch", 109, false)
	// Partition related
	ErrPartitionNotFound       = newMilvusError("partition not found", 200, false)
	ErrPartitionNotLoaded      = newMilvusError("partition not loaded", 201, false)
	ErrPartitionNotFullyLoaded = newMilvusError("partition not fully loaded", 202, true)

	// General capacity related
	ErrGeneralCapacityExceeded = newMilvusError("general capacity exceeded", 250, false)

	// ResourceGroup related
	ErrResourceGroupNotFound      = newMilvusError("resource group not found", 300, false)
	ErrResourceGroupAlreadyExist  = newMilvusError("resource group already exist, but create with different config", 301, false)
	ErrResourceGroupReachLimit    = newMilvusError("resource group num reach limit", 302, false)
	ErrResourceGroupIllegalConfig = newMilvusError("resource group illegal config", 303, false)
	// go:deprecated
	ErrResourceGroupNodeNotEnough    = newMilvusError("resource group node not enough", 304, false)
	ErrResourceGroupServiceAvailable = newMilvusError("resource group service available", 305, true)

	// Replica related
	ErrReplicaNotFound     = newMilvusError("replica not found", 400, false)
	ErrReplicaNotAvailable = newMilvusError("replica not available", 401, false)

	// Channel & Delegator related
	ErrChannelNotFound         = newMilvusError("channel not found", 500, false)
	ErrChannelLack             = newMilvusError("channel lacks", 501, false)
	ErrChannelReduplicate      = newMilvusError("channel reduplicates", 502, false)
	ErrChannelNotAvailable     = newMilvusError("channel not available", 503, false)
	ErrChannelCPExceededMaxLag = newMilvusError("channel checkpoint exceed max lag", 504, false)

	// Segment related
	ErrSegmentNotFound    = newMilvusError("segment not found", 600, false)
	ErrSegmentNotLoaded   = newMilvusError("segment not loaded", 601, false)
	ErrSegmentLack        = newMilvusError("segment lacks", 602, false)
	ErrSegmentReduplicate = newMilvusError("segment reduplicates", 603, false)
	ErrSegmentLoadFailed  = newMilvusError("segment load failed", 604, false)

	// Index related
	ErrIndexNotFound     = newMilvusError("index not found", 700, false)
	ErrIndexNotSupported = newMilvusError("index type not supported", 701, false)
	ErrIndexDuplicate    = newMilvusError("index duplicates", 702, false)
	ErrTaskDuplicate     = newMilvusError("task duplicates", 703, false)

	// Database related
	ErrDatabaseNotFound         = newMilvusError("database not found", 800, false)
	ErrDatabaseNumLimitExceeded = newMilvusError("exceeded the limit number of database", 801, false)
	ErrDatabaseInvalidName      = newMilvusError("invalid database name", 802, false)

	// Node related
	ErrNodeNotFound        = newMilvusError("node not found", 901, false)
	ErrNodeOffline         = newMilvusError("node offline", 902, false)
	ErrNodeLack            = newMilvusError("node lacks", 903, false)
	ErrNodeNotMatch        = newMilvusError("node not match", 904, false)
	ErrNodeNotAvailable    = newMilvusError("node not available", 905, false)
	ErrNodeStateUnexpected = newMilvusError("node state unexpected", 906, false)

	// IO related
	ErrIoKeyNotFound = newMilvusError("key not found", 1000, false)
	ErrIoFailed      = newMilvusError("IO failed", 1001, false)
	ErrIoUnexpectEOF = newMilvusError("unexpected EOF", 1002, true)

	// Parameter related
	ErrParameterInvalid  = newMilvusError("invalid parameter", 1100, false)
	ErrParameterMissing  = newMilvusError("missing parameter", 1101, false)
	ErrParameterTooLarge = newMilvusError("parameter too large", 1102, false)

	// Metrics related
	ErrMetricNotFound = newMilvusError("metric not found", 1200, false)

	// Message queue related
	ErrMqTopicNotFound = newMilvusError("topic not found", 1300, false)
	ErrMqTopicNotEmpty = newMilvusError("topic not empty", 1301, false)
	ErrMqInternal      = newMilvusError("message queue internal error", 1302, false)
	ErrDenyProduceMsg  = newMilvusError("deny to write the message to mq", 1303, false)

	// Privilege related
	// this operation is denied because the user not authorized, user need to login in first
	ErrPrivilegeNotAuthenticated = newMilvusError("not authenticated", 1400, false)
	// this operation is denied because the user has no permission to do this, user need higher privilege
	ErrPrivilegeNotPermitted     = newMilvusError("privilege not permitted", 1401, false)
	ErrPrivilegeGroupInvalidName = newMilvusError("invalid privilege group name", 1402, false)

	// Alias related
	ErrAliasNotFound               = newMilvusError("alias not found", 1600, false)
	ErrAliasCollectionNameConfilct = newMilvusError("alias and collection name conflict", 1601, false)
	ErrAliasAlreadyExist           = newMilvusError("alias already exist", 1602, false)
	ErrCollectionIDOfAliasNotFound = newMilvusError("collection id of alias not found", 1603, false)

	// field related
	ErrFieldNotFound    = newMilvusError("field not found", 1700, false)
	ErrFieldInvalidName = newMilvusError("field name invalid", 1701, false)

	// high-level restful api related
	ErrNeedAuthenticate          = newMilvusError("user hasn't authenticated", 1800, false)
	ErrIncorrectParameterFormat  = newMilvusError("can only accept json format request", 1801, false)
	ErrMissingRequiredParameters = newMilvusError("missing required parameters", 1802, false)
	ErrMarshalCollectionSchema   = newMilvusError("fail to marshal collection schema", 1803, false)
	ErrInvalidInsertData         = newMilvusError("fail to deal the insert data", 1804, false)
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
	ErrImportFailed = newMilvusError("importing data failed", 2100, false)

	// Search/Query related
	ErrInconsistentRequery = newMilvusError("inconsistent requery result", 2200, true)

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
}

func newMilvusError(msg string, code int32, retriable bool, options ...errorOption) milvusError {
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
	return e.msg
}

func (e milvusError) Detail() string {
	return e.detail
}

func (e milvusError) Is(err error) bool {
	cause := errors.Cause(err)
	if cause, ok := cause.(milvusError); ok {
		return e.errCode == cause.errCode
	}
	return false
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
	return multiErrors{
		errs,
	}
}
