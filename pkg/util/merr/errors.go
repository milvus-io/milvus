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
	retryableFlag       = 1 << 16
	CanceledCode  int32 = 10000
	TimeoutCode   int32 = 10001
)

// Define leaf errors here,
// WARN: take care to add new error,
// check whehter you can use the erorrs below before adding a new one.
// Name: Err + related prefix + error name
var (
	// Service related
	ErrServiceNotReady             = newMilvusError("service not ready", 1, true) // This indicates the service is still in init
	ErrServiceUnavailable          = newMilvusError("service unavailable", 2, true)
	ErrServiceMemoryLimitExceeded  = newMilvusError("memory limit exceeded", 3, false)
	ErrServiceRequestLimitExceeded = newMilvusError("request limit exceeded", 4, true)
	ErrServiceInternal             = newMilvusError("service internal error", 5, false) // Never return this error out of Milvus
	ErrServiceCrossClusterRouting  = newMilvusError("cross cluster routing", 6, false)
	ErrServiceDiskLimitExceeded    = newMilvusError("disk limit exceeded", 7, false)
	ErrServiceRateLimit            = newMilvusError("rate limit exceeded", 8, true)
	ErrServiceForceDeny            = newMilvusError("force deny", 9, false)
	ErrServiceUnimplemented        = newMilvusError("service unimplemented", 10, false)

	// Collection related
	ErrCollectionNotFound         = newMilvusError("collection not found", 100, false)
	ErrCollectionNotLoaded        = newMilvusError("collection not loaded", 101, false)
	ErrCollectionNumLimitExceeded = newMilvusError("exceeded the limit number of collections", 102, false)
	ErrCollectionNotFullyLoaded   = newMilvusError("collection not fully loaded", 103, true)

	// Partition related
	ErrPartitionNotFound       = newMilvusError("partition not found", 200, false)
	ErrPartitionNotLoaded      = newMilvusError("partition not loaded", 201, false)
	ErrPartitionNotFullyLoaded = newMilvusError("partition not fully loaded", 202, true)

	// ResourceGroup related
	ErrResourceGroupNotFound = newMilvusError("resource group not found", 300, false)

	// Replica related
	ErrReplicaNotFound     = newMilvusError("replica not found", 400, false)
	ErrReplicaNotAvailable = newMilvusError("replica not available", 401, false)

	// Channel & Delegator related
	ErrChannelNotFound     = newMilvusError("channel not found", 500, false)
	ErrChannelLack         = newMilvusError("channel lacks", 501, false)
	ErrChannelReduplicate  = newMilvusError("channel reduplicates", 502, false)
	ErrChannelNotAvailable = newMilvusError("channel not available", 503, false)

	// Segment related
	ErrSegmentNotFound    = newMilvusError("segment not found", 600, false)
	ErrSegmentNotLoaded   = newMilvusError("segment not loaded", 601, false)
	ErrSegmentLack        = newMilvusError("segment lacks", 602, false)
	ErrSegmentReduplicate = newMilvusError("segment reduplicates", 603, false)

	// Index related
	ErrIndexNotFound     = newMilvusError("index not found", 700, false)
	ErrIndexNotSupported = newMilvusError("index type not supported", 701, false)
	ErrIndexDuplicate    = newMilvusError("index duplicates", 702, false)

	// Database related
	ErrDatabaseNotFound         = newMilvusError("database not found", 800, false)
	ErrDatabaseNumLimitExceeded = newMilvusError("exceeded the limit number of database", 801, false)
	ErrDatabaseInvalidName      = newMilvusError("invalid database name", 802, false)

	// Node related
	ErrNodeNotFound     = newMilvusError("node not found", 901, false)
	ErrNodeOffline      = newMilvusError("node offline", 902, false)
	ErrNodeLack         = newMilvusError("node lacks", 903, false)
	ErrNodeNotMatch     = newMilvusError("node not match", 904, false)
	ErrNodeNotAvailable = newMilvusError("node not available", 905, false)

	// IO related
	ErrIoKeyNotFound = newMilvusError("key not found", 1000, false)
	ErrIoFailed      = newMilvusError("IO failed", 1001, false)

	// Parameter related
	ErrParameterInvalid = newMilvusError("invalid parameter", 1100, false)

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
	ErrPrivilegeNotPermitted = newMilvusError("privilege not permitted", 1401, false)

	// Alias related
	ErrAliasNotFound               = newMilvusError("alias not found", 1600, false)
	ErrAliasCollectionNameConfilct = newMilvusError("alias and collection name conflict", 1601, false)
	ErrAliasAlreadyExist           = newMilvusError("alias already exist", 1602, false)

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

	// replicate related
	ErrDenyReplicateMessage = newMilvusError("deny to use the replicate message in the normal instance", 1900, false)
	ErrInvalidMsgBytes      = newMilvusError("invalid replicate msg bytes", 1901, false)
	ErrNoAssignSegmentID    = newMilvusError("no assign segment id", 1902, false)
	ErrInvalidStreamObj     = newMilvusError("invalid stream object", 1903, false)

	// Segcore related
	ErrSegcore = newMilvusError("segcore error", 2000, false)

	// Do NOT export this,
	// never allow programmer using this, keep only for converting unknown error to milvusError
	errUnexpected = newMilvusError("unexpected error", (1<<16)-1, false)
)

type milvusError struct {
	msg     string
	errCode int32
}

func newMilvusError(msg string, code int32, retriable bool) milvusError {
	if retriable {
		code |= retryableFlag
	}
	return milvusError{
		msg:     msg,
		errCode: code,
	}
}

func (e milvusError) code() int32 {
	return e.errCode
}

func (e milvusError) Error() string {
	return e.msg
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
