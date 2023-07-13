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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

var (
	// For compatibility
	oldErrCodes = map[int32]commonpb.ErrorCode{
		ErrServiceNotReady.code():    commonpb.ErrorCode_NotReadyServe,
		ErrCollectionNotFound.code(): commonpb.ErrorCode_CollectionNotExists,
	}
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

func IsRetriable(err error) bool {
	return Code(err)&retriableFlag != 0
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

func oldCode(code int32) commonpb.ErrorCode {
	switch code {
	case ErrServiceNotReady.code():
		return commonpb.ErrorCode_NotReadyServe
	case ErrCollectionNotFound.code():
		return commonpb.ErrorCode_CollectionNotExists
	case ErrNodeNotMatch.code():
		return commonpb.ErrorCode_NodeIDNotMatch
	default:
		return commonpb.ErrorCode_UnexpectedError
	}
}

func Ok(status *commonpb.Status) bool {
	return status.ErrorCode == commonpb.ErrorCode_Success && status.Code == 0
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
		return newMilvusError(fmt.Sprintf("legacy error code:%d, reason: %s", status.GetErrorCode(), status.GetReason()), errUnexpected.errCode, false)
	}

	return newMilvusError(status.GetReason(), code, code&retriableFlag != 0)
}

// CheckHealthy checks whether the state is healthy,
// returns nil if healthy,
// otherwise returns ErrServiceNotReady wrapped with current state
func CheckHealthy(state commonpb.StateCode) error {
	if state != commonpb.StateCode_Healthy {
		return WrapErrServiceNotReady(state.String())
	}

	return nil
}

// Service related
func WrapErrServiceNotReady(stage string, msg ...string) error {
	err := errors.Wrapf(ErrServiceNotReady, "stage=%s", stage)
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

func WrapErrCrossClusterRouting(expectedCluster, actualCluster string, msg ...string) error {
	err := errors.Wrapf(ErrCrossClusterRouting, "expectedCluster=%s, actualCluster=%s", expectedCluster, actualCluster)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrDatabaseNotFound(database any, msg ...string) error {
	err := wrapWithField(ErrDatabaseNotfound, "database", database)
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

func WrapErrInvalidedDatabaseName(database any, msg ...string) error {
	err := wrapWithField(ErrInvalidedDatabaseName, "database", database)
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

func WrapErrNoAvailableNodeInReplica(id int64, msg ...string) error {
	err := wrapWithField(ErrNoAvailableNodeInReplica, "replica", id)
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
func WrapErrIndexNotFound(msg ...string) error {
	err := error(ErrIndexNotFound)
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

func WrapErrNoAvailableNode(msg ...string) error {
	err := error(ErrNoAvailableNode)
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

func WrapErrIoFailed(key string, msg ...string) error {
	err := errors.Wrapf(ErrIoFailed, "key=%s", key)
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
	err := errors.Wrapf(ErrParameterInvalid, "expected in (%v, %v), actual=%v", lower, upper, actual)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrParameterDuplicateFieldData(fieldName string, msg ...string) error {
	err := errors.Wrapf(ErrParameterInvalid, "field name=%v", fieldName)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
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

// Topic related
func WrapErrTopicNotFound(name string, msg ...string) error {
	err := errors.Wrapf(ErrTopicNotFound, "topic=%s", name)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrTopicNotEmpty(name string, msg ...string) error {
	err := errors.Wrapf(ErrTopicNotEmpty, "topic=%s", name)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// Average related
func WrapErrAverageLabelNotRegister(label string, msg ...string) error {
	err := errors.Wrapf(ErrAverageLabelNotRegister, "averageLabel=%s", label)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// shard delegator related
func WrapErrShardDelegatorNotFound(channel string, msg ...string) error {
	err := errors.Wrapf(ErrShardDelegatorNotFound, "channel=%s", channel)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrShardDelegatorAccessFailed(channel string, msg ...string) error {
	err := errors.Wrapf(ErrShardDelegatorAccessFailed, "channel=%s", channel)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrShardDelegatorSearchFailed(msg ...string) error {
	err := error(ErrShardDelegatorSearchFailed)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrShardDelegatorQueryFailed(msg ...string) error {
	err := error(ErrShardDelegatorQueryFailed)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func WrapErrShardDelegatorStatisticFailed(msg ...string) error {
	err := error(ErrShardDelegatorStatisticFailed)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// task related
func WrapErrTaskQueueFull(msg ...string) error {
	err := error(ErrTaskQueueFull)
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

func wrapWithField(err error, name string, value any) error {
	return errors.Wrapf(err, "%s=%v", name, value)
}
