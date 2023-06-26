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

package datacoord

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
)

// Response response interface for verification
type Response interface {
	GetStatus() *commonpb.Status
}

// VerifyResponse verify grpc Response 1. check error is nil 2. check response.GetStatus() with status success
func VerifyResponse(response interface{}, err error) error {
	if err != nil {
		return err
	}
	if response == nil {
		return errNilResponse
	}
	switch resp := response.(type) {
	case Response:
		// note that resp will not be nil here, since it's still an interface
		if resp.GetStatus() == nil {
			return errNilStatusResponse
		}
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return errors.New(resp.GetStatus().GetReason())
		}
	case *commonpb.Status:
		if resp == nil {
			return errNilResponse
		}
		if resp.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.GetReason())
		}
	default:
		return errUnknownResponseType
	}
	return nil
}

// failResponse sets status to failed with unexpected error and reason.
func failResponse(status *commonpb.Status, reason string) {
	status.ErrorCode = commonpb.ErrorCode_UnexpectedError
	status.Reason = reason
}

// failResponseWithCode sets status to failed with error code and reason.
func failResponseWithCode(status *commonpb.Status, errCode commonpb.ErrorCode, reason string) {
	status.ErrorCode = errCode
	status.Reason = reason
}

func GetCompactTime(ctx context.Context, allocator allocator) (*compactTime, error) {
	ts, err := allocator.allocTimestamp(ctx)
	if err != nil {
		return nil, err
	}

	pts, _ := tsoutil.ParseTS(ts)
	ttRetention := pts.Add(-1 * Params.CommonCfg.RetentionDuration.GetAsDuration(time.Second))
	ttRetentionLogic := tsoutil.ComposeTS(ttRetention.UnixNano()/int64(time.Millisecond), 0)

	// TODO, change to collection level
	if Params.CommonCfg.EntityExpirationTTL.GetAsInt() > 0 {
		ttexpired := pts.Add(-1 * Params.CommonCfg.EntityExpirationTTL.GetAsDuration(time.Second))
		ttexpiredLogic := tsoutil.ComposeTS(ttexpired.UnixNano()/int64(time.Millisecond), 0)
		return &compactTime{ttRetentionLogic, ttexpiredLogic, Params.CommonCfg.EntityExpirationTTL.GetAsDuration(time.Second)}, nil
	}
	// no expiration time
	return &compactTime{ttRetentionLogic, 0, 0}, nil
}

func FilterInIndexedSegments(handler Handler, mt *meta, segments ...*SegmentInfo) []*SegmentInfo {
	if len(segments) == 0 {
		return nil
	}

	segmentMap := make(map[int64]*SegmentInfo)
	collectionSegments := make(map[int64][]int64)
	// TODO(yah01): This can't handle the case of multiple vector fields exist,
	// modify it if we support multiple vector fields.
	vecFieldID := make(map[int64]int64)
	for _, segment := range segments {
		collectionID := segment.GetCollectionID()
		segmentMap[segment.GetID()] = segment
		collectionSegments[collectionID] = append(collectionSegments[collectionID], segment.GetID())
	}
	for collection := range collectionSegments {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		coll, err := handler.GetCollection(ctx, collection)
		cancel()
		if err != nil {
			log.Warn("failed to get collection schema", zap.Error(err))
			continue
		}
		for _, field := range coll.Schema.GetFields() {
			if field.GetDataType() == schemapb.DataType_BinaryVector ||
				field.GetDataType() == schemapb.DataType_FloatVector {
				vecFieldID[collection] = field.GetFieldID()
				break
			}
		}
	}

	indexedSegments := make([]*SegmentInfo, 0)
	for _, segment := range segments {
		segmentState := mt.GetSegmentIndexStateOnField(segment.GetCollectionID(), segment.GetID(), vecFieldID[segment.GetCollectionID()])
		if segmentState.state == commonpb.IndexState_Finished {
			indexedSegments = append(indexedSegments, segment)
		}
	}

	return indexedSegments
}

func getZeroTime() time.Time {
	var t time.Time
	return t
}

// getCollectionTTL returns ttl if collection's ttl is specified, or return global ttl
func getCollectionTTL(properties map[string]string) (time.Duration, error) {
	v, ok := properties[common.CollectionTTLConfigKey]
	if ok {
		ttl, err := strconv.Atoi(v)
		if err != nil {
			return -1, err
		}
		return time.Duration(ttl) * time.Second, nil
	}

	return Params.CommonCfg.EntityExpirationTTL.GetAsDuration(time.Second), nil
}

func getCompactedSegmentSize(s *datapb.CompactionResult) int64 {
	var segmentSize int64

	if s != nil {
		for _, binlogs := range s.GetInsertLogs() {
			for _, l := range binlogs.GetBinlogs() {
				segmentSize += l.GetLogSize()
			}
		}

		for _, deltaLogs := range s.GetDeltalogs() {
			for _, l := range deltaLogs.GetBinlogs() {
				segmentSize += l.GetLogSize()
			}
		}

		for _, statsLogs := range s.GetDeltalogs() {
			for _, l := range statsLogs.GetBinlogs() {
				segmentSize += l.GetLogSize()
			}
		}
	}

	return segmentSize
}

// getCollectionAutoCompactionEnabled returns whether auto compaction for collection is enabled.
// if not set, returns global auto compaction config.
func getCollectionAutoCompactionEnabled(properties map[string]string) (bool, error) {
	v, ok := properties[common.CollectionAutoCompactionKey]
	if ok {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return false, err
		}
		return enabled, nil
	}
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool(), nil
}

func getIndexType(indexParams []*commonpb.KeyValuePair) string {
	for _, param := range indexParams {
		if param.Key == common.IndexTypeKey {
			return param.Value
		}
	}
	return invalidIndex
}

func isFlatIndex(indexType string) bool {
	return indexType == flatIndex || indexType == binFlatIndex
}

func parseBuildIDFromFilePath(key string) (UniqueID, error) {
	ss := strings.Split(key, "/")
	if strings.HasSuffix(key, "/") {
		return strconv.ParseInt(ss[len(ss)-2], 10, 64)
	}
	return strconv.ParseInt(ss[len(ss)-1], 10, 64)
}
