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
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
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
	ttRetention := pts.Add(-time.Duration(Params.CommonCfg.RetentionDuration) * time.Second)
	ttRetentionLogic := tsoutil.ComposeTS(ttRetention.UnixNano()/int64(time.Millisecond), 0)

	// TODO, change to collection level
	if Params.CommonCfg.EntityExpirationTTL > 0 {
		ttexpired := pts.Add(-Params.CommonCfg.EntityExpirationTTL)
		ttexpiredLogic := tsoutil.ComposeTS(ttexpired.UnixNano()/int64(time.Millisecond), 0)
		return &compactTime{ttRetentionLogic, ttexpiredLogic, Params.CommonCfg.EntityExpirationTTL}, nil
	}
	// no expiration time
	return &compactTime{ttRetentionLogic, 0, 0}, nil
}

func FilterInIndexedSegments(handler Handler, indexCoord types.IndexCoord, segments ...*SegmentInfo) ([]*SegmentInfo, error) {
	if len(segments) == 0 {
		return nil, nil
	}

	segmentMap := make(map[int64]*SegmentInfo)
	collectionSegments := make(map[int64][]int64)
	collectionIndexedSegments := make(map[int64][]int64)
	// TODO(yah01): This can't handle the case of multiple vector fields exist,
	// modify it if we support multiple vector fields.
	vecFieldID := make(map[int64]int64)
	for _, segment := range segments {
		collectionID := segment.GetCollectionID()
		segmentMap[segment.GetID()] = segment
		collectionSegments[collectionID] = append(collectionSegments[collectionID], segment.GetID())
	}
	for collection, segmentIDs := range collectionSegments {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		coll, err := handler.GetCollection(ctx, collection)
		cancel()
		if err != nil {
			log.Warn("failed to get collection schema", zap.Error(err))
			return nil, err
		}
		for _, field := range coll.Schema.GetFields() {
			if field.GetDataType() == schemapb.DataType_BinaryVector ||
				field.GetDataType() == schemapb.DataType_FloatVector {
				vecFieldID[collection] = field.GetFieldID()
				break
			}
		}
		collectionIndexedSegments[collection] = make([]int64, 0, len(segmentIDs))
	}

	const (
		MaxSegmentNumPerRPC = 1000
	)

	group := &errgroup.Group{}
	mutex := &sync.Mutex{}
	for collectionID, segmentIDs := range collectionSegments {
		collectionID := collectionID
		segmentIDs := segmentIDs
		group.Go(func() error {
			segmentBatches := lo.Chunk(segmentIDs, MaxSegmentNumPerRPC)
			for _, segmentBatch := range segmentBatches {
				segmentInfos, err := getIndexInfos(indexCoord, collectionID, segmentBatch)
				if err != nil {
					return err
				}
				indexed := extractSegmentsWithVectorIndex(vecFieldID, segmentInfos)
				mutex.Lock()
				collectionIndexedSegments[collectionID] = append(collectionIndexedSegments[collectionID], indexed...)
				mutex.Unlock()
			}
			return nil
		})
	}
	err := group.Wait()
	if err != nil {
		return nil, err
	}

	indexedSegments := make([]*SegmentInfo, 0)
	for _, segmentIDs := range collectionIndexedSegments {
		for _, segment := range segmentIDs {
			if info, ok := segmentMap[segment]; ok {
				delete(segmentMap, segment)
				indexedSegments = append(indexedSegments, info)
			}
		}
	}

	return indexedSegments, nil
}

func getIndexInfos(indexCoord types.IndexCoord, collectionID int64, segmentIDs []int64) (map[int64]*indexpb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // TODO: make it configurable
	defer cancel()
	resp, err := indexCoord.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{
		CollectionID: collectionID,
		SegmentIDs:   segmentIDs,
	})
	if err != nil {
		log.Warn("failed to get index of collection",
			zap.Int64("collection", collectionID),
			zap.Int64s("segments", segmentIDs),
			zap.Error(err),
		)
		return nil, err
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("indexcoord return error when get index",
			zap.Int64("collection", collectionID),
			zap.Int64s("segments", segmentIDs),
			zap.String("status", resp.GetStatus().String()),
			zap.String("reason", resp.GetStatus().GetReason()),
		)
		return nil, fmt.Errorf("GetIndex failed for segments, status: %s, reason: %s",
			resp.GetStatus().GetErrorCode().String(), resp.GetStatus().GetReason())
	}
	return resp.GetSegmentInfo(), nil
}

func extractSegmentsWithVectorIndex(vecFieldID map[int64]int64, segentIndexInfo map[int64]*indexpb.SegmentInfo) []int64 {
	indexedSegments := make(typeutil.UniqueSet)
	for _, indexInfo := range segentIndexInfo {
		for _, index := range indexInfo.GetIndexInfos() {
			if index.GetFieldID() == vecFieldID[indexInfo.GetCollectionID()] {
				indexedSegments.Insert(indexInfo.GetSegmentID())
				break
			}
		}
	}
	return indexedSegments.Collect()
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

	return Params.CommonCfg.EntityExpirationTTL, nil
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
