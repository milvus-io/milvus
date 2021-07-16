// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"reflect"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

const queryBufferSize = 1024

type queryMsg interface {
	msgstream.TsMsg
	GuaranteeTs() Timestamp
	TravelTs() Timestamp
}

type searchMsg struct {
	*msgstream.SearchMsg
	plan *SearchPlan
	reqs []*searchRequest
}

type retrieveMsg struct {
	*msgstream.RetrieveMsg
	plan *RetrievePlan
}

type queryResult interface {
	ID() UniqueID
	Type() msgstream.MsgType
}

type retrieveResult struct {
	err              error
	msg              *msgstream.RetrieveMsg
	segmentRetrieved []UniqueID
	res              []*segcorepb.RetrieveResults
}

type searchResult struct {
	err                   error
	msg                   *searchMsg
	reqs                  []*searchRequest
	searchResults         []*SearchResult
	matchedSegments       []*Segment
	sealedSegmentSearched []UniqueID
}

type queryCollection struct {
	releaseCtx context.Context
	cancel     context.CancelFunc

	collectionID UniqueID
	historical   *historical
	streaming    *streaming

	unsolvedMsgMu sync.Mutex // guards unsolvedMsg
	unsolvedMsg   []queryMsg

	tSafeWatchers     map[Channel]*tSafeWatcher
	watcherSelectCase []reflect.SelectCase

	serviceableTimeMutex sync.Mutex // guards serviceableTime
	serviceableTime      Timestamp

	queryMsgStream       msgstream.MsgStream
	queryResultMsgStream msgstream.MsgStream
}

type ResultEntityIds []UniqueID

func newQueryCollection(releaseCtx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	historical *historical,
	streaming *streaming,
	factory msgstream.Factory) *queryCollection {

	unsolvedMsg := make([]queryMsg, 0)

	queryStream, _ := factory.NewQueryMsgStream(releaseCtx)
	queryResultStream, _ := factory.NewQueryMsgStream(releaseCtx)

	qc := &queryCollection{
		releaseCtx: releaseCtx,
		cancel:     cancel,

		collectionID: collectionID,
		historical:   historical,
		streaming:    streaming,

		tSafeWatchers: make(map[Channel]*tSafeWatcher),

		unsolvedMsg: unsolvedMsg,

		queryMsgStream:       queryStream,
		queryResultMsgStream: queryResultStream,
	}

	return qc
}

func (q *queryCollection) start() {
	go q.queryMsgStream.Start()
	go q.queryResultMsgStream.Start()
}

func (q *queryCollection) close() {
	if q.queryMsgStream != nil {
		q.queryMsgStream.Close()
	}
	if q.queryResultMsgStream != nil {
		q.queryResultMsgStream.Close()
	}
}

func (q *queryCollection) getVectorOutputFieldIDs(msg queryMsg) ([]int64, error) {
	var collID UniqueID
	var outputFieldsID []int64
	var resultFieldIDs []int64

	msgType := msg.Type()
	switch msgType {
	case commonpb.MsgType_Retrieve:
		retrieveMsg := msg.(*msgstream.RetrieveMsg)
		collID = retrieveMsg.CollectionID
		outputFieldsID = retrieveMsg.OutputFieldsId
	case commonpb.MsgType_Search:
		searchMsg := msg.(*msgstream.SearchMsg)
		collID = searchMsg.CollectionID
		outputFieldsID = searchMsg.OutputFieldsId
	default:
		return resultFieldIDs, fmt.Errorf("receive invalid msgType = %d", msgType)
	}

	vecFields, err := q.historical.replica.getVecFieldIDsByCollectionID(collID)
	if err != nil {
		return resultFieldIDs, err
	}

	for _, fieldID := range vecFields {
		if funcutil.SliceContain(outputFieldsID, fieldID) {
			resultFieldIDs = append(resultFieldIDs, fieldID)
		}
	}
	return resultFieldIDs, nil
}

func (q *queryCollection) fillVectorOutputFieldsIfNeeded(msg queryMsg, segment *Segment, result *segcorepb.RetrieveResults) error {
	// result is not empty
	if len(result.Offset) <= 0 {
		return nil
	}

	// get all vector output field ids
	vecOutputFieldIDs, err := q.getVectorOutputFieldIDs(msg)
	if err != nil {
		return err
	}

	// output_fields contain vector field
	for _, vecOutputFieldID := range vecOutputFieldIDs {
		log.Debug("CYD - ", zap.Int64("vecOutputFieldID", vecOutputFieldID))
		vecFieldInfo, err := segment.getVectorFieldInfo(vecOutputFieldID)
		if err != nil {
			return fmt.Errorf("cannot get vector field info, fileID %d", vecOutputFieldID)
		}
		// vector field raw data is not loaded into memory
		if !vecFieldInfo.getRawDataInMemory() {
			if err = q.historical.loader.loadSegmentVectorFieldsData(vecFieldInfo); err != nil {
				return err
			}
			if err = segment.fillRetrieveResults(result, vecOutputFieldID, vecFieldInfo); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *retrieveResult) Type() msgstream.MsgType {
	return commonpb.MsgType_Retrieve
}

func (s *searchResult) Type() msgstream.MsgType {
	return commonpb.MsgType_Search
}

func (r *retrieveResult) ID() UniqueID {
	return r.msg.ID()
}

func (s *searchResult) ID() UniqueID {
	return s.msg.ID()
}
