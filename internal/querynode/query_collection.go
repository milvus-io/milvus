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

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
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
	msg              *retrieveMsg
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

	queryStream, _ := factory.NewQueryMsgStream(releaseCtx)
	queryResultStream, _ := factory.NewQueryMsgStream(releaseCtx)

	qc := &queryCollection{
		releaseCtx: releaseCtx,
		cancel:     cancel,

		collectionID: collectionID,
		historical:   historical,
		streaming:    streaming,

		queryMsgStream:       queryStream,
		queryResultMsgStream: queryResultStream,
	}

	return qc
}

func (q *queryCollection) start() error {
	q.queryMsgStream.Start()
	q.queryResultMsgStream.Start()

	// create query stages
	col, err := q.streaming.replica.getCollectionByID(q.collectionID)
	if err != nil {
		return err
	}
	channels := col.getVChannels()

	lbChan := make(chan *msgstream.LoadBalanceSegmentsMsg, queryBufferSize)
	reqChan := make(chan queryMsg, queryBufferSize)
	iStage := newInputStage(q.releaseCtx,
		q.collectionID,
		q.queryMsgStream,
		lbChan,
		reqChan)
	lbStage := newLoadBalanceStage(q.releaseCtx,
		q.collectionID,
		lbChan)

	hisChan := make(chan queryMsg, queryBufferSize)
	vChannelChan := make(map[Channel]chan queryMsg)
	unsolvedChan := make(map[Channel]chan queryMsg)
	for _, c := range channels {
		vChannelChan[c] = make(chan queryMsg, queryBufferSize)
		unsolvedChan[c] = make(chan queryMsg, queryBufferSize)
	}
	reqStage := newRequestHandlerStage(q.releaseCtx,
		q.collectionID,
		reqChan,
		hisChan,
		vChannelChan,
		q.streaming,
		q.historical,
		q.queryResultMsgStream)
	resChan := make(chan queryResult, queryBufferSize*(len(channels)+1)) // vChannels + historical
	hisStage := newHistoricalStage(q.releaseCtx,
		q.collectionID,
		hisChan,
		resChan,
		q.historical)

	vcStages := make(map[Channel]*vChannelStage)
	unsolvedStages := make(map[Channel]*unsolvedStage)
	for _, c := range channels {
		vcStages[c] = newVChannelStage(q.releaseCtx,
			q.collectionID,
			c,
			vChannelChan[c],
			unsolvedChan[c],
			resChan,
			q.streaming)
		unsolvedStages[c] = newUnsolvedStage(q.releaseCtx,
			q.collectionID,
			c,
			unsolvedChan[c],
			resChan,
			q.streaming,
			q.queryResultMsgStream)
	}
	resStage := newResultHandlerStage(q.releaseCtx,
		q.collectionID,
		q.streaming,
		q.historical,
		resChan,
		q.queryResultMsgStream,
		len(channels))

	// start stages
	go iStage.start()
	go lbStage.start()
	go reqStage.start()
	go hisStage.start()
	for _, s := range vcStages {
		go s.start()
	}
	for _, s := range unsolvedStages {
		go s.start()
	}
	go resStage.start()

	return nil
}

func (q *queryCollection) close() {
	if q.queryMsgStream != nil {
		q.queryMsgStream.Close()
	}
	if q.queryResultMsgStream != nil {
		q.queryResultMsgStream.Close()
	}
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
