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
	"encoding/binary"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
)

const queryBufferSize = 1024

type queryMsg interface {
	msgstream.TsMsg
	GuaranteeTs() Timestamp
	TravelTs() Timestamp
}

type searchMsg struct {
	*msgstream.SearchMsg
	channelNum int
	plan       *SearchPlan
	reqs       []*searchRequest
}

type retrieveMsg struct {
	*msgstream.RetrieveMsg
	channelNum int
	plan       *RetrievePlan
}

type queryResult interface {
	ID() UniqueID
	Type() msgstream.MsgType
	ChannelNum() int // num of channels should be queried
}

type retrieveResult struct {
	err              error
	msg              *retrieveMsg
	segmentRetrieved []UniqueID
	vChannel         Channel
	res              []*segcorepb.RetrieveResults
}

type searchResult struct {
	err                   error
	msg                   *searchMsg
	reqs                  []*searchRequest
	searchResults         []*SearchResult
	sealedSegmentSearched []UniqueID
	vChannel              Channel
}

type queryCollection struct {
	releaseCtx context.Context
	cancel     context.CancelFunc

	collectionID UniqueID
	collection   *Collection
	historical   *historical
	streaming    *streaming

	queryMsgStream       msgstream.MsgStream
	queryResultMsgStream msgstream.MsgStream

	localChunkManager  storage.ChunkManager
	remoteChunkManager storage.ChunkManager
	vectorChunkManager storage.ChunkManager
	localCacheEnabled  bool

	inputStage      *inputStage
	reqStage        *requestHandlerStage
	historicalStage *historicalStage
	vChannelStages  map[Channel]*vChannelStage
	resStage        *resultHandlerStage

	vChannelChan sync.Map
	resChan      chan queryResult
}

type ResultEntityIds []UniqueID

func newQueryCollection(releaseCtx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	historical *historical,
	streaming *streaming,
	factory msgstream.Factory,
	localChunkManager storage.ChunkManager,
	remoteChunkManager storage.ChunkManager,
	localCacheEnabled bool) (*queryCollection, error) {

	queryStream, _ := factory.NewQueryMsgStream(releaseCtx)
	queryResultStream, _ := factory.NewQueryMsgStream(releaseCtx)

	collection, _ := streaming.replica.getCollectionByID(collectionID)

	qc := &queryCollection{
		releaseCtx: releaseCtx,
		cancel:     cancel,

		collectionID: collectionID,
		collection:   collection,
		historical:   historical,
		streaming:    streaming,

		queryMsgStream:       queryStream,
		queryResultMsgStream: queryResultStream,

		localChunkManager:  localChunkManager,
		remoteChunkManager: remoteChunkManager,
		localCacheEnabled:  localCacheEnabled,
	}

	// create query stages
	col, err := qc.streaming.replica.getCollectionByID(qc.collectionID)
	if err != nil {
		return nil, err
	}
	channels := col.getVChannels()

	reqChan := make(chan queryMsg, queryBufferSize)
	iStage := newInputStage(qc.releaseCtx,
		qc.collectionID,
		qc.queryMsgStream,
		reqChan)
	qc.inputStage = iStage

	hisChan := make(chan queryMsg, queryBufferSize)
	for _, c := range channels {
		qc.vChannelChan.Store(c, make(chan queryMsg, queryBufferSize))
	}
	reqStage := newRequestHandlerStage(qc.releaseCtx,
		qc.collectionID,
		reqChan,
		hisChan,
		&qc.vChannelChan,
		qc.streaming,
		qc.historical,
		qc.queryResultMsgStream)
	qc.reqStage = reqStage
	// TODO: expand channel's capacity is not allowed
	qc.resChan = make(chan queryResult, queryBufferSize*(len(channels)+1)) // vChannels + historical
	hisStage := newHistoricalStage(qc.releaseCtx,
		qc.collectionID,
		hisChan,
		qc.resChan,
		qc.historical,
		qc.localChunkManager,
		qc.remoteChunkManager,
		qc.localCacheEnabled)
	qc.historicalStage = hisStage

	qc.vChannelStages = make(map[Channel]*vChannelStage)
	for _, c := range channels {
		qc.addVChannelStage(c)
	}
	resStage := newResultHandlerStage(qc.releaseCtx,
		qc.collectionID,
		qc.streaming,
		qc.historical,
		qc.resChan,
		qc.queryResultMsgStream)
	qc.resStage = resStage

	return qc, nil
}

func (q *queryCollection) start() {
	q.queryMsgStream.Start()
	q.queryResultMsgStream.Start()

	// start stages
	go q.inputStage.start()
	go q.reqStage.start()
	go q.historicalStage.start()
	for c := range q.vChannelStages {
		q.startVChannelStage(c)
	}
	go q.resStage.start()
}

func (q *queryCollection) close() {
	if q.queryMsgStream != nil {
		q.queryMsgStream.Close()
	}
	if q.queryResultMsgStream != nil {
		q.queryResultMsgStream.Close()
	}
}

// vChannel stage management
func (q *queryCollection) startVChannelStage(channel Channel) {
	if _, ok := q.vChannelStages[channel]; !ok {
		return
	}
	go q.vChannelStages[channel].start()
}

func (q *queryCollection) addVChannelStage(channel Channel) {
	if _, ok := q.vChannelStages[channel]; ok {
		log.Warn("vChannelStage has been existed",
			zap.Any("collectionID", q.collectionID),
			zap.Any("vChannel", channel),
		)
	}
	vChannelChan := make(chan queryMsg, queryBufferSize)
	q.vChannelChan.Store(channel, vChannelChan)
	stage := newVChannelStage(q.releaseCtx,
		q.collectionID,
		channel,
		vChannelChan,
		q.resChan,
		q.streaming)

	q.vChannelStages[channel] = stage
}

func (q *queryCollection) removeVChannelStage(channel Channel) {
	if _, ok := q.vChannelStages[channel]; ok {
		q.vChannelStages[channel].stop()
	}
	delete(q.vChannelStages, channel)
	q.vChannelChan.Delete(channel)
}

// result functions
func (s *searchResult) Type() msgstream.MsgType {
	return commonpb.MsgType_Search
}

func (s *searchResult) ID() UniqueID {
	return s.msg.ID()
}

func (s *searchResult) ChannelNum() int {
	return s.msg.channelNum
}

func (r *retrieveResult) Type() msgstream.MsgType {
	return commonpb.MsgType_Retrieve
}

func (r *retrieveResult) ID() UniqueID {
	return r.msg.ID()
}

func (r *retrieveResult) ChannelNum() int {
	return r.msg.channelNum
}

func getSegmentsByPKs(pks []int64, segments []*Segment) (map[int64][]int64, error) {
	if pks == nil {
		return nil, fmt.Errorf("pks is nil when getSegmentsByPKs")
	}
	if segments == nil {
		return nil, fmt.Errorf("segments is nil when getSegmentsByPKs")
	}
	results := make(map[int64][]int64)
	buf := make([]byte, 8)
	for _, segment := range segments {
		for _, pk := range pks {
			binary.BigEndian.PutUint64(buf, uint64(pk))
			exist := segment.pkFilter.Test(buf)
			if exist {
				results[segment.segmentID] = append(results[segment.segmentID], pk)
			}
		}
	}
	return results, nil
}
