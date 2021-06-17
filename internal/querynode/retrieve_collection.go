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
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/golang/protobuf/proto"
	oplog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
)

type retrieveCollection struct {
	releaseCtx context.Context
	cancel     context.CancelFunc

	collectionID UniqueID
	historical   *historical
	streaming    *streaming

	msgBuffer     chan *msgstream.RetrieveMsg
	unsolvedMsgMu sync.Mutex
	unsolvedMsg   []*msgstream.RetrieveMsg

	tSafeWatchers     map[Channel]*tSafeWatcher
	watcherSelectCase []reflect.SelectCase

	serviceableTimeMutex sync.Mutex
	serviceableTime      Timestamp

	retrieveResultMsgStream msgstream.MsgStream
}

func newRetrieveCollection(releaseCtx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	historical *historical,
	streaming *streaming,
	retrieveResultStream msgstream.MsgStream) *retrieveCollection {
	receiveBufSize := Params.RetrieveReceiveBufSize
	msgBuffer := make(chan *msgstream.RetrieveMsg, receiveBufSize)
	unsolvedMsg := make([]*msgstream.RetrieveMsg, 0)

	rc := &retrieveCollection{
		releaseCtx: releaseCtx,
		cancel:     cancel,

		collectionID: collectionID,
		historical:   historical,
		streaming:    streaming,

		tSafeWatchers: make(map[Channel]*tSafeWatcher),

		msgBuffer:   msgBuffer,
		unsolvedMsg: unsolvedMsg,

		retrieveResultMsgStream: retrieveResultStream,
	}

	rc.register()
	return rc
}

func (rc *retrieveCollection) getServiceableTime() Timestamp {
	rc.serviceableTimeMutex.Lock()
	defer rc.serviceableTimeMutex.Unlock()
	return rc.serviceableTime
}

func (rc *retrieveCollection) setServiceableTime(t Timestamp) {
	rc.serviceableTimeMutex.Lock()
	defer rc.serviceableTimeMutex.Unlock()

	if t < rc.serviceableTime {
		return
	}

	gracefulTimeInMilliSecond := Params.GracefulTime
	if gracefulTimeInMilliSecond > 0 {
		gracefulTime := tsoutil.ComposeTS(gracefulTimeInMilliSecond, 0)
		rc.serviceableTime = t + gracefulTime
	} else {
		rc.serviceableTime = t
	}
}

func (rc *retrieveCollection) waitNewTSafe() Timestamp {
	// block until any vChannel updating tSafe
	_, _, recvOK := reflect.Select(rc.watcherSelectCase)
	if !recvOK {
		log.Error("tSafe has been closed")
		return invalidTimestamp
	}
	t := Timestamp(math.MaxInt64)
	for channel := range rc.tSafeWatchers {
		ts := rc.streaming.tSafeReplica.getTSafe(channel)
		if ts <= t {
			t = ts
		}
	}
	return t
}

func (rc *retrieveCollection) start() {
	go rc.receiveRetrieveMsg()
	go rc.doUnsolvedMsgRetrieve()
}

func (rc *retrieveCollection) register() {
	// register tSafe watcher and init watcher select case
	collection, err := rc.streaming.replica.getCollectionByID(rc.collectionID)
	if err != nil {
		log.Error(err.Error())
		return
	}

	rc.watcherSelectCase = make([]reflect.SelectCase, 0)
	for _, channel := range collection.getVChannels() {
		rc.streaming.tSafeReplica.addTSafe(channel)
		rc.tSafeWatchers[channel] = newTSafeWatcher()
		rc.streaming.tSafeReplica.registerTSafeWatcher(channel, rc.tSafeWatchers[channel])
		rc.watcherSelectCase = append(rc.watcherSelectCase, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(rc.tSafeWatchers[channel].watcherChan()),
		})
	}
}

func (rc *retrieveCollection) addToUnsolvedMsg(msg *msgstream.RetrieveMsg) {
	rc.unsolvedMsgMu.Lock()
	defer rc.unsolvedMsgMu.Unlock()
	rc.unsolvedMsg = append(rc.unsolvedMsg, msg)
}

func (rc *retrieveCollection) receiveRetrieveMsg() {
	for {
		select {
		case <-rc.releaseCtx.Done():
			log.Debug("stop retrieveCollection's receiveRetrieveMsg", zap.Int64("collectionID", rc.collectionID))
			return
		case rm := <-rc.msgBuffer:
			log.Info("RetrieveCollection get retrieve message from msgBuffer",
				zap.Int64("collectionID", rm.CollectionID),
				zap.Int64("requestID", rm.ID()),
				zap.Any("requestType", "retrieve"),
			)

			sp, ctx := trace.StartSpanFromContext(rm.TraceCtx())
			rm.SetTraceCtx(ctx)
			serviceTime := rc.getServiceableTime()
			if rm.BeginTs() > serviceTime {
				bt, _ := tsoutil.ParseTS(rm.BeginTs())
				st, _ := tsoutil.ParseTS(serviceTime)
				log.Debug("Timestamp of retrieve request great than serviceTime, add to unsolvedMsgs",
					zap.Any("sm.BeginTs", bt),
					zap.Any("serviceTime", st),
					zap.Any("delta seconds", (rm.BeginTs()-serviceTime)/(1000*1000*1000)),
					zap.Any("collectionID", rc.collectionID),
					zap.Int64("collectionID", rm.CollectionID),
					zap.Int64("requestID", rm.ID()),
					zap.Any("requestType", "retrieve"),
				)
				rc.addToUnsolvedMsg(rm)
				sp.LogFields(
					oplog.String("send to unsolved retrieve buffer", "send to unsolved buffer"),
					oplog.Object("begin ts", bt),
					oplog.Object("serviceTime", st),
					oplog.Float64("delta seconds", float64(rm.BeginTs()-serviceTime)/(1000.0*1000.0*1000.0)),
				)
				sp.Finish()
				continue
			}

			log.Info("Doing retrieve in receiveRetrieveMsg...",
				zap.Int64("collectionID", rm.CollectionID),
				zap.Int64("requestID", rm.ID()),
				zap.Any("requestType", "retrieve"),
			)
			err := rc.retrieve(rm)

			if err != nil {
				log.Error(err.Error(),
					zap.Int64("requestID", rm.ID()),
					zap.Any("requestType", "retrieve"),
				)

				log.Debug("Failed to execute retrieve, prepare to publish failed retrieve result",
					zap.Int64("collectionID", rm.CollectionID),
					zap.Int64("requestID", rm.ID()),
					zap.Any("requestType", "retrieve"),
				)

				err2 := rc.publishFailedRetrieveResult(rm, err.Error())
				if err2 != nil {
					log.Error("Failed to publish FailedRetrieveResult",
						zap.Error(err2),
						zap.Int64("requestID", rm.ID()),
						zap.Any("requestType", "retrieve"),
					)
				}
			}

			log.Debug("Do retrieve done in retrieveRetrieveMsg",
				zap.Int64("msgID", rm.ID()),
				zap.Int64("collectionID", rm.CollectionID),
				zap.Int64("requestID", rm.ID()),
				zap.Any("requestType", "retrieve"),
			)
			sp.Finish()
		}
	}
}

func (rc *retrieveCollection) doUnsolvedMsgRetrieve() {
	for {
		select {
		case <-rc.releaseCtx.Done():
			log.Debug("stop retrieveCollection's doUnsolvedMsgRetrieve", zap.Int64("collectionID", rc.collectionID))
			return
		default:
			serviceTime := rc.waitNewTSafe()
			rc.setServiceableTime(serviceTime)
			log.Debug("Update serviceTime",
				zap.Any("serviceTime", serviceTime),
				zap.Uint64("tSafe", serviceTime),
				zap.Int64("collectionID", rc.collectionID),
			)

			retrieveMsg := make([]*msgstream.RetrieveMsg, 0)
			rc.unsolvedMsgMu.Lock()
			tmpMsg := rc.unsolvedMsg
			rc.unsolvedMsg = rc.unsolvedMsg[:0]
			rc.unsolvedMsgMu.Unlock()

			for _, rm := range tmpMsg {
				log.Debug("Get retrieve message from unsolvedMsg",
					zap.Int64("collectionID", rm.CollectionID),
					zap.Int64("requestID", rm.ID()),
					zap.Any("requestType", "retrieve"),
				)

				if rm.EndTs() <= serviceTime {
					retrieveMsg = append(retrieveMsg, rm)
					continue
				}
				rc.addToUnsolvedMsg(rm)
			}

			if len(retrieveMsg) <= 0 {
				continue
			}

			for _, rm := range retrieveMsg {
				sp, ctx := trace.StartSpanFromContext(rm.TraceCtx())
				rm.SetTraceCtx(ctx)

				log.Debug("Doing retrieve in doUnsolvedMsgRetrieve...",
					zap.Int64("collectionID", rm.CollectionID),
					zap.Int64("requestID", rm.ID()),
					zap.Any("requestType", "retrieve"),
				)
				err := rc.retrieve(rm)

				if err != nil {
					log.Error(err.Error(),
						zap.Int64("requestID", rm.ID()),
						zap.Any("requestType", "retrieve"),
					)

					log.Debug("Failed to do retrieve in doUnsolvedMsgRetrieve, prepare to publish failed retrieve result",
						zap.Int64("collectionID", rm.CollectionID),
						zap.Int64("requestID", rm.ID()),
						zap.Any("requestType", "retrieve"),
					)

					err2 := rc.publishFailedRetrieveResult(rm, err.Error())
					if err2 != nil {
						log.Error("Failed to publish FailedRetrieveResult",
							zap.Error(err2),
							zap.Int64("requestID", rm.ID()),
							zap.Any("requestType", "retrieve"),
						)
					}
				}

				sp.Finish()
				log.Debug("Do retrieve done in doUnsolvedMsgRetrieve",
					zap.Int64("collectionID", rm.CollectionID),
					zap.Int64("requestID", rm.ID()),
					zap.Any("requestType", "retrieve"),
				)
			}
			log.Debug("doUnsolvedMsgRetrieve, do retrieve done", zap.Int("num of retrieveMsg", len(retrieveMsg)))
		}
	}
}

func mergeRetrieveResults(dataArr []*segcorepb.RetrieveResults) (*segcorepb.RetrieveResults, error) {
	var final *segcorepb.RetrieveResults
	for _, data := range dataArr {
		if data == nil {
			continue
		}

		if final == nil {
			final = proto.Clone(data).(*segcorepb.RetrieveResults)
			continue
		}

		proto.Merge(final.Ids, data.Ids)
		if len(final.FieldsData) != len(data.FieldsData) {
			return nil, fmt.Errorf("mismatch FieldData in RetrieveResults")
		}

		for i := range final.FieldsData {
			proto.Merge(final.FieldsData[i], data.FieldsData[i])
		}
	}

	return final, nil
}

func (rc *retrieveCollection) retrieve(retrieveMsg *msgstream.RetrieveMsg) error {
	// TODO(yukun)
	// step 1: get retrieve object and defer destruction
	// step 2: for each segment, call retrieve to get ids proto buffer
	// step 3: merge all proto in go
	// step 4: publish results
	// retrieveProtoBlob, err := proto.Marshal(&retrieveMsg.RetrieveRequest)
	sp, ctx := trace.StartSpanFromContext(retrieveMsg.TraceCtx())
	defer sp.Finish()
	retrieveMsg.SetTraceCtx(ctx)
	timestamp := retrieveMsg.Base.Timestamp

	collectionID := retrieveMsg.CollectionID
	collection, err := rc.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	req := &segcorepb.RetrieveRequest{
		Ids:          retrieveMsg.Ids,
		OutputFields: retrieveMsg.OutputFields,
	}

	plan, err := createRetrievePlan(collection, req, timestamp)
	if err != nil {
		return err
	}
	defer plan.delete()

	var partitionIDsInHistorical []UniqueID
	var partitionIDsInStreaming []UniqueID
	partitionIDsInQuery := retrieveMsg.PartitionIDs
	if len(partitionIDsInQuery) == 0 {
		partitionIDsInHistoricalCol, err1 := rc.historical.replica.getPartitionIDs(collectionID)
		partitionIDsInStreamingCol, err2 := rc.streaming.replica.getPartitionIDs(collectionID)
		if err1 != nil && err2 != nil {
			return err2
		}
		if len(partitionIDsInHistoricalCol) == 0 {
			return errors.New("none of this collection's partition has been loaded")
		}
		partitionIDsInHistorical = partitionIDsInHistoricalCol
		partitionIDsInStreaming = partitionIDsInStreamingCol
	} else {
		for _, id := range partitionIDsInQuery {
			_, err1 := rc.historical.replica.getPartitionByID(id)
			if err1 == nil {
				partitionIDsInHistorical = append(partitionIDsInHistorical, id)
			}
			_, err2 := rc.streaming.replica.getPartitionByID(id)
			if err2 == nil {
				partitionIDsInStreaming = append(partitionIDsInStreaming, id)
			}
			if err1 != nil && err2 != nil {
				return err2
			}
		}
	}

	sealedSegmentRetrieved := make([]UniqueID, 0)
	var mergeList []*segcorepb.RetrieveResults
	for _, partitionID := range partitionIDsInHistorical {
		segmentIDs, err := rc.historical.replica.getSegmentIDs(partitionID)
		if err != nil {
			return err
		}
		for _, segmentID := range segmentIDs {
			segment, err := rc.historical.replica.getSegmentByID(segmentID)
			if err != nil {
				return err
			}
			result, err := segment.segmentGetEntityByIds(plan)
			if err != nil {
				return err
			}
			mergeList = append(mergeList, result)
			sealedSegmentRetrieved = append(sealedSegmentRetrieved, segmentID)
		}
	}

	for _, partitionID := range partitionIDsInStreaming {
		segmentIDs, err := rc.streaming.replica.getSegmentIDs(partitionID)
		if err != nil {
			return err
		}
		for _, segmentID := range segmentIDs {
			segment, err := rc.streaming.replica.getSegmentByID(segmentID)
			if err != nil {
				return err
			}
			result, err := segment.segmentGetEntityByIds(plan)
			if err != nil {
				return err
			}
			mergeList = append(mergeList, result)
		}
	}

	result, err := mergeRetrieveResults(mergeList)
	if err != nil {
		return err
	}

	resultChannelInt := 0
	retrieveResultMsg := &msgstream.RetrieveResultMsg{
		BaseMsg: msgstream.BaseMsg{Ctx: retrieveMsg.Ctx, HashValues: []uint32{uint32(resultChannelInt)}},
		RetrieveResults: internalpb.RetrieveResults{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_RetrieveResult,
				MsgID:    retrieveMsg.Base.MsgID,
				SourceID: retrieveMsg.Base.SourceID,
			},
			Status:                    &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Ids:                       result.Ids,
			FieldsData:                result.FieldsData,
			ResultChannelID:           retrieveMsg.ResultChannelID,
			SealedSegmentIDsRetrieved: sealedSegmentRetrieved,
			ChannelIDsRetrieved:       collection.getPChannels(),
			//TODO(yukun):: get global sealed segment from etcd
			GlobalSealedSegmentIDs: sealedSegmentRetrieved,
		},
	}
	log.Debug("QueryNode RetrieveResultMsg",
		zap.Any("pChannels", collection.getPChannels()),
		zap.Any("collectionID", collection.ID()),
		zap.Any("sealedSegmentRetrieved", sealedSegmentRetrieved),
	)
	err3 := rc.publishRetrieveResult(retrieveResultMsg, retrieveMsg.CollectionID)
	if err3 != nil {
		return err3
	}
	return nil
}

func (rc *retrieveCollection) publishRetrieveResult(msg msgstream.TsMsg, collectionID UniqueID) error {
	log.Debug("publishing retrieve result...",
		zap.Int64("msgID", msg.ID()),
		zap.Int64("collectionID", collectionID))
	span, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	defer span.Finish()
	msg.SetTraceCtx(ctx)
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	err := rc.retrieveResultMsgStream.Produce(&msgPack)
	if err != nil {
		log.Error(err.Error())
	} else {
		log.Debug("publish retrieve result done",
			zap.Int64("msgID", msg.ID()),
			zap.Int64("collectionID", collectionID))
	}
	return err
}

func (rc *retrieveCollection) publishFailedRetrieveResult(retrieveMsg *msgstream.RetrieveMsg, errMsg string) error {
	span, ctx := trace.StartSpanFromContext(retrieveMsg.TraceCtx())
	defer span.Finish()
	retrieveMsg.SetTraceCtx(ctx)
	msgPack := msgstream.MsgPack{}

	resultChannelInt := 0
	retrieveResultMsg := &msgstream.RetrieveResultMsg{
		BaseMsg: msgstream.BaseMsg{HashValues: []uint32{uint32(resultChannelInt)}},
		RetrieveResults: internalpb.RetrieveResults{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_RetrieveResult,
				MsgID:     retrieveMsg.Base.MsgID,
				Timestamp: retrieveMsg.Base.Timestamp,
				SourceID:  retrieveMsg.Base.SourceID,
			},
			Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: errMsg},
			ResultChannelID: retrieveMsg.ResultChannelID,
			Ids:             nil,
			FieldsData:      nil,
		},
	}

	msgPack.Msgs = append(msgPack.Msgs, retrieveResultMsg)
	err := rc.retrieveResultMsgStream.Produce(&msgPack)
	if err != nil {
		return err
	}

	return nil
}
