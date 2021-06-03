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
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	oplog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"
)

type retrieveCollection struct {
	releaseCtx context.Context
	cancel     context.CancelFunc

	collectionID      UniqueID
	historicalReplica ReplicaInterface
	streamingReplica  ReplicaInterface
	tSafeReplica      TSafeReplicaInterface

	msgBuffer     chan *msgstream.RetrieveMsg
	unsolvedMsgMu sync.Mutex
	unsolvedMsg   []*msgstream.RetrieveMsg

	tSafeMutex   sync.Mutex
	tSafeWatcher *tSafeWatcher

	serviceableTimeMutex sync.Mutex
	serviceableTime      Timestamp

	retrieveResultMsgStream msgstream.MsgStream
}

func newRetrieveCollection(releaseCtx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	historicalReplica ReplicaInterface,
	streamingReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	retrieveResultStream msgstream.MsgStream) *retrieveCollection {
	receiveBufSize := Params.RetrieveReceiveBufSize
	msgBuffer := make(chan *msgstream.RetrieveMsg, receiveBufSize)
	unsolvedMsg := make([]*msgstream.RetrieveMsg, 0)

	rc := &retrieveCollection{
		releaseCtx: releaseCtx,
		cancel:     cancel,

		collectionID:      collectionID,
		historicalReplica: historicalReplica,
		streamingReplica:  streamingReplica,
		tSafeReplica:      tSafeReplica,

		msgBuffer:   msgBuffer,
		unsolvedMsg: unsolvedMsg,

		retrieveResultMsgStream: retrieveResultStream,
	}

	rc.register(collectionID)
	return rc
}

func (rc *retrieveCollection) getServiceableTime() Timestamp {
	rc.serviceableTimeMutex.Lock()
	defer rc.serviceableTimeMutex.Unlock()
	return rc.serviceableTime
}

func (rc *retrieveCollection) setServiceableTime(t Timestamp) {
	rc.serviceableTimeMutex.Lock()
	gracefulTimeInMilliSecond := Params.GracefulTime
	if gracefulTimeInMilliSecond > 0 {
		gracefulTime := tsoutil.ComposeTS(gracefulTimeInMilliSecond, 0)
		rc.serviceableTime = t + gracefulTime
	} else {
		rc.serviceableTime = t
	}
	rc.serviceableTimeMutex.Unlock()
}

func (rc *retrieveCollection) waitNewTSafe() Timestamp {
	// block until dataSyncService updating tSafe
	// TODO: remove and use vChannel
	vChannel := collectionIDToChannel(rc.collectionID)
	// block until dataSyncService updating tSafe
	rc.tSafeWatcher.hasUpdate()
	ts := rc.tSafeReplica.getTSafe(vChannel)
	return ts
}

func (rc *retrieveCollection) start() {
	go rc.receiveRetrieveMsg()
	go rc.doUnsolvedMsgRetrieve()
}

func (rc *retrieveCollection) register(collectionID UniqueID) {
	vChannel := collectionIDToChannel(collectionID)
	rc.tSafeReplica.addTSafe(vChannel)
	rc.tSafeMutex.Lock()
	rc.tSafeWatcher = newTSafeWatcher()
	rc.tSafeMutex.Unlock()
	rc.tSafeReplica.registerTSafeWatcher(vChannel, rc.tSafeWatcher)
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
			sp, ctx := trace.StartSpanFromContext(rm.TraceCtx())
			rm.SetTraceCtx(ctx)
			log.Debug("get retrieve message from msgBuffer",
				zap.Int64("msgID", rm.ID()),
				zap.Int64("collectionID", rm.CollectionID))
			serviceTime := rc.getServiceableTime()
			if rm.BeginTs() > serviceTime {
				bt, _ := tsoutil.ParseTS(rm.BeginTs())
				st, _ := tsoutil.ParseTS(serviceTime)
				log.Debug("querynode::receiveRetrieveMsg: add to unsolvedMsgs",
					zap.Any("sm.BeginTs", bt),
					zap.Any("serviceTime", st),
					zap.Any("delta seconds", (rm.BeginTs()-serviceTime)/(1000*1000*1000)),
					zap.Any("collectionID", rc.collectionID),
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
			log.Debug("doing retrieve in receiveRetrieveMsg...",
				zap.Int64("msgID", rm.ID()),
				zap.Int64("collectionID", rm.CollectionID))
			err := rc.retrieve(rm)
			if err != nil {
				log.Error(err.Error())
				log.Debug("do retrieve failed in receiveRetrieveMsg, prepare to publish failed retrieve result",
					zap.Int64("msgID", rm.ID()),
					zap.Int64("collectionID", rm.CollectionID))
				err2 := rc.publishFailedRetrieveResult(rm, err.Error())
				if err2 != nil {
					log.Error("publish FailedRetrieveResult failed", zap.Error(err2))
				}
			}
			log.Debug("do retrieve done in retrieveRetrieveMsg",
				zap.Int64("msgID", rm.ID()),
				zap.Int64("collectionID", rm.CollectionID))
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
			log.Debug("querynode::doUnsolvedMsgRetrieve: setServiceableTime",
				zap.Any("serviceTime", serviceTime),
			)
			log.Debug("get tSafe from flow graph",
				zap.Int64("collectionID", rc.collectionID),
				zap.Uint64("tSafe", serviceTime))

			retrieveMsg := make([]*msgstream.RetrieveMsg, 0)
			rc.unsolvedMsgMu.Lock()
			tmpMsg := rc.unsolvedMsg
			rc.unsolvedMsg = rc.unsolvedMsg[:0]
			rc.unsolvedMsgMu.Unlock()

			for _, rm := range tmpMsg {
				log.Debug("get retrieve message from unsolvedMsg",
					zap.Int64("msgID", rm.ID()),
					zap.Int64("collectionID", rm.CollectionID))

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
				log.Debug("doing retrieve in doUnsolvedMsgRetrieve...",
					zap.Int64("msgID", rm.ID()),
					zap.Int64("collectionID", rm.CollectionID))
				err := rc.retrieve(rm)
				if err != nil {
					log.Error(err.Error())
					log.Debug("do retrieve failed in doUnsolvedMsgRetrieve, prepare to publish failed retrieve result",
						zap.Int64("msgID", rm.ID()),
						zap.Int64("collectionID", rm.CollectionID))
					err2 := rc.publishFailedRetrieveResult(rm, err.Error())
					if err2 != nil {
						log.Error("publish FailedRetrieveResult failed", zap.Error(err2))
					}
				}
				sp.Finish()
				log.Debug("do retrieve done in doUnsolvedMsgRetrieve",
					zap.Int64("msgID", rm.ID()),
					zap.Int64("collectionID", rm.CollectionID))
			}
			log.Debug("doUnsolvedMsgRetrieve, do retrieve done", zap.Int("num of retrieveMsg", len(retrieveMsg)))
		}
	}
}

func (rc *retrieveCollection) retrieve(retrieveMsg *msgstream.RetrieveMsg) error {
	// TODO(yukun)
	resultChannelInt := 0
	retrieveResultMsg := &msgstream.RetrieveResultMsg{
		BaseMsg: msgstream.BaseMsg{Ctx: retrieveMsg.Ctx, HashValues: []uint32{uint32(resultChannelInt)}},
		RetrieveResults: internalpb.RetrieveResults{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_RetrieveResult,
				MsgID:    retrieveMsg.Base.MsgID,
				SourceID: retrieveMsg.Base.SourceID,
			},
			Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			ResultChannelID: retrieveMsg.ResultChannelID,
		},
	}
	err := rc.publishRetrieveResult(retrieveResultMsg, retrieveMsg.CollectionID)
	if err != nil {
		return err
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
