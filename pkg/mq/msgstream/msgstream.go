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

package msgstream

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// UniqueID is an alias for short
type UniqueID = typeutil.UniqueID

// Timestamp is an alias for short
type Timestamp = typeutil.Timestamp

// IntPrimaryKey is an alias for short
type IntPrimaryKey = typeutil.IntPrimaryKey

// MsgPosition is an alias for short
type MsgPosition = msgpb.MsgPosition

// MessageID is an alias for short
type MessageID = common.MessageID

// MsgPack represents a batch of msg in msgstream
type MsgPack struct {
	BeginTs        Timestamp
	EndTs          Timestamp
	Msgs           []TsMsg
	StartPositions []*MsgPosition
	EndPositions   []*MsgPosition
}

// ConsumeMsgPack represents a batch of msg in consumer
type ConsumeMsgPack struct {
	BeginTs        Timestamp
	EndTs          Timestamp
	Msgs           []PackMsg
	StartPositions []*MsgPosition
	EndPositions   []*MsgPosition
}

// PackMsg used for ConumserMsgPack
// support fetch some properties metric
type PackMsg interface {
	GetPosition() *msgpb.MsgPosition
	SetPosition(*msgpb.MsgPosition)

	GetSize() int
	GetTimestamp() uint64
	GetChannel() string
	GetMessageID() []byte
	GetID() int64
	GetType() commonpb.MsgType
	GetReplicateID() string

	SetTraceCtx(ctx context.Context)
	Unmarshal(unmarshalDispatcher UnmarshalDispatcher) (TsMsg, error)
}

// UnmarshalledMsg pack unmarshalled tsMsg as PackMsg
// For Compatibility or Test
type UnmarshalledMsg struct {
	msg TsMsg
}

func (m *UnmarshalledMsg) GetTimestamp() uint64 {
	return m.msg.BeginTs()
}

func (m *UnmarshalledMsg) GetChannel() string {
	return m.msg.Position().GetChannelName()
}

func (m *UnmarshalledMsg) GetMessageID() []byte {
	return m.msg.Position().GetMsgID()
}

func (m *UnmarshalledMsg) GetID() int64 {
	return m.msg.ID()
}

func (m *UnmarshalledMsg) GetType() commonpb.MsgType {
	return m.msg.Type()
}

func (m *UnmarshalledMsg) GetSize() int {
	return m.msg.Size()
}

func (m *UnmarshalledMsg) GetReplicateID() string {
	msgBase, ok := m.msg.(interface{ GetBase() *commonpb.MsgBase })
	if !ok {
		log.Warn("fail to get msg base, please check it", zap.Any("type", m.msg.Type()))
		return ""
	}
	return msgBase.GetBase().GetReplicateInfo().GetReplicateID()
}

func (m *UnmarshalledMsg) SetPosition(pos *msgpb.MsgPosition) {
	m.msg.SetPosition(pos)
}

func (m *UnmarshalledMsg) GetPosition() *msgpb.MsgPosition {
	return m.msg.Position()
}

func (m *UnmarshalledMsg) SetTraceCtx(ctx context.Context) {
	m.msg.SetTraceCtx(ctx)
}

func (m *UnmarshalledMsg) Unmarshal(unmarshalDispatcher UnmarshalDispatcher) (TsMsg, error) {
	return m.msg, nil
}

// MarshaledMsg pack marshaled tsMsg
// and parse properties
type MarshaledMsg struct {
	msg         common.Message
	pos         *MsgPosition
	msgType     MsgType
	msgID       int64
	timestamp   uint64
	vchannel    string
	replicateID string
	traceCtx    context.Context
}

func (m *MarshaledMsg) GetTimestamp() uint64 {
	return m.timestamp
}

func (m *MarshaledMsg) GetChannel() string {
	return m.vchannel
}

func (m *MarshaledMsg) GetMessageID() []byte {
	return m.msg.ID().Serialize()
}

func (m *MarshaledMsg) GetID() int64 {
	return m.msgID
}

func (m *MarshaledMsg) GetType() commonpb.MsgType {
	return m.msgType
}

func (m *MarshaledMsg) GetSize() int {
	return len(m.msg.Payload())
}

func (m *MarshaledMsg) GetReplicateID() string {
	return m.replicateID
}

func (m *MarshaledMsg) SetPosition(pos *msgpb.MsgPosition) {
	m.pos = pos
}

func (m *MarshaledMsg) GetPosition() *msgpb.MsgPosition {
	return m.pos
}

func (m *MarshaledMsg) SetTraceCtx(ctx context.Context) {
	m.traceCtx = ctx
}

func (m *MarshaledMsg) Unmarshal(unmarshalDispatcher UnmarshalDispatcher) (TsMsg, error) {
	tsMsg, err := GetTsMsgFromConsumerMsg(unmarshalDispatcher, m.msg)
	if err != nil {
		return nil, err
	}
	tsMsg.SetTraceCtx(m.traceCtx)
	tsMsg.SetPosition(m.pos)
	return tsMsg, nil
}

func NewMarshaledMsg(msg common.Message, group string) (PackMsg, error) {
	properties := msg.Properties()
	vchannel, ok := properties[common.ChannelTypeKey]
	if !ok {
		return nil, fmt.Errorf("get channel namse from msg properties failed")
	}

	tsStr, ok := properties[common.TimestampTypeKey]
	if !ok {
		return nil, fmt.Errorf("get minTs from msg properties failed")
	}

	timestamp, err := strconv.ParseUint(tsStr, 10, 64)
	if err != nil {
		log.Warn("parse message properties minTs failed, unknown message", zap.Error(err))
		return nil, fmt.Errorf("parse minTs from msg properties failed")
	}

	val, ok := properties[common.MsgTypeKey]
	if !ok {
		return nil, fmt.Errorf("get msgType from msg properties failed")
	}
	msgType := commonpb.MsgType(commonpb.MsgType_value[val])

	result := &MarshaledMsg{
		msg:       msg,
		timestamp: timestamp,
		msgType:   msgType,
		vchannel:  vchannel,
	}

	replicateID, ok := properties[common.ReplicateIDTypeKey]
	if ok {
		result.replicateID = replicateID
	}
	return result, nil
}

// unmarshal common message to UnmarshalledMsg
func UnmarshalMsg(msg common.Message, unmarshalDispatcher UnmarshalDispatcher) (PackMsg, error) {
	tsMsg, err := GetTsMsgFromConsumerMsg(unmarshalDispatcher, msg)
	if err != nil {
		return nil, err
	}

	return &UnmarshalledMsg{
		msg: tsMsg,
	}, nil
}

// RepackFunc is a function type which used to repack message after hash by primary key
type RepackFunc func(msgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error)

// MsgStream is an interface that can be used to produce and consume message on message queue
type MsgStream interface {
	Close()

	AsProducer(ctx context.Context, channels []string)
	Produce(context.Context, *MsgPack) error
	SetRepackFunc(repackFunc RepackFunc)
	GetProduceChannels() []string
	Broadcast(context.Context, *MsgPack) (map[string][]MessageID, error)

	AsConsumer(ctx context.Context, channels []string, subName string, position common.SubscriptionInitialPosition) error
	Chan() <-chan *ConsumeMsgPack
	GetUnmarshalDispatcher() UnmarshalDispatcher
	// Seek consume message from the specified position
	// includeCurrentMsg indicates whether to consume the current message, and in the milvus system, it should be always false
	Seek(ctx context.Context, msgPositions []*MsgPosition, includeCurrentMsg bool) error

	GetLatestMsgID(channel string) (MessageID, error)
	CheckTopicValid(channel string) error

	ForceEnableProduce(can bool)
}

type ReplicateConfig struct {
	ReplicateID string
	CheckFunc   CheckReplicateMsgFunc
}

type CheckReplicateMsgFunc func(*ReplicateMsg) bool

func GetReplicateConfig(replicateID, dbName, colName string) *ReplicateConfig {
	if replicateID == "" {
		return nil
	}
	replicateConfig := &ReplicateConfig{
		ReplicateID: replicateID,
		CheckFunc: func(msg *ReplicateMsg) bool {
			if !msg.GetIsEnd() {
				return false
			}
			log.Info("check replicate msg",
				zap.String("replicateID", replicateID),
				zap.String("dbName", dbName),
				zap.String("colName", colName),
				zap.Any("msg", msg))
			if msg.GetIsCluster() {
				return true
			}
			return msg.GetDatabase() == dbName && (msg.GetCollection() == colName || msg.GetCollection() == "")
		},
	}
	return replicateConfig
}

func GetReplicateID(msg TsMsg) string {
	msgBase, ok := msg.(interface{ GetBase() *commonpb.MsgBase })
	if !ok {
		log.Warn("fail to get msg base, please check it", zap.Any("type", msg.Type()))
		return ""
	}
	return msgBase.GetBase().GetReplicateInfo().GetReplicateID()
}

func GetTimestamp(msg TsMsg) uint64 {
	msgBase, ok := msg.(interface{ GetBase() *commonpb.MsgBase })
	if !ok {
		log.Warn("fail to get msg base, please check it", zap.Any("type", msg.Type()))
		return 0
	}
	return msgBase.GetBase().GetTimestamp()
}

func MatchReplicateID(msg TsMsg, replicateID string) bool {
	return GetReplicateID(msg) == replicateID
}

type Factory interface {
	NewMsgStream(ctx context.Context) (MsgStream, error)
	NewTtMsgStream(ctx context.Context) (MsgStream, error)
	NewMsgStreamDisposer(ctx context.Context) func([]string, string) error
}

// Filter and parse ts message for temporary stream
type SimpleMsgDispatcher struct {
	stream              MsgStream
	unmarshalDispatcher UnmarshalDispatcher
	filter              func(PackMsg) bool
	ch                  chan *MsgPack
	chOnce              sync.Once

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func NewSimpleMsgDispatcher(stream MsgStream, filter func(PackMsg) bool) *SimpleMsgDispatcher {
	return &SimpleMsgDispatcher{
		stream:              stream,
		filter:              filter,
		unmarshalDispatcher: stream.GetUnmarshalDispatcher(),
		closeCh:             make(chan struct{}),
	}
}

func (p *SimpleMsgDispatcher) filterAndParase() {
	defer func() {
		close(p.ch)
		p.wg.Done()
	}()
	for {
		select {
		case <-p.closeCh:
			return
		case marshalPack, ok := <-p.stream.Chan():
			if !ok {
				log.Warn("dispatcher fail to read delta msg")
				return
			}

			msgPack := &MsgPack{
				BeginTs:        marshalPack.BeginTs,
				EndTs:          marshalPack.EndTs,
				Msgs:           make([]TsMsg, 0),
				StartPositions: marshalPack.StartPositions,
				EndPositions:   marshalPack.EndPositions,
			}
			for _, marshalMsg := range marshalPack.Msgs {
				if !p.filter(marshalMsg) {
					continue
				}
				// unmarshal message
				msg, err := marshalMsg.Unmarshal(p.unmarshalDispatcher)
				if err != nil {
					log.Warn("unmarshal message failed, invalid message", zap.Error(err))
					continue
				}
				msgPack.Msgs = append(msgPack.Msgs, msg)
			}
			p.ch <- msgPack
		}
	}
}

func (p *SimpleMsgDispatcher) Chan() chan *MsgPack {
	p.chOnce.Do(func() {
		p.ch = make(chan *MsgPack, paramtable.Get().MQCfg.ReceiveBufSize.GetAsInt64())
		p.wg.Add(1)
		go p.filterAndParase()
	})
	return p.ch
}

func (p *SimpleMsgDispatcher) Close() {
	p.closeOnce.Do(func() {
		p.stream.Close()
		close(p.closeCh)
		p.wg.Wait()
	})
}
