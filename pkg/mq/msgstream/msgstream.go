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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/common"
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
	Chan() <-chan *MsgPack
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

func MatchReplicateID(msg TsMsg, replicateID string) bool {
	return GetReplicateID(msg) == replicateID
}

type Factory interface {
	NewMsgStream(ctx context.Context) (MsgStream, error)
	NewTtMsgStream(ctx context.Context) (MsgStream, error)
	NewMsgStreamDisposer(ctx context.Context) func([]string, string) error
}
