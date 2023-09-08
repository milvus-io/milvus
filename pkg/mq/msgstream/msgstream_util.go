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
	"math/rand"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

// unsubscribeChannels create consumer first, and unsubscribe channel through msgStream.close()
// TODO use streamnative pulsarctl
func UnsubscribeChannels(ctx context.Context, factory Factory, subName string, channels []string) {
	log.Info("unsubscribe channel", zap.String("subname", subName), zap.Any("channels", channels))
	err := factory.NewMsgStreamDisposer(ctx)(channels, subName)
	if err != nil {
		log.Warn("failed to unsubscribe channels", zap.String("subname", subName), zap.Any("channels", channels), zap.Error(err))
		panic(err)
	}
}

func GetChannelLatestMsgID(ctx context.Context, factory Factory, channelName string) ([]byte, error) {
	dmlStream, err := factory.NewMsgStream(ctx)
	if err != nil {
		log.Warn("fail to NewMsgStream", zap.String("channelName", channelName), zap.Error(err))
		return nil, err
	}
	defer dmlStream.Close()

	subName := fmt.Sprintf("get-latest_msg_id-%s-%d", channelName, rand.Int())
	err = dmlStream.AsConsumer(ctx, []string{channelName}, subName, mqwrapper.SubscriptionPositionUnknown)
	if err != nil {
		log.Warn("fail to AsConsumer", zap.String("channelName", channelName), zap.Error(err))
		return nil, err
	}
	id, err := dmlStream.GetLatestMsgID(channelName)
	if err != nil {
		log.Error("fail to GetLatestMsgID", zap.String("channelName", channelName), zap.Error(err))
		return nil, err
	}
	return id.Serialize(), nil
}
