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
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	pcommon "github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/common"
	kafkamqwrapper "github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/kafka"
	pulsarmqwrapper "github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/pulsar"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	err = dmlStream.AsConsumer(ctx, []string{channelName}, subName, common.SubscriptionPositionUnknown)
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

// PulsarHealthCheck Perform a health check by retrieving cluster metadata
func PulsarHealthCheck(clusterStatus *pcommon.MQClusterStatus) {
	pulsarCfg := &paramtable.Get().PulsarCfg
	admin, err := pulsarmqwrapper.NewAdminClient(pulsarCfg.WebAddress.GetValue(), pulsarCfg.AuthPlugin.GetValue(), pulsarCfg.AuthParams.GetValue())
	if err != nil {
		clusterStatus.Reason = fmt.Sprintf("establish client connection failed, err: %v", err)
		return
	}

	err = admin.Brokers().HealthCheck()
	if err != nil {
		clusterStatus.Reason = fmt.Sprintf("health check failed, err: %v", err)
		return
	}

	clusters, err := admin.Clusters().List()
	if err != nil {
		clusterStatus.Reason = fmt.Sprintf("failed to list Pulsar cluster, err: %v", err)
		return
	}

	if len(clusters) == 0 {
		clusterStatus.Reason = "not found Pulsar available cluster"
		return
	}

	brokers, err := admin.Brokers().GetActiveBrokers(clusters[0])
	if err != nil {
		clusterStatus.Reason = fmt.Sprintf("failed to list Pulsar brokers, err: %v", err)
		return
	}

	var healthList []pcommon.EPHealth
	for _, b := range brokers {
		healthList = append(healthList, pcommon.EPHealth{EP: b, Health: true})
	}

	clusterStatus.Health = true
	clusterStatus.Members = healthList
}

// KafkaHealthCheck Perform a health check by retrieving cluster metadata
func KafkaHealthCheck(clusterStatus *pcommon.MQClusterStatus) {
	config := kafkamqwrapper.GetBasicConfig(&paramtable.Get().KafkaCfg)
	// Set extra config for producer
	pConfig := (&paramtable.Get().KafkaCfg).ProducerExtraConfig.GetValue()
	for k, v := range pConfig {
		config.SetKey(k, v)
	}
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		clusterStatus.Reason = fmt.Sprintf("failed to create Kafka producer: %v", err)
		return
	}
	defer producer.Close()

	metadata, err := producer.GetMetadata(nil, false, 3000)
	if err != nil {
		clusterStatus.Reason = fmt.Sprintf("failed to retrieve Kafka metadata: %v", err)
		return
	}

	// Check if brokers are available
	if len(metadata.Brokers) == 0 {
		clusterStatus.Reason = "no Kafka brokers found"
		return
	}

	var healthList []pcommon.EPHealth
	for _, broker := range metadata.Brokers {
		healthList = append(healthList, pcommon.EPHealth{EP: broker.Host, Health: true})
	}

	clusterStatus.Health = true
	clusterStatus.Members = healthList
}

func GetPorperties(msg TsMsg) map[string]string {
	properties := map[string]string{}

	properties[common.ChannelTypeKey] = msg.Position().GetChannelName()
	properties[common.MsgTypeKey] = msg.Type().String()
	msgBase, ok := msg.(interface{ GetBase() *commonpb.MsgBase })
	if ok {
		properties[common.TimestampTypeKey] = strconv.FormatUint(msgBase.GetBase().GetTimestamp(), 10)
		properties[common.ReplicateIDTypeKey] = msgBase.GetBase().GetReplicateInfo().GetReplicateID()
	}

	return properties
}

func BuildConsumeMsgPack(pack *MsgPack) *ConsumeMsgPack {
	return &ConsumeMsgPack{
		BeginTs: pack.BeginTs,
		EndTs:   pack.EndTs,
		Msgs: lo.Map(pack.Msgs, func(msg TsMsg, _ int) PackMsg {
			return &UnmarshalledMsg{msg: msg}
		}),
		StartPositions: pack.StartPositions,
		EndPositions:   pack.EndPositions,
	}
}
