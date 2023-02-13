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

package mq

import (
	"context"
	"errors"
	"strings"

	"github.com/streamnative/pulsarctl/pkg/cli"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"

	pulsar_client "github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/cdc/core/config"
	"github.com/milvus-io/milvus/cdc/core/mq/api"
	"github.com/milvus-io/milvus/cdc/core/mq/kafka"
	"github.com/milvus-io/milvus/cdc/core/mq/pulsar"
	"github.com/milvus-io/milvus/cdc/core/util"
	"go.uber.org/zap"
)

type PmsFactory struct {
	util.CDCMark
	dispatcherFactory *api.ProtoUDFactory
	PulsarConfig      *config.PulsarConfig
	ReceiveBufSize    int64
	PulsarBufSize     int64
}

func NewPmsFactory(cfg *config.PulsarConfig) *PmsFactory {
	return &PmsFactory{
		dispatcherFactory: &api.ProtoUDFactory{},
		PulsarBufSize:     1024,
		ReceiveBufSize:    1024,
		PulsarConfig:      cfg,
	}
}

func (f *PmsFactory) NewMsgStream(ctx context.Context) (api.MsgStream, error) {
	auth, err := f.getAuthentication()
	if err != nil {
		return nil, err
	}
	clientOpts := pulsar_client.ClientOptions{
		URL:            f.PulsarConfig.Address,
		Authentication: auth,
	}

	pulsarClient, err := pulsar.NewClient(f.PulsarConfig.Tenant, f.PulsarConfig.Namespace, clientOpts)
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, f.PulsarBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *PmsFactory) getAuthentication() (pulsar_client.Authentication, error) {
	auth, err := pulsar_client.NewAuthentication(f.PulsarConfig.AuthPlugin, f.PulsarConfig.AuthParams)

	if err != nil {
		util.Log.Error("build authencation from config failed, please check it!",
			zap.String("authPlugin", f.PulsarConfig.AuthPlugin),
			zap.Error(err))
		return nil, errors.New("build authencation from config failed")
	}
	return auth, nil
}

func (f *PmsFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return func(channels []string, subname string) error {
		// try to delete the old subscription
		admin, err := pulsar.NewAdminClient(f.PulsarConfig.WebAddress, f.PulsarConfig.AuthPlugin, f.PulsarConfig.AuthParams)
		if err != nil {
			return err
		}
		for _, channel := range channels {
			fullTopicName, err := pulsar.GetFullTopicName(f.PulsarConfig.Tenant, f.PulsarConfig.Namespace, channel)
			if err != nil {
				return err
			}
			topic, err := utils.GetTopicName(fullTopicName)
			if err != nil {
				util.Log.Warn("failed to get topic name", zap.Error(err))
				return util.Unrecoverable(err)
			}
			err = admin.Subscriptions().Delete(*topic, subname, true)
			if err != nil {
				pulsarErr, ok := err.(cli.Error)
				if ok {
					// subscription not found, ignore error
					if strings.Contains(pulsarErr.Reason, "Subscription not found") {
						return nil
					}
				}
				util.Log.Warn("failed to clean up subscriptions", zap.String("pulsar web", f.PulsarConfig.WebAddress),
					zap.String("topic", channel), zap.Any("subname", subname), zap.Error(err))
			}
		}
		return nil
	}
}

type KmsFactory struct {
	util.CDCMark
	dispatcherFactory *api.ProtoUDFactory
	config            *config.KafkaConfig
	ReceiveBufSize    int64
}

func NewKmsFactory(config *config.KafkaConfig) api.Factory {
	f := &KmsFactory{
		dispatcherFactory: &api.ProtoUDFactory{},
		ReceiveBufSize:    1024,
		config:            config,
	}
	return f
}

func (f *KmsFactory) NewMsgStream(ctx context.Context) (api.MsgStream, error) {
	kafkaClient := kafka.NewKafkaClientInstanceWithConfig(f.config)
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, -1, kafkaClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *KmsFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return func(channels []string, subname string) error {
		msgstream, err := f.NewMsgStream(ctx)
		if err != nil {
			return err
		}
		msgstream.AsConsumer(channels, subname, api.SubscriptionPositionUnknown)
		msgstream.Close()
		return nil
	}
}

func NewFactory(mqType string) (api.Factory, error) {
	switch mqType {
	case "kafka":
		if !config.KafkaEnable() {
			util.Log.Panic("kafka not configured")
		}
		return NewKmsFactory(config.Kafka), nil
	case "pulsar":
		if !config.PulsarEnable() {
			util.Log.Panic("pulsar not configured")
		}
		return NewPmsFactory(config.Pulsar), nil
	default:
		util.Log.Panic("unknown mq type:" + mqType)
	}
	return nil, nil
}
