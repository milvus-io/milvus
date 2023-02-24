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
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
	rmqimplserver "github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	kafkawrapper "github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/kafka"
	pulsarmqwrapper "github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/pulsar"
	rmqwrapper "github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/rmq"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/streamnative/pulsarctl/pkg/cli"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
	"go.uber.org/zap"
)

// PmsFactory is a pulsar msgstream factory that implemented Factory interface(msgstream.go)
type PmsFactory struct {
	dispatcherFactory ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	PulsarAddress    string
	PulsarWebAddress string
	ReceiveBufSize   int64
	PulsarBufSize    int64
	PulsarAuthPlugin string
	PulsarAuthParams string
	PulsarTenant     string
	PulsarNameSpace  string
}

func NewPmsFactory(config *paramtable.PulsarConfig) *PmsFactory {
	return &PmsFactory{
		PulsarBufSize:    1024,
		ReceiveBufSize:   1024,
		PulsarAddress:    config.Address.GetValue(),
		PulsarWebAddress: config.WebAddress.GetValue(),
		PulsarAuthPlugin: config.AuthPlugin.GetValue(),
		PulsarAuthParams: config.AuthParams.GetValue(),
		PulsarTenant:     config.Tenant.GetValue(),
		PulsarNameSpace:  config.Namespace.GetValue(),
	}
}

// NewMsgStream is used to generate a new Msgstream object
func (f *PmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	auth, err := f.getAuthentication()
	if err != nil {
		return nil, err
	}
	clientOpts := pulsar.ClientOptions{
		URL:            f.PulsarAddress,
		Authentication: auth,
	}

	pulsarClient, err := pulsarmqwrapper.NewClient(f.PulsarTenant, f.PulsarNameSpace, clientOpts)
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.PulsarBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewTtMsgStream is used to generate a new TtMsgstream object
func (f *PmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	auth, err := f.getAuthentication()
	if err != nil {
		return nil, err
	}
	clientOpts := pulsar.ClientOptions{
		URL:            f.PulsarAddress,
		Authentication: auth,
	}

	pulsarClient, err := pulsarmqwrapper.NewClient(f.PulsarTenant, f.PulsarNameSpace, clientOpts)
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, f.PulsarBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *PmsFactory) getAuthentication() (pulsar.Authentication, error) {
	auth, err := pulsar.NewAuthentication(f.PulsarAuthPlugin, f.PulsarAuthParams)

	if err != nil {
		log.Error("build authencation from config failed, please check it!",
			zap.String("authPlugin", f.PulsarAuthPlugin),
			zap.Error(err))
		return nil, errors.New("build authencation from config failed")
	}
	return auth, nil
}

// NewQueryMsgStream is used to generate a new QueryMsgstream object
func (f *PmsFactory) NewQueryMsgStream(ctx context.Context) (MsgStream, error) {
	return f.NewMsgStream(ctx)
}

func (f *PmsFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return func(channels []string, subname string) error {
		// try to delete the old subscription
		admin, err := pulsarmqwrapper.NewAdminClient(f.PulsarWebAddress, f.PulsarAuthPlugin, f.PulsarAuthParams)
		if err != nil {
			return err
		}
		for _, channel := range channels {
			fullTopicName, err := pulsarmqwrapper.GetFullTopicName(f.PulsarTenant, f.PulsarNameSpace, channel)
			if err != nil {
				return err
			}
			topic, err := utils.GetTopicName(fullTopicName)
			if err != nil {
				log.Warn("failed to get topic name", zap.Error(err))
				return retry.Unrecoverable(err)
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
				log.Warn("failed to clean up subscriptions", zap.String("pulsar web", f.PulsarWebAddress),
					zap.String("topic", channel), zap.Any("subname", subname), zap.Error(err))
			}
		}
		return nil
	}
}

// RmsFactory is a rocksmq msgstream factory that implemented Factory interface(msgstream.go)
type RmsFactory struct {
	dispatcherFactory ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	ReceiveBufSize int64
	RmqBufSize     int64
}

// NewMsgStream is used to generate a new Msgstream object
func (f *RmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	rmqClient, err := rmqwrapper.NewClientWithDefaultOptions()
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewTtMsgStream is used to generate a new TtMsgstream object
func (f *RmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	rmqClient, err := rmqwrapper.NewClientWithDefaultOptions()
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewQueryMsgStream is used to generate a new QueryMsgstream object
func (f *RmsFactory) NewQueryMsgStream(ctx context.Context) (MsgStream, error) {
	rmqClient, err := rmqwrapper.NewClientWithDefaultOptions()
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *RmsFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return func(channels []string, subname string) error {
		msgstream, err := f.NewMsgStream(ctx)
		if err != nil {
			return err
		}
		msgstream.AsConsumer(channels, subname, mqwrapper.SubscriptionPositionUnknown)
		msgstream.Close()
		return nil
	}
}

// NewRmsFactory is used to generate a new RmsFactory object
func NewRmsFactory(path string) *RmsFactory {
	f := &RmsFactory{
		dispatcherFactory: ProtoUDFactory{},
		ReceiveBufSize:    1024,
		RmqBufSize:        1024,
	}

	err := rmqimplserver.InitRocksMQ(path)
	if err != nil {
		log.Error("init rmq error", zap.Error(err))
	}
	return f
}

type KmsFactory struct {
	dispatcherFactory ProtoUDFactory
	config            *paramtable.KafkaConfig
	ReceiveBufSize    int64
}

func (f *KmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	kafkaClient := kafkawrapper.NewKafkaClientInstanceWithConfig(f.config)
	return NewMqMsgStream(ctx, f.ReceiveBufSize, -1, kafkaClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *KmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	kafkaClient := kafkawrapper.NewKafkaClientInstanceWithConfig(f.config)
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, -1, kafkaClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *KmsFactory) NewQueryMsgStream(ctx context.Context) (MsgStream, error) {
	return f.NewMsgStream(ctx)
}

func (f *KmsFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return func(channels []string, subname string) error {
		msgstream, err := f.NewMsgStream(ctx)
		if err != nil {
			return err
		}
		msgstream.AsConsumer(channels, subname, mqwrapper.SubscriptionPositionUnknown)
		msgstream.Close()
		return nil
	}
}

func NewKmsFactory(config *paramtable.KafkaConfig) Factory {
	f := &KmsFactory{
		dispatcherFactory: ProtoUDFactory{},
		ReceiveBufSize:    1024,
		config:            config,
	}
	return f
}
