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
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/streamnative/pulsarctl/pkg/cli"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/mqimpl/rocksmq/server"
	kafkawrapper "github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper/kafka"
	pulsarmqwrapper "github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper/pulsar"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

// PmsFactory is a pulsar msgstream factory that implemented Factory interface(msgstream.go)
type PmsFactory struct {
	dispatcherFactory ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	PulsarAddress    string
	PulsarWebAddress string
	ReceiveBufSize   int64
	MQBufSize        int64
	PulsarAuthPlugin string
	PulsarAuthParams string
	PulsarTenant     string
	PulsarNameSpace  string
	RequestTimeout   time.Duration
	metricRegisterer prometheus.Registerer
}

func NewPmsFactory(serviceParam *paramtable.ServiceParam) *PmsFactory {
	config := &serviceParam.PulsarCfg
	f := &PmsFactory{
		MQBufSize:        serviceParam.MQCfg.MQBufSize.GetAsInt64(),
		ReceiveBufSize:   serviceParam.MQCfg.ReceiveBufSize.GetAsInt64(),
		PulsarAddress:    config.Address.GetValue(),
		PulsarWebAddress: config.WebAddress.GetValue(),
		PulsarAuthPlugin: config.AuthPlugin.GetValue(),
		PulsarAuthParams: config.AuthParams.GetValue(),
		PulsarTenant:     config.Tenant.GetValue(),
		PulsarNameSpace:  config.Namespace.GetValue(),
		RequestTimeout:   config.RequestTimeout.GetAsDuration(time.Second),
	}
	if config.EnableClientMetrics.GetAsBool() {
		// Enable client metrics if config.EnableClientMetrics is true, use pkg-defined registerer.
		f.metricRegisterer = metrics.GetRegisterer()
	}
	return f
}

// NewMsgStream is used to generate a new Msgstream object
func (f *PmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	var timeout time.Duration = f.RequestTimeout

	if deadline, ok := ctx.Deadline(); ok {
		if deadline.Before(time.Now()) {
			return nil, errors.New("context timeout when NewMsgStream")
		}
		timeout = time.Until(deadline)
	}

	auth, err := f.getAuthentication()
	if err != nil {
		return nil, err
	}
	clientOpts := pulsar.ClientOptions{
		URL:               f.PulsarAddress,
		Authentication:    auth,
		OperationTimeout:  timeout,
		MetricsRegisterer: f.metricRegisterer,
	}

	pulsarClient, err := pulsarmqwrapper.NewClient(f.PulsarTenant, f.PulsarNameSpace, clientOpts)
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(context.Background(), f.ReceiveBufSize, f.MQBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewTtMsgStream is used to generate a new TtMsgstream object
func (f *PmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	var timeout time.Duration = f.RequestTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if deadline.Before(time.Now()) {
			return nil, errors.New("context timeout when NewTtMsgStream")
		}
		timeout = time.Until(deadline)
	}
	auth, err := f.getAuthentication()
	if err != nil {
		return nil, err
	}
	clientOpts := pulsar.ClientOptions{
		URL:               f.PulsarAddress,
		Authentication:    auth,
		OperationTimeout:  timeout,
		MetricsRegisterer: f.metricRegisterer,
	}

	pulsarClient, err := pulsarmqwrapper.NewClient(f.PulsarTenant, f.PulsarNameSpace, clientOpts)
	if err != nil {
		return nil, err
	}

	return NewMqTtMsgStream(context.Background(), f.ReceiveBufSize, f.MQBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
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
					zap.String("topic", channel), zap.String("subname", subname), zap.Error(err))
			}
		}
		return nil
	}
}

type KmsFactory struct {
	dispatcherFactory ProtoUDFactory
	config            *paramtable.KafkaConfig
	ReceiveBufSize    int64
	MQBufSize         int64
}

func (f *KmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	kafkaClient, err := kafkawrapper.NewKafkaClientInstanceWithConfig(ctx, f.config)
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(context.Background(), f.ReceiveBufSize, f.MQBufSize, kafkaClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *KmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	kafkaClient, err := kafkawrapper.NewKafkaClientInstanceWithConfig(ctx, f.config)
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(context.Background(), f.ReceiveBufSize, f.MQBufSize, kafkaClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *KmsFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return func(channels []string, subname string) error {
		msgstream, err := f.NewMsgStream(ctx)
		if err != nil {
			return err
		}
		msgstream.AsConsumer(ctx, channels, subname, common.SubscriptionPositionUnknown)
		msgstream.Close()
		return nil
	}
}

func NewKmsFactory(config *paramtable.ServiceParam) Factory {
	f := &KmsFactory{
		dispatcherFactory: ProtoUDFactory{},
		ReceiveBufSize:    config.MQCfg.ReceiveBufSize.GetAsInt64(),
		MQBufSize:         config.MQCfg.MQBufSize.GetAsInt64(),
		config:            &config.KafkaCfg,
	}
	return f
}

// NewRocksmqFactory creates a new message stream factory based on rocksmq.
func NewRocksmqFactory(path string, cfg *paramtable.ServiceParam) Factory {
	if err := server.InitRocksMQ(path); err != nil {
		log.Fatal("fail to init rocksmq", zap.Error(err))
	}
	log.Info("init rocksmq msgstream success", zap.String("path", path))

	return &CommonFactory{
		Newer:             rmq.NewClientWithDefaultOptions,
		DispatcherFactory: ProtoUDFactory{},
		ReceiveBufSize:    cfg.MQCfg.ReceiveBufSize.GetAsInt64(),
		MQBufSize:         cfg.MQCfg.MQBufSize.GetAsInt64(),
	}
}
