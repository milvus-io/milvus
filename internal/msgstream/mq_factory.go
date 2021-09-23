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

package msgstream

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/mitchellh/mapstructure"

	"github.com/milvus-io/milvus/internal/util/mqclient"
	"github.com/milvus-io/milvus/internal/util/rocksmq/client/rocksmq"
	rocksmqserver "github.com/milvus-io/milvus/internal/util/rocksmq/server/rocksmq"
)

// PmsFactory is a pulsar msgstream factory that implemented Factory interface(msgstream.go)
type PmsFactory struct {
	dispatcherFactory ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	PulsarAddress  string
	ReceiveBufSize int64
	PulsarBufSize  int64
}

// SetParams is used to set parameters for PmsFactory
func (f *PmsFactory) SetParams(params map[string]interface{}) error {
	err := mapstructure.Decode(params, f)
	if err != nil {
		return err
	}
	return nil
}

// NewMsgStream is used to generate a new Msgstream object
func (f *PmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	pulsarClient, err := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: f.PulsarAddress})
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.PulsarBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewTtMsgStream is used to generate a new TtMsgstream object
func (f *PmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	pulsarClient, err := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: f.PulsarAddress})
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, f.PulsarBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewQueryMsgStream is used to generate a new QueryMsgstream object
func (f *PmsFactory) NewQueryMsgStream(ctx context.Context) (MsgStream, error) {
	return f.NewMsgStream(ctx)
}

// NewPmsFactory is used to generate a new PmsFactory object
func NewPmsFactory() Factory {
	f := &PmsFactory{
		dispatcherFactory: ProtoUDFactory{},
		ReceiveBufSize:    64,
		PulsarBufSize:     64,
	}
	return f
}

// RmsFactory is a rocksmq msgstream factory that implemented Factory interface(msgstream.go)
type RmsFactory struct {
	dispatcherFactory ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	ReceiveBufSize int64
	RmqBufSize     int64
}

// SetParams is used to set parameters for RmsFactory
func (f *RmsFactory) SetParams(params map[string]interface{}) error {
	err := mapstructure.Decode(params, f)
	if err != nil {
		return err
	}
	return nil
}

// NewMsgStream is used to generate a new Msgstream object
func (f *RmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	rmqClient, err := mqclient.NewRmqClient(rocksmq.ClientOptions{Server: rocksmqserver.Rmq})
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewTtMsgStream is used to generate a new TtMsgstream object
func (f *RmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	rmqClient, err := mqclient.NewRmqClient(rocksmq.ClientOptions{Server: rocksmqserver.Rmq})
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewQueryMsgStream is used to generate a new QueryMsgstream object
func (f *RmsFactory) NewQueryMsgStream(ctx context.Context) (MsgStream, error) {
	rmqClient, err := mqclient.NewRmqClient(rocksmq.ClientOptions{Server: rocksmqserver.Rmq})
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewRmsFactory is used to generate a new RmsFactory object
func NewRmsFactory() Factory {
	f := &RmsFactory{
		dispatcherFactory: ProtoUDFactory{},
		ReceiveBufSize:    1024,
		RmqBufSize:        1024,
	}

	rocksmqserver.InitRocksMQ()
	return f
}
