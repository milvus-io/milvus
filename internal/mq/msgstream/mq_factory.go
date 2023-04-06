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

	rmqimplserver "github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	rmqwrapper "github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/rmq"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

// RmsFactory is a rocksmq msgstream factory that implemented Factory interface(msgstream.go)
type RmsFactory struct {
	dispatcherFactory msgstream.ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	ReceiveBufSize int64
	RmqBufSize     int64
}

// NewMsgStream is used to generate a new Msgstream object
func (f *RmsFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	rmqClient, err := rmqwrapper.NewClientWithDefaultOptions()
	if err != nil {
		return nil, err
	}
	return msgstream.NewMqMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewTtMsgStream is used to generate a new TtMsgstream object
func (f *RmsFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	rmqClient, err := rmqwrapper.NewClientWithDefaultOptions()
	if err != nil {
		return nil, err
	}
	return msgstream.NewMqTtMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

// NewQueryMsgStream is used to generate a new QueryMsgstream object
func (f *RmsFactory) NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	rmqClient, err := rmqwrapper.NewClientWithDefaultOptions()
	if err != nil {
		return nil, err
	}
	return msgstream.NewMqMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
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
		dispatcherFactory: msgstream.ProtoUDFactory{},
		ReceiveBufSize:    1024,
		RmqBufSize:        1024,
	}

	err := rmqimplserver.InitRocksMQ(path)
	if err != nil {
		log.Error("init rmq error", zap.Error(err))
	}
	return f
}
