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

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

type Factory interface {
	msgstream.Factory
	Init(p *paramtable.ComponentParam)
}

type DefaultFactory struct {
	standAlone       bool
	msgStreamFactory msgstream.Factory
}

func NewDefaultFactory(standAlone bool) *DefaultFactory {
	return &DefaultFactory{
		standAlone:       standAlone,
		msgStreamFactory: msgstream.NewRmsFactory("/tmp/milvus/rocksmq/"),
	}
}

func NewFactory(standAlone bool) *DefaultFactory {
	return &DefaultFactory{standAlone: standAlone}
}

// Init create a msg factory(TODO only support one mq at the same time.)
// In order to guarantee backward compatibility of config file, we still support multiple mq configs.
// 1. Rocksmq only run on local mode, and it has the highest priority
// 2. Pulsar has higher priority than Kafka within remote msg
func (f *DefaultFactory) Init(params *paramtable.ComponentParam) {
	// skip if using default factory
	if f.msgStreamFactory != nil {
		return
	}

	// init mq storage
	if f.standAlone {
		f.msgStreamFactory = f.initMQLocalService(params)
		if f.msgStreamFactory == nil {
			f.msgStreamFactory = f.initMQRemoteService(params)
			if f.msgStreamFactory == nil {
				panic("no available mq configuration, must config rocksmq, Pulsar or Kafka at least one of these!")
			}
		}
		return
	}

	f.msgStreamFactory = f.initMQRemoteService(params)
	if f.msgStreamFactory == nil {
		panic("no available remote mq configuration, must config Pulsar or Kafka at least one of these!")
	}
}

func (f *DefaultFactory) initMQLocalService(params *paramtable.ComponentParam) msgstream.Factory {
	if params.RocksmqEnable() {
		path, _ := params.Load("_RocksmqPath")
		return msgstream.NewRmsFactory(path)
	}
	return nil
}

// initRemoteService Pulsar has higher priority than Kafka.
func (f *DefaultFactory) initMQRemoteService(params *paramtable.ComponentParam) msgstream.Factory {
	if params.PulsarEnable() {
		return msgstream.NewPmsFactory(&params.PulsarCfg)
	}

	if params.KafkaEnable() {
		return msgstream.NewKmsFactory(&params.KafkaCfg)
	}

	return nil
}

func (f *DefaultFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return f.msgStreamFactory.NewMsgStream(ctx)
}

func (f *DefaultFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return f.msgStreamFactory.NewTtMsgStream(ctx)
}

func (f *DefaultFactory) NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return f.msgStreamFactory.NewQueryMsgStream(ctx)
}

func (f *DefaultFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return f.msgStreamFactory.NewMsgStreamDisposer(ctx)
}
