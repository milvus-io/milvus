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

package mqclient

import (
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/rocksmq/client/rocksmq"
)

type rmqClient struct {
	client rocksmq.Client
}

func NewRmqClient(opts rocksmq.ClientOptions) (*rmqClient, error) {
	c, err := rocksmq.NewClient(opts)
	if err != nil {
		log.Error("Set rmq client failed, error", zap.Error(err))
		return nil, err
	}
	return &rmqClient{client: c}, nil
}

func (rc *rmqClient) CreateProducer(options ProducerOptions) (Producer, error) {
	rmqOpts := rocksmq.ProducerOptions{Topic: options.Topic}
	pp, err := rc.client.CreateProducer(rmqOpts)
	if err != nil {
		return nil, err
	}
	rp := rmqProducer{p: pp}
	return &rp, nil
}

func (rc *rmqClient) Subscribe(options ConsumerOptions) (Consumer, error) {
	receiveChannel := make(chan rocksmq.ConsumerMessage, options.BufSize)

	cli, err := rc.client.Subscribe(rocksmq.ConsumerOptions{
		Topic:            options.Topic,
		SubscriptionName: options.SubscriptionName,
		MessageChannel:   receiveChannel,
	})
	if err != nil {
		return nil, err
	}

	rConsumer := &RmqConsumer{c: cli, closeCh: make(chan struct{})}

	return rConsumer, nil
}

func (rc *rmqClient) EarliestMessageID() MessageID {
	rID := rocksmq.EarliestMessageID()
	return &rmqID{messageID: rID}
}

func (rc *rmqClient) StringToMsgID(id string) (MessageID, error) {
	rID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, err
	}
	return &rmqID{messageID: rID}, nil
}

func (rc *rmqClient) BytesToMsgID(id []byte) (MessageID, error) {
	rID, err := DeserializeRmqID(id)
	if err != nil {
		return nil, err
	}
	return &rmqID{messageID: rID}, nil
}

func (rc *rmqClient) Close() {
	// TODO(yukun): What to do here?
	// rc.client.Close()
}
