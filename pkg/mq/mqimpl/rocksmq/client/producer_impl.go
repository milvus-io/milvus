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

package client

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
)

// assertion make sure implementation
var _ Producer = (*producer)(nil)

// producer contains a client instance and topic name
type producer struct {
	// client which the producer belong to
	c     *client
	topic string
}

// newProducer creates a rocksmq producer from options
func newProducer(c *client, options ProducerOptions) (*producer, error) {
	if c == nil {
		return nil, newError(InvalidConfiguration, "client is nil")
	}

	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is empty")
	}

	return &producer{
		c:     c,
		topic: options.Topic,
	}, nil
}

// Topic returns the topic name of producer
func (p *producer) Topic() string {
	return p.topic
}

// Send produce message in rocksmq
func (p *producer) Send(message *common.ProducerMessage) (UniqueID, error) {
	// NOTICE: this is the hack.
	// we should not unmarshal the payload here but we can not extend the payload byte
	payload := message.Payload
	header, err := UnmarshalHeader(message.Payload)
	if err == nil && header != nil && header.Base != nil {
		// try to marshal properties into message if message is real message
		header.Base.Properties = message.Properties
		payload, err = MarshalHeader(header)
		if err != nil {
			return 0, err
		}
	}

	ids, err := p.c.server.Produce(p.topic, []server.ProducerMessage{
		{
			Payload: payload,
		},
	})
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

func (p *producer) SendForStreamingService(message *common.ProducerMessage) (UniqueID, error) {
	rmqMessage := &messagespb.RMQMessageLayout{
		Payload:    message.Payload,
		Properties: message.Properties,
	}
	payload, err := proto.Marshal(rmqMessage)
	if err != nil {
		return 0, err
	}
	finalPayload := make([]byte, len(payload)+len(magicPrefix))
	copy(finalPayload, magicPrefix)
	copy(finalPayload[len(magicPrefix):], payload)
	ids, err := p.c.server.Produce(p.topic, []server.ProducerMessage{{
		Payload: finalPayload,
	}})
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

// Close destroy the topic of this producer in rocksmq
func (p *producer) Close() {
	err := p.c.server.DestroyTopic(p.topic)
	if err != nil {
		log.Warn("Producer close failed", zap.String("topicName", p.topic), zap.Error(err))
	}
}
