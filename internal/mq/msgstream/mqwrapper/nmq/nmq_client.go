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

package nmq

import (
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"

	"github.com/milvus-io/milvus/internal/mq/mqimpl/natsmq/server"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

// nmqClient contains a natsmq client
type nmqClient struct {
	conn *nats.Conn
}

func NewClientWithDefaultOptions() (*nmqClient, error) {
	url := server.Nmq.ClientURL()
	return NewClient(url)
}

// NewClient returns a new nmqClient object
func NewClient(url string, options ...nats.Option) (*nmqClient, error) {
	c, err := nats.Connect(url, options...)
	if err != nil {
		return nil, util.WrapError("failed to set nmq client", err)
	}
	return &nmqClient{conn: c}, nil
}

// CreateProducer creates a producer for natsmq client
func (nc *nmqClient) CreateProducer(options mqwrapper.ProducerOptions) (mqwrapper.Producer, error) {
	// TODO: inject jetstream options.
	js, err := nc.conn.JetStream()
	if err != nil {
		return nil, util.WrapError("failed to create jetstream context", err)
	}
	// TODO: (1) investigate on performance of multiple streams vs multiple topics.
	//       (2) investigate if we should have topics under the same stream.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     options.Topic,
		Subjects: []string{options.Topic},
	})
	if err != nil {
		return nil, util.WrapError("failed to add/connect to jetstream for producer", err)
	}
	rp := nmqProducer{js: js, topic: options.Topic}
	return &rp, nil
}

func (nc *nmqClient) Subscribe(options mqwrapper.ConsumerOptions) (mqwrapper.Consumer, error) {
	if options.Topic == "" {
		return nil, fmt.Errorf("invalid consumer config: empty topic")
	}

	if options.SubscriptionName == "" {
		return nil, fmt.Errorf("invalid consumer config: empty subscription name")
	}
	// TODO: inject jetstream options.
	js, err := nc.conn.JetStream()
	if err != nil {
		return nil, util.WrapError("failed to create jetstream context", err)
	}
	// TODO: do we allow passing in an existing natsChan from options?
	// also, revisit the size or make it a user param
	natsChan := make(chan *nats.Msg, 8192)
	// TODO: should we allow subscribe to a topic that doesn't exist yet? Current logic allows it.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     options.Topic,
		Subjects: []string{options.Topic},
	})
	if err != nil {
		return nil, util.WrapError("failed to add/connect to jetstream for consumer", err)
	}
	closeChan := make(chan struct{})

	var sub *nats.Subscription
	position := options.SubscriptionInitialPosition
	if position != mqwrapper.SubscriptionPositionUnknown {
		// TODO: should we only allow exclusive subscribe? Current logic allows double subscribe.
		sub, err = js.ChanSubscribe(options.Topic, natsChan)
		if err != nil {
			return nil, util.WrapError("failed to subscribe", err)
		}
		if position == mqwrapper.SubscriptionPositionLatest {
			cinfo, err := sub.ConsumerInfo()
			if err != nil {
				return nil, util.WrapError("failed to get consumer info", err)
			}
			msgID := cinfo.Delivered.Stream
			natsChan = make(chan *nats.Msg, 8192)
			sub.Unsubscribe()
			sub, err = js.ChanSubscribe(options.Topic, natsChan, nats.StartSequence(msgID))
			if err != nil {
				return nil, util.WrapError("failed to subscribe from latest message", err)
			}
		}
	}

	return &Consumer{
		js:        js,
		sub:       sub,
		topic:     options.Topic,
		groupName: options.SubscriptionName,
		options:   options,
		natsChan:  natsChan,
		closeChan: closeChan,
	}, nil
}

// EarliestMessageID returns the earliest message ID for nmq client
func (nc *nmqClient) EarliestMessageID() mqwrapper.MessageID {
	return &nmqID{messageID: 1}
}

// StringToMsgID converts string id to MessageID
func (nc *nmqClient) StringToMsgID(id string) (mqwrapper.MessageID, error) {
	rID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return nil, util.WrapError("failed to parse string to MessageID", err)
	}
	return &nmqID{messageID: rID}, nil
}

// BytesToMsgID converts a byte array to messageID
func (nc *nmqClient) BytesToMsgID(id []byte) (mqwrapper.MessageID, error) {
	rID := DeserializeNmqID(id)
	return &nmqID{messageID: rID}, nil
}

func (nc *nmqClient) Close() {
	nc.conn.Close()
}
