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

import "context"

// ProducerOptions contains the options of a producer
type ProducerOptions struct {
	// The topic that this Producer will publish
	Topic string
}

// ProducerMessage contains the messages of a producer
type ProducerMessage struct {
	// Payload get the payload of the message
	Payload []byte
	// Properties are application defined key/value pairs that will be attached to the message.
	// Return the properties attached to the message.
	Properties map[string]string
}

// Producer is the interface that provides operations of producer
type Producer interface {
	// return the topic which producer is publishing to
	//Topic() string

	// publish a message
	Send(ctx context.Context, message *ProducerMessage) (MessageID, error)

	Close()
}
