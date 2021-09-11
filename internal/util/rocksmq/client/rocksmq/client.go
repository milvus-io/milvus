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

package rocksmq

import (
	server "github.com/milvus-io/milvus/internal/util/rocksmq/server/rocksmq"
)

type RocksMQ = server.RocksMQ

func NewClient(options ClientOptions) (Client, error) {
	if options.Server == nil {
		options.Server = server.Rmq
	}
	return newClient(options)
}

type ClientOptions struct {
	Server RocksMQ
}

type Client interface {
	// Create a producer instance
	CreateProducer(options ProducerOptions) (Producer, error)

	// Create a consumer instance and subscribe a topic
	Subscribe(options ConsumerOptions) (Consumer, error)

	// Close the client and free associated resources
	Close()
}
