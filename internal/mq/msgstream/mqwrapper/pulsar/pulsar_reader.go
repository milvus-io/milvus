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

package pulsar

import (
	"context"

	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"

	"github.com/apache/pulsar-client-go/pulsar"
)

// pulsarReader contains a pulsar reader
type pulsarReader struct {
	r pulsar.Reader
}

// Topic returns the topic of pulsar reader
func (pr *pulsarReader) Topic() string {
	return pr.r.Topic()
}

// Next read the next message in the topic, blocking until a message is available
func (pr *pulsarReader) Next(ctx context.Context) (mqwrapper.Message, error) {
	pm, err := pr.r.Next(ctx)
	if err != nil {
		return nil, err
	}

	return &pulsarMessage{msg: pm}, nil
}

// HasNext check if there is any message available to read from the current position
func (pr *pulsarReader) HasNext() bool {
	return pr.r.HasNext()
}

func (pr *pulsarReader) Close() {
	pr.r.Close()
}

func (pr *pulsarReader) Seek(id mqwrapper.MessageID) error {
	messageID := id.(*pulsarID).messageID
	err := pr.r.Seek(messageID)
	if err != nil {
		return err
	}
	return nil
}
