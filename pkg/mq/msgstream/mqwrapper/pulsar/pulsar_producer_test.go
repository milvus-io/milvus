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
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mq/common"
)

func TestPulsarProducer(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)

	topic := "TEST"
	producer, err := pc.CreateProducer(context.TODO(), common.ProducerOptions{Topic: topic})
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	pulsarProd := producer.(*pulsarProducer)
	fullTopicName, err := GetFullTopicName(DefaultPulsarTenant, DefaultPulsarNamespace, topic)
	assert.NoError(t, err)
	assert.Equal(t, pulsarProd.Topic(), fullTopicName)

	msg := &common.ProducerMessage{
		Payload:    []byte{},
		Properties: map[string]string{},
	}
	_, err = producer.Send(context.TODO(), msg)
	assert.NoError(t, err)

	pulsarProd.Close()
}
