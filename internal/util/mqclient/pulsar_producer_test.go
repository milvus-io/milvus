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
	"context"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

func TestPulsarProducer(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)

	topic := "TEST"
	producer, err := pc.CreateProducer(ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	pulsarProd := producer.(*pulsarProducer)
	assert.Equal(t, pulsarProd.Topic(), topic)

	msg := &ProducerMessage{
		Payload:    []byte{},
		Properties: map[string]string{},
	}
	_, err = producer.Send(context.TODO(), msg)
	assert.Nil(t, err)

	pulsarProd.Close()
}
