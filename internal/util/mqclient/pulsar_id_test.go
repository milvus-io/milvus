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
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

func TestPulsarID_Serialize(t *testing.T) {
	mid := pulsar.EarliestMessageID()
	pid := &pulsarID{
		messageID: mid,
	}

	binary := pid.Serialize()
	assert.NotNil(t, binary)
	assert.NotZero(t, len(binary))
}

func Test_SerializePulsarMsgID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	binary := SerializePulsarMsgID(mid)
	assert.NotNil(t, binary)
	assert.NotZero(t, len(binary))
}

func Test_DeserializePulsarMsgID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	binary := SerializePulsarMsgID(mid)
	res, err := DeserializePulsarMsgID(binary)
	assert.Nil(t, err)
	assert.NotNil(t, res)
}

func Test_PulsarMsgIDToString(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	str := PulsarMsgIDToString(mid)
	assert.NotNil(t, str)
	assert.NotZero(t, len(str))
}

func Test_StringToPulsarMsgID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	str := PulsarMsgIDToString(mid)
	res, err := StringToPulsarMsgID(str)
	assert.Nil(t, err)
	assert.NotNil(t, res)
}
