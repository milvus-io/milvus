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

func Test_AtEarliestPosition(t *testing.T) {
	mid := pulsar.EarliestMessageID()
	pid := &pulsarID{
		messageID: mid,
	}
	assert.True(t, pid.AtEarliestPosition())

	mid = pulsar.LatestMessageID()
	pid = &pulsarID{
		messageID: mid,
	}
	assert.False(t, pid.AtEarliestPosition())
}

func TestLessOrEqualThan(t *testing.T) {
	msg1 := pulsar.EarliestMessageID()
	pid1 := &pulsarID{
		messageID: msg1,
	}

	msg2 := pulsar.LatestMessageID()
	pid2 := &pulsarID{
		messageID: msg2,
	}

	ret, err := pid1.LessOrEqualThan(pid2.Serialize())
	assert.Nil(t, err)
	assert.True(t, ret)

	ret, err = pid2.LessOrEqualThan(pid1.Serialize())
	assert.Nil(t, err)
	assert.False(t, ret)

	ret, err = pid2.LessOrEqualThan([]byte{1})
	assert.NotNil(t, err)
	assert.False(t, ret)
}

func TestPulsarID_Equal(t *testing.T) {
	msg1 := pulsar.EarliestMessageID()
	pid1 := &pulsarID{
		messageID: msg1,
	}

	msg2 := pulsar.LatestMessageID()
	pid2 := &pulsarID{
		messageID: msg2,
	}

	{
		ret, err := pid1.Equal(pid1.Serialize())
		assert.Nil(t, err)
		assert.True(t, ret)
	}

	{
		ret, err := pid1.Equal(pid2.Serialize())
		assert.Nil(t, err)
		assert.False(t, ret)
	}
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

	str := msgIDToString(mid)
	assert.NotNil(t, str)
	assert.NotZero(t, len(str))
}

func Test_StringToPulsarMsgID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	str := msgIDToString(mid)
	res, err := stringToMsgID(str)
	assert.Nil(t, err)
	assert.NotNil(t, res)
}
