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

package mqclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRmqID_Serialize(t *testing.T) {
	rid := &rmqID{
		messageID: 8,
	}

	bin := rid.Serialize()
	assert.NotNil(t, bin)
	assert.NotZero(t, len(bin))

	rid.LedgerID()
	rid.EntryID()
	rid.BatchIdx()
	rid.PartitionIdx()
}

func Test_SerializeRmqID(t *testing.T) {
	bin := SerializeRmqID(10)
	assert.NotNil(t, bin)
	assert.NotZero(t, len(bin))
}

func Test_DeserializeRmqID(t *testing.T) {
	bin := SerializeRmqID(5)
	id, err := DeserializeRmqID(bin)
	assert.Nil(t, err)
	assert.Equal(t, id, int64(5))
}
