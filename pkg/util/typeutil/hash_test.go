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

package typeutil

import (
	"log"
	"testing"
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestUint64(t *testing.T) {
	var i int64 = -1
	var u = uint64(i)
	t.Log(i)
	t.Log(u)
}

func TestHash32_Uint64(t *testing.T) {
	var u uint64 = 0x12
	h, err := Hash32Uint64(u)
	assert.NoError(t, err)
	t.Log(h)

	h1, err := Hash32Int64(int64(u))
	assert.NoError(t, err)
	t.Log(h1)
	assert.Equal(t, h, h1)

	/* #nosec G103 */
	b := make([]byte, unsafe.Sizeof(u))
	b[0] = 0x12
	h2, err := Hash32Bytes(b)
	assert.NoError(t, err)

	t.Log(h2)
	assert.Equal(t, h, h2)
}

func TestHash32_String(t *testing.T) {
	var u = "ok"
	h, err := Hash32String(u)
	assert.NoError(t, err)

	t.Log(h)
	log.Println(h)

	b := []byte(u)
	h2, err := Hash32Bytes(b)
	assert.NoError(t, err)
	log.Println(h2)

	assert.Equal(t, uint32(h), h2)
}

func TestHashPK2Channels(t *testing.T) {
	channels := []string{"test1", "test2"}
	int64IDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{100, 102, 102, 103, 104},
			},
		},
	}
	ret := HashPK2Channels(int64IDs, channels)
	assert.Equal(t, 5, len(ret))
	//same pk hash to same channel
	assert.Equal(t, ret[1], ret[2])

	stringIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: []string{"ab", "bc", "bc", "abd", "milvus"},
			},
		},
	}
	ret = HashPK2Channels(stringIDs, channels)
	assert.Equal(t, 5, len(ret))
	assert.Equal(t, ret[1], ret[2])
}

func TestRearrangePartitionsForPartitionKey(t *testing.T) {
	// invalid partition name
	partitions := map[string]int64{
		"invalid": 1,
	}

	partitionNames, partitionIDs, err := RearrangePartitionsForPartitionKey(partitions)
	assert.Error(t, err)
	assert.Nil(t, partitionNames)
	assert.Nil(t, partitionIDs)

	// invalid partition index
	partitions = map[string]int64{
		"invalid_a": 1,
	}

	partitionNames, partitionIDs, err = RearrangePartitionsForPartitionKey(partitions)
	assert.Error(t, err)
	assert.Nil(t, partitionNames)
	assert.Nil(t, partitionIDs)

	partitions = map[string]int64{
		"invalid_1": 1,
	}

	partitionNames, partitionIDs, err = RearrangePartitionsForPartitionKey(partitions)
	assert.Error(t, err)
	assert.Nil(t, partitionNames)
	assert.Nil(t, partitionIDs)

	// success cases
	validateFunc := func(partitions map[string]int64) {
		partitionNames, partitionIDs, err = RearrangePartitionsForPartitionKey(partitions)
		assert.NoError(t, err)
		assert.Equal(t, len(partitions), len(partitionNames))
		assert.Equal(t, len(partitions), len(partitionIDs))

		for i := 0; i < len(partitions); i++ {
			assert.Contains(t, partitions, partitionNames[i])
			assert.Equal(t, partitions[partitionNames[i]], partitionIDs[i])

			if i > 0 {
				assert.Greater(t, partitionIDs[i], partitionIDs[i-1])
			}
		}
	}

	validateFunc(map[string]int64{
		"p_0": 1,
		"p_1": 2,
		"p_2": 3,
	})

	validateFunc(map[string]int64{
		"p_2": 3,
		"p_1": 2,
		"p_0": 1,
	})

	validateFunc(map[string]int64{
		"p_1": 2,
		"p_2": 3,
		"p_0": 1,
	})
}
