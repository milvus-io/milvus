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

package proxy

import (
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus/internal/msgstream"

	"github.com/stretchr/testify/assert"
)

func Test_insertRepackFunc(t *testing.T) {
	var err error

	// tsMsgs is empty
	ret1, err := insertRepackFunc(nil, [][]int32{{1, 2}})
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ret1))

	// hashKeys is empty
	tsMsgs2 := []msgstream.TsMsg{
		&msgstream.InsertMsg{}, // not important
		&msgstream.InsertMsg{}, // not important
	}
	ret2, err := insertRepackFunc(tsMsgs2, nil)
	assert.NotNil(t, err)
	assert.Nil(t, ret2)

	// len(hashKeys) < len(tsMsgs), 1 < 2
	ret2, err = insertRepackFunc(tsMsgs2, [][]int32{{1, 2}})
	assert.NotNil(t, err)
	assert.Nil(t, ret2)

	// both tsMsgs & hashKeys are empty
	ret3, err := insertRepackFunc(nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ret3))

	num := rand.Int()%100 + 1
	tsMsgs4 := make([]msgstream.TsMsg, 0)
	for i := 0; i < num; i++ {
		tsMsgs4 = append(tsMsgs4, &msgstream.InsertMsg{
			// not important
		})
	}

	// len(hashKeys) = len(tsMsgs), but no hash key
	hashKeys1 := make([][]int32, num)
	ret4, err := insertRepackFunc(tsMsgs4, hashKeys1)
	assert.NotNil(t, err)
	assert.Nil(t, ret4)

	// all messages are shuffled to same bucket
	hashKeys2 := make([][]int32, num)
	key := int32(0)
	for i := 0; i < num; i++ {
		hashKeys2[i] = []int32{key}
	}
	ret5, err := insertRepackFunc(tsMsgs4, hashKeys2)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(ret5))
	assert.Equal(t, num, len(ret5[key].Msgs))

	// evenly shuffle
	hashKeys3 := make([][]int32, num)
	for i := 0; i < num; i++ {
		hashKeys3[i] = []int32{int32(i)}
	}
	ret6, err := insertRepackFunc(tsMsgs4, hashKeys3)
	assert.Nil(t, err)
	assert.Equal(t, num, len(ret6))
	for key := range ret6 {
		assert.Equal(t, 1, len(ret6[key].Msgs))
	}

	// randomly shuffle
	histogram := make(map[int32]int) // key -> key num
	hashKeys4 := make([][]int32, num)
	for i := 0; i < num; i++ {
		k := int32(rand.Uint32())
		hashKeys4[i] = []int32{k}
		_, exist := histogram[k]
		if exist {
			histogram[k]++
		} else {
			histogram[k] = 1
		}
	}
	ret7, err := insertRepackFunc(tsMsgs4, hashKeys4)
	assert.Nil(t, err)
	assert.Equal(t, len(histogram), len(ret7))
	for key := range ret7 {
		assert.Equal(t, histogram[key], len(ret7[key].Msgs))
	}
}

func Test_defaultInsertRepackFunc(t *testing.T) {
	var err error

	// tsMsgs is empty
	ret1, err := defaultInsertRepackFunc(nil, [][]int32{{1, 2}})
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ret1))

	// hashKeys is empty
	tsMsgs2 := []msgstream.TsMsg{
		&msgstream.InsertMsg{}, // not important
		&msgstream.InsertMsg{}, // not important
	}
	ret2, err := defaultInsertRepackFunc(tsMsgs2, nil)
	assert.NotNil(t, err)
	assert.Nil(t, ret2)

	// len(hashKeys) < len(tsMsgs), 1 < 2
	ret2, err = defaultInsertRepackFunc(tsMsgs2, [][]int32{{1, 2}})
	assert.NotNil(t, err)
	assert.Nil(t, ret2)

	// both tsMsgs & hashKeys are empty
	ret3, err := defaultInsertRepackFunc(nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ret3))

	num := rand.Int()%100 + 1
	tsMsgs4 := make([]msgstream.TsMsg, 0)
	for i := 0; i < num; i++ {
		tsMsgs4 = append(tsMsgs4, &msgstream.InsertMsg{
			// not important
		})
	}

	// len(hashKeys) = len(tsMsgs), but no hash key
	hashKeys1 := make([][]int32, num)
	ret4, err := defaultInsertRepackFunc(tsMsgs4, hashKeys1)
	assert.NotNil(t, err)
	assert.Nil(t, ret4)

	// all messages are shuffled to same bucket
	hashKeys2 := make([][]int32, num)
	key := int32(0)
	for i := 0; i < num; i++ {
		hashKeys2[i] = []int32{key}
	}
	ret5, err := defaultInsertRepackFunc(tsMsgs4, hashKeys2)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(ret5))
	assert.Equal(t, num, len(ret5[key].Msgs))

	// evenly shuffle
	hashKeys3 := make([][]int32, num)
	for i := 0; i < num; i++ {
		hashKeys3[i] = []int32{int32(i)}
	}
	ret6, err := defaultInsertRepackFunc(tsMsgs4, hashKeys3)
	assert.Nil(t, err)
	assert.Equal(t, num, len(ret6))
	for key := range ret6 {
		assert.Equal(t, 1, len(ret6[key].Msgs))
	}

	// randomly shuffle
	histogram := make(map[int32]int) // key -> key num
	hashKeys4 := make([][]int32, num)
	for i := 0; i < num; i++ {
		k := int32(rand.Uint32())
		hashKeys4[i] = []int32{k}
		_, exist := histogram[k]
		if exist {
			histogram[k]++
		} else {
			histogram[k] = 1
		}
	}
	ret7, err := defaultInsertRepackFunc(tsMsgs4, hashKeys4)
	assert.Nil(t, err)
	assert.Equal(t, len(histogram), len(ret7))
	for key := range ret7 {
		assert.Equal(t, histogram[key], len(ret7[key].Msgs))
	}
}
