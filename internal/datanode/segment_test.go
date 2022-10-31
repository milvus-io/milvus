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

package datanode

import (
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/stretchr/testify/assert"
)

func TestSegment_UpdatePKRange(t *testing.T) {
	seg := &Segment{}

	cases := make([]int64, 0, 100)
	for i := 0; i < 100; i++ {
		cases = append(cases, rand.Int63())
	}
	buf := make([]byte, 8)
	for _, c := range cases {
		seg.updatePKRange(&storage.Int64FieldData{
			Data: []int64{c},
		})

		pk := newInt64PrimaryKey(c)

		assert.Equal(t, true, seg.currentStat.MinPK.LE(pk))
		assert.Equal(t, true, seg.currentStat.MaxPK.GE(pk))

		common.Endian.PutUint64(buf, uint64(c))
		assert.True(t, seg.currentStat.PkFilter.Test(buf))

		assert.True(t, seg.isPKExist(pk))
	}
}

func TestEmptySegment(t *testing.T) {
	seg := &Segment{}

	pk := newInt64PrimaryKey(1000)
	assert.False(t, seg.isPKExist(pk))
}
