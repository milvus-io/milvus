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
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/storage"
)

func TestSegment_UpdatePKRange(t *testing.T) {
	seg := &Segment{
		pkStat: pkStatistics{
			pkFilter: bloom.NewWithEstimates(100000, 0.005),
		},
	}

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

		assert.Equal(t, true, seg.pkStat.minPK.LE(pk))
		assert.Equal(t, true, seg.pkStat.maxPK.GE(pk))

		common.Endian.PutUint64(buf, uint64(c))
		assert.True(t, seg.pkStat.pkFilter.Test(buf))
	}
}

func TestSegment_getSegmentStatslog(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	cases := make([][]int64, 0, 100)
	for i := 0; i < 100; i++ {
		tc := make([]int64, 0, 10)
		for j := 0; j < 100; j++ {
			tc = append(tc, rand.Int63())
		}
		cases = append(cases, tc)
	}
	buf := make([]byte, 8)
	for _, tc := range cases {
		seg := &Segment{
			pkStat: pkStatistics{
				pkFilter: bloom.NewWithEstimates(100000, 0.005),
			}}

		seg.updatePKRange(&storage.Int64FieldData{
			Data: tc,
		})

		statBytes, err := seg.getSegmentStatslog(1, schemapb.DataType_Int64)
		assert.NoError(t, err)

		pks := storage.PrimaryKeyStats{}
		err = json.Unmarshal(statBytes, &pks)
		require.NoError(t, err)

		assert.Equal(t, int64(1), pks.FieldID)
		assert.Equal(t, int64(schemapb.DataType_Int64), pks.PkType)

		for _, v := range tc {
			pk := newInt64PrimaryKey(v)
			assert.True(t, pks.MinPk.LE(pk))
			assert.True(t, pks.MaxPk.GE(pk))

			common.Endian.PutUint64(buf, uint64(v))
			assert.True(t, seg.pkStat.pkFilter.Test(buf))
		}
	}

	pks := &storage.PrimaryKeyStats{}
	_, err := json.Marshal(pks)
	assert.NoError(t, err)
}
