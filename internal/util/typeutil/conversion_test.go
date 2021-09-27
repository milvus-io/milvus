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

package typeutil

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConversion(t *testing.T) {
	t.Run("TestConvertFloat32", func(t *testing.T) {
		comp := func(f float32) {
			fb := Float32ToBytes(f)
			f1 := BytesToFloat32(fb)
			assert.Less(t, math.Abs(float64(f)-float64(f1)), 0.00001)
		}
		comp(float32(3.14))
		comp(float32(0))
		comp(float32(-139.866))
		comp(float32(math.MaxFloat32))
		comp(float32(-math.MaxFloat32))
	})

	t.Run("TestConvertInt64", func(t *testing.T) {
		comp := func(i int64) {
			ib := Int64ToBytes(i)
			i1, err := BytesToInt64(ib)
			assert.Nil(t, err)
			assert.Equal(t, i, i1)
		}
		comp(int64(314))
		comp(int64(0))
		comp(int64(-8654273))
		comp(int64(math.MaxInt64))
		comp(int64(math.MinInt64))

		_, err := BytesToInt64([]byte("ab"))
		assert.NotNil(t, err)
	})

	t.Run("TestConvertUint64", func(t *testing.T) {
		comp := func(u uint64) {
			ub := Uint64ToBytes(u)
			u1, err := BytesToUint64(ub)
			assert.Nil(t, err)
			assert.Equal(t, u, u1)
		}
		comp(uint64(314))
		comp(uint64(0))
		comp(uint64(75123348654273))
		comp(uint64(math.MaxUint64))

		_, err := BytesToUint64([]byte("ab"))
		assert.NotNil(t, err)
	})

	t.Run("TestSliceRemoveDuplicate", func(t *testing.T) {
		ret := SliceRemoveDuplicate(1)
		assert.Equal(t, 0, len(ret))

		arr := []int64{1, 1, 1, 2, 2, 3}
		ret1 := SliceRemoveDuplicate(arr)
		assert.Equal(t, 3, len(ret1))
	})

}
