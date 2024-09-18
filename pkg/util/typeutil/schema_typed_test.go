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
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func BenchmarkCopyPK(b *testing.B) {
	internal := make([]int64, 1000)
	for i := 0; i < 1000; i++ {
		internal[i] = int64(i)
	}
	src := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: internal,
			},
		},
	}

	b.ResetTimer()

	b.Run("Typed", func(b *testing.B) {
		dst := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: make([]int64, 0, 1000),
				},
			},
		}
		for i := 0; i < b.N; i++ {
			for j := 0; j < GetSizeOfIDs(src); j++ {
				CopyPk(dst, src, j)
			}
		}
	})
	b.Run("Any", func(b *testing.B) {
		dst := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: make([]int64, 0, 1000),
				},
			},
		}
		for i := 0; i < b.N; i++ {
			for j := 0; j < GetSizeOfIDs(src); j++ {
				pk := GetPK(src, int64(j))
				AppendPKs(dst, pk)
			}
		}
	})
}
