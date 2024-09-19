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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestPKSlice(t *testing.T) {
	data1 := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{1, 2, 3},
			},
		},
	}

	ints := NewInt64PkSchemaSlice(data1)
	assert.Equal(t, int64(1), ints.Get(0))
	assert.Equal(t, int64(2), ints.Get(1))
	assert.Equal(t, int64(3), ints.Get(2))
	ints.Append(4)
	assert.Equal(t, int64(4), ints.Get(3))

	data2 := &schemapb.IDs{
		IdField: &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: []string{"1", "2", "3"},
			},
		},
	}
	strs := NewStringPkSchemaSlice(data2)
	assert.Equal(t, "1", strs.Get(0))
	assert.Equal(t, "2", strs.Get(1))
	assert.Equal(t, "3", strs.Get(2))
	strs.Append("4")
	assert.Equal(t, "4", strs.Get(3))
}

func TestCopyPk(t *testing.T) {
	type args struct {
		dst      *schemapb.IDs
		src      *schemapb.IDs
		offset   int
		dstAfter *schemapb.IDs
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "ints",
			args: args{
				dst: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
				src: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
				offset: 0,
				dstAfter: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3, 1},
						},
					},
				},
			},
		},
		{
			name: "strs",
			args: args{
				dst: &schemapb.IDs{
					IdField: &schemapb.IDs_StrId{
						StrId: &schemapb.StringArray{
							Data: []string{"1", "2", "3"},
						},
					},
				},
				src: &schemapb.IDs{
					IdField: &schemapb.IDs_StrId{
						StrId: &schemapb.StringArray{
							Data: []string{"1", "2", "3"},
						},
					},
				},
				offset: 0,
				dstAfter: &schemapb.IDs{
					IdField: &schemapb.IDs_StrId{
						StrId: &schemapb.StringArray{
							Data: []string{"1", "2", "3", "1"},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			CopyPk(tt.args.dst, tt.args.src, tt.args.offset)
			assert.Equal(t, tt.args.dst, tt.args.dstAfter)
		})
	}
}

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
