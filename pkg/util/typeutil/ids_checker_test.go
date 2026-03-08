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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestNewIDsChecker(t *testing.T) {
	t.Run("nil IDs", func(t *testing.T) {
		checker, err := NewIDsChecker(nil)
		assert.NoError(t, err)
		assert.True(t, checker.IsEmpty())
	})

	t.Run("empty IDs", func(t *testing.T) {
		ids := &schemapb.IDs{}
		checker, err := NewIDsChecker(ids)
		assert.NoError(t, err)
		assert.True(t, checker.IsEmpty())
	})

	t.Run("int64 IDs", func(t *testing.T) {
		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5},
				},
			},
		}
		checker, err := NewIDsChecker(ids)
		assert.NoError(t, err)
		assert.False(t, checker.IsEmpty())
		assert.Equal(t, 5, checker.Size())
		assert.Equal(t, schemapb.DataType_Int64, checker.GetIDType())
	})

	t.Run("string IDs", func(t *testing.T) {
		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: []string{"a", "b", "c", "d"},
				},
			},
		}
		checker, err := NewIDsChecker(ids)
		assert.NoError(t, err)
		assert.False(t, checker.IsEmpty())
		assert.Equal(t, 4, checker.Size())
		assert.Equal(t, schemapb.DataType_VarChar, checker.GetIDType())
	})

	t.Run("duplicate IDs", func(t *testing.T) {
		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 2, 3, 3, 3},
				},
			},
		}
		checker, err := NewIDsChecker(ids)
		assert.NoError(t, err)
		assert.Equal(t, 3, checker.Size()) // Only unique IDs are counted
	})
}

func TestIDsChecker_Contains(t *testing.T) {
	// Create checker with int64 IDs
	checkerIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{10, 20, 30, 40, 50},
			},
		},
	}
	checker, err := NewIDsChecker(checkerIDs)
	require.NoError(t, err)

	t.Run("valid int64 lookups", func(t *testing.T) {
		queryIDs := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{10, 15, 20, 25, 30},
				},
			},
		}

		// Test each position
		exists, err := checker.Contains(queryIDs, 0) // 10 - should exist
		assert.NoError(t, err)
		assert.True(t, exists)

		exists, err = checker.Contains(queryIDs, 1) // 15 - should not exist
		assert.NoError(t, err)
		assert.False(t, exists)

		exists, err = checker.Contains(queryIDs, 2) // 20 - should exist
		assert.NoError(t, err)
		assert.True(t, exists)

		exists, err = checker.Contains(queryIDs, 3) // 25 - should not exist
		assert.NoError(t, err)
		assert.False(t, exists)

		exists, err = checker.Contains(queryIDs, 4) // 30 - should exist
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("out of bounds", func(t *testing.T) {
		queryIDs := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{10, 20},
				},
			},
		}

		_, err := checker.Contains(queryIDs, -1)
		assert.Error(t, err)

		_, err = checker.Contains(queryIDs, 2)
		assert.Error(t, err)
	})

	t.Run("type mismatch", func(t *testing.T) {
		queryIDs := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: []string{"a", "b"},
				},
			},
		}

		_, err := checker.Contains(queryIDs, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "type mismatch")
	})

	t.Run("nil query IDs", func(t *testing.T) {
		_, err := checker.Contains(nil, 0)
		assert.Error(t, err)
	})
}

func TestIDsChecker_ContainsString(t *testing.T) {
	// Create checker with string IDs
	checkerIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: []string{"apple", "banana", "cherry", "date"},
			},
		},
	}
	checker, err := NewIDsChecker(checkerIDs)
	require.NoError(t, err)

	t.Run("valid string lookups", func(t *testing.T) {
		queryIDs := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: []string{"apple", "grape", "banana", "kiwi"},
				},
			},
		}

		exists, err := checker.Contains(queryIDs, 0) // "apple" - should exist
		assert.NoError(t, err)
		assert.True(t, exists)

		exists, err = checker.Contains(queryIDs, 1) // "grape" - should not exist
		assert.NoError(t, err)
		assert.False(t, exists)

		exists, err = checker.Contains(queryIDs, 2) // "banana" - should exist
		assert.NoError(t, err)
		assert.True(t, exists)

		exists, err = checker.Contains(queryIDs, 3) // "kiwi" - should not exist
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestIDsChecker_ContainsAny(t *testing.T) {
	checkerIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{10, 20, 30, 40, 50},
			},
		},
	}
	checker, err := NewIDsChecker(checkerIDs)
	require.NoError(t, err)

	t.Run("some matches", func(t *testing.T) {
		queryIDs := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{5, 10, 15, 20, 25, 30},
				},
			},
		}

		indices, err := checker.ContainsAny(queryIDs)
		assert.NoError(t, err)
		assert.Equal(t, []int{1, 3, 5}, indices) // positions of 10, 20, 30
	})

	t.Run("no matches", func(t *testing.T) {
		queryIDs := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4},
				},
			},
		}

		indices, err := checker.ContainsAny(queryIDs)
		assert.NoError(t, err)
		assert.Empty(t, indices)
	})

	t.Run("all matches", func(t *testing.T) {
		queryIDs := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{10, 20, 30},
				},
			},
		}

		indices, err := checker.ContainsAny(queryIDs)
		assert.NoError(t, err)
		assert.Equal(t, []int{0, 1, 2}, indices)
	})
}

func TestIDsChecker_ContainsIDsAtCursors(t *testing.T) {
	checkerIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{10, 20, 30, 40, 50},
			},
		},
	}
	checker, err := NewIDsChecker(checkerIDs)
	require.NoError(t, err)

	queryIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{10, 15, 20, 25, 30, 35},
			},
		},
	}

	t.Run("batch check", func(t *testing.T) {
		cursors := []int{0, 1, 2, 4}
		results, err := checker.ContainsIDsAtCursors(queryIDs, cursors)
		assert.NoError(t, err)
		assert.Equal(t, []bool{true, false, true, true}, results)
	})

	t.Run("out of bounds in batch", func(t *testing.T) {
		cursors := []int{0, 1, 10} // 10 is out of bounds
		_, err := checker.ContainsIDsAtCursors(queryIDs, cursors)
		assert.Error(t, err)
	})
}

func TestIDsChecker_EmptyChecker(t *testing.T) {
	checker, err := NewIDsChecker(nil)
	require.NoError(t, err)

	queryIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{1, 2, 3},
			},
		},
	}

	// Empty checker should return false for any query
	exists, err := checker.Contains(queryIDs, 0)
	assert.NoError(t, err)
	assert.False(t, exists)

	indices, err := checker.ContainsAny(queryIDs)
	assert.NoError(t, err)
	assert.Empty(t, indices)
}

// Benchmark tests
func BenchmarkIDsChecker_Contains(b *testing.B) {
	// Create a large checker
	data := make([]int64, 10000)
	for i := range data {
		data[i] = int64(i * 2) // Even numbers
	}
	checkerIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: data},
		},
	}
	checker, _ := NewIDsChecker(checkerIDs)

	// Create query IDs
	queryData := make([]int64, 1000)
	for i := range queryData {
		queryData[i] = int64(i) // Mix of even and odd numbers
	}
	queryIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: queryData},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		checker.Contains(queryIDs, i%1000)
	}
}

func BenchmarkIDsChecker_ContainsAny(b *testing.B) {
	// Create a large checker
	data := make([]int64, 10000)
	for i := range data {
		data[i] = int64(i * 2) // Even numbers
	}
	checkerIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: data},
		},
	}
	checker, _ := NewIDsChecker(checkerIDs)

	// Create query IDs
	queryData := make([]int64, 1000)
	for i := range queryData {
		queryData[i] = int64(i) // Mix of even and odd numbers
	}
	queryIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: queryData},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		checker.ContainsAny(queryIDs)
	}
}
