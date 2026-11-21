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

package compaction

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestCanMergeSort(t *testing.T) {
	params := Params{UseMergeSort: true, MaxSegmentMergeSort: 30}
	// EnableNamespace is false here, so the merge key is [pk] and IsSorted is
	// the flag canMergeSort requires. TestCanMergeSortMatchesMergeKey covers
	// the namespace-enabled half.
	namespaceDisabledSchema := &schemapb.CollectionSchema{}

	t.Run("disabled by param", func(t *testing.T) {
		plan := &datapb.CompactionPlan{
			Schema:         namespaceDisabledSchema,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{SegmentID: 1, IsSorted: true}},
		}
		assert.False(t, CanMergeSort(plan, Params{UseMergeSort: false, MaxSegmentMergeSort: 30}))
	})

	t.Run("unsorted segment rejected", func(t *testing.T) {
		plan := &datapb.CompactionPlan{
			Schema:         namespaceDisabledSchema,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{SegmentID: 1}},
		}
		assert.False(t, CanMergeSort(plan, params))
	})

	t.Run("single sorted segment allowed", func(t *testing.T) {
		plan := &datapb.CompactionPlan{
			Schema:         namespaceDisabledSchema,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{SegmentID: 1, IsSorted: true}},
		}
		assert.True(t, CanMergeSort(plan, params))
	})

	t.Run("too many segments rejected", func(t *testing.T) {
		segs := make([]*datapb.CompactionSegmentBinlogs, params.MaxSegmentMergeSort+1)
		for i := range segs {
			segs[i] = &datapb.CompactionSegmentBinlogs{SegmentID: int64(i), IsSorted: true}
		}
		plan := &datapb.CompactionPlan{Schema: namespaceDisabledSchema, SegmentBinlogs: segs}
		assert.False(t, CanMergeSort(plan, params))
	})

	t.Run("exactly max segments allowed", func(t *testing.T) {
		segs := make([]*datapb.CompactionSegmentBinlogs, params.MaxSegmentMergeSort)
		for i := range segs {
			segs[i] = &datapb.CompactionSegmentBinlogs{SegmentID: int64(i), IsSorted: true}
		}
		plan := &datapb.CompactionPlan{Schema: namespaceDisabledSchema, SegmentBinlogs: segs}
		assert.True(t, CanMergeSort(plan, params))
	})

	t.Run("later unsorted segment rejected", func(t *testing.T) {
		plan := &datapb.CompactionPlan{
			Schema: namespaceDisabledSchema,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 1, IsSorted: true},
				{SegmentID: 2},
			},
		}
		assert.False(t, CanMergeSort(plan, params))
	})
}

func TestCanMergeSortMatchesMergeKey(t *testing.T) {
	params := Params{UseMergeSort: true, MaxSegmentMergeSort: 30}

	t.Run("namespace enabled rejects pk-only sorted segment", func(t *testing.T) {
		plan := &datapb.CompactionPlan{
			Schema: &schemapb.CollectionSchema{EnableNamespace: true},
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 1, IsSorted: true, IsSortedByNamespace: false},
			},
		}
		assert.False(t, CanMergeSort(plan, params))
	})

	t.Run("namespace enabled accepts namespace-sorted segment", func(t *testing.T) {
		plan := &datapb.CompactionPlan{
			Schema: &schemapb.CollectionSchema{EnableNamespace: true},
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 1, IsSorted: false, IsSortedByNamespace: true},
			},
		}
		assert.True(t, CanMergeSort(plan, params))
	})

	t.Run("namespace disabled rejects namespace-sorted segment", func(t *testing.T) {
		plan := &datapb.CompactionPlan{
			Schema: &schemapb.CollectionSchema{EnableNamespace: false},
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 1, IsSorted: false, IsSortedByNamespace: true},
			},
		}
		assert.False(t, CanMergeSort(plan, params))
	})

	t.Run("mixed flags rejected when one does not match", func(t *testing.T) {
		plan := &datapb.CompactionPlan{
			Schema: &schemapb.CollectionSchema{EnableNamespace: true},
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 1, IsSortedByNamespace: true},
				{SegmentID: 2, IsSorted: true},
			},
		}
		assert.False(t, CanMergeSort(plan, params))
	})
}
