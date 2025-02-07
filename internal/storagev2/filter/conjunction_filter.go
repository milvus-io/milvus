// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package filter

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/parquet/metadata"
	"github.com/bits-and-blooms/bitset"
)

type ConjunctionAndFilter struct {
	filters    []Filter
	columnName string
}

func (f *ConjunctionAndFilter) GetColumnName() string {
	return f.columnName
}

// FIXME: should have 3 cases.
// 1. all records satisfy the filter, this group dont need to check filter again.
// 2. no record satisfies the filter.
// 3. some records satisfy the filter, this group should check filter again.
func (f *ConjunctionAndFilter) CheckStatistics(stats metadata.TypedStatistics) bool {
	for _, filter := range f.filters {
		if filter.CheckStatistics(stats) {
			return true
		}
	}
	return false
}

func (f *ConjunctionAndFilter) Type() FilterType {
	return And
}

func (f *ConjunctionAndFilter) Apply(colData arrow.Array, filterBitSet *bitset.BitSet) {
	for i := 0; i < len(f.filters); i++ {
		f.filters[i].Apply(colData, filterBitSet)
	}
}

type ConjunctionOrFilter struct {
	filters []Filter
}

func (f *ConjunctionOrFilter) CheckStatistics(stats metadata.TypedStatistics) bool {
	for _, filter := range f.filters {
		if !filter.CheckStatistics(stats) {
			return false
		}
	}
	return true
}

func (f *ConjunctionOrFilter) Apply(colData arrow.Array, filterBitSet *bitset.BitSet) {
	orBitSet := bitset.New(filterBitSet.Len())
	for i := 1; i < len(f.filters); i++ {
		childBitSet := filterBitSet.Clone()
		f.filters[i].Apply(colData, childBitSet)
		orBitSet.Intersection(childBitSet)
	}
	filterBitSet.Union(orBitSet)
}

func (f *ConjunctionOrFilter) Type() FilterType {
	return Or
}

func NewConjunctionAndFilter(filters ...Filter) *ConjunctionAndFilter {
	return &ConjunctionAndFilter{filters: filters}
}
