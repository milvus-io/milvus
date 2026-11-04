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

package view

import (
	"math"
	"sort"

	"github.com/bits-and-blooms/bitset"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
)

type Sizable interface {
	GetSegmentSize() int64
	GetID() int64
	GetEarliestTs() uint64
	GetResidualSegmentSize() int64
}

type Knapsack[T Sizable] struct {
	name       string
	Candidates []T
}

func newKnapsack[T Sizable](name string, candidates []T, less func(T, T) bool) *Knapsack[T] {
	sort.Slice(candidates, func(i, j int) bool {
		return less(candidates[i], candidates[j])
	})
	return &Knapsack[T]{
		name:       name,
		Candidates: candidates,
	}
}

func (c *Knapsack[T]) tryPack(size, maxLeftSize, minSegs, maxSegs int64) (bitset.BitSet, int64) {
	selection := bitset.New(uint(len(c.Candidates)))
	left := size
	for i, segment := range c.Candidates {
		if maxSegs == 0 {
			break
		}
		if segment.GetResidualSegmentSize() <= left {
			selection.Set(uint(i))
			left -= segment.GetResidualSegmentSize()
			maxSegs--
		}
	}

	nSelections := selection.Count()
	if left > maxLeftSize || int64(nSelections) < minSegs {
		selection.ClearAll()
		left = size
	}
	return *selection, left
}

func (c *Knapsack[T]) commit(selection bitset.BitSet) []T {
	var (
		candidates = make([]T, 0, len(c.Candidates)-int(selection.Count()))
		returns    = make([]T, 0, int(selection.Count()))
	)

	for i, candidate := range c.Candidates {
		if selection.Test(uint(i)) {
			returns = append(returns, candidate)
		} else {
			candidates = append(candidates, candidate)
		}
	}
	c.Candidates = candidates
	return returns
}

// pack packs up to maxSegs segments into a single segment to match the total size given by size.
// If the remaining size is greater than maxLeftSize, or the number of segments is less than minSegs, return nil.
// returns the packed segments and the remaining size
func (c *Knapsack[T]) Pack(size, maxLeftSize, minSegs, maxSegs int64) ([]T, int64) {
	selection, left := c.tryPack(size, maxLeftSize, minSegs, maxSegs)
	if selection.Count() == 0 {
		return nil, size
	}
	segs := c.commit(selection)
	return segs, left
}

// packSliding is like pack but slides past blocking head candidates.
// When the greedy packer fails because the largest candidate consumes
// too much capacity for the rest to form a valid group, packSliding
// skips that candidate and retries with the remainder. Skipped
// candidates are returned to the pool afterward.
func (c *Knapsack[T]) PackSliding(size, maxLeftSize, minSegs, maxSegs int64) ([]T, int64) {
	var skipped []T
	for {
		segs, left := c.Pack(size, maxLeftSize, minSegs, maxSegs)
		if len(segs) > 0 {
			c.Candidates = append(skipped, c.Candidates...)
			return segs, left
		}
		if len(c.Candidates) < 2 {
			break
		}
		skipped = append(skipped, c.Candidates[0])
		c.Candidates = c.Candidates[1:]
	}
	c.Candidates = append(skipped, c.Candidates...)
	return nil, size
}

func (c *Knapsack[T]) PackWith(size, maxLeftSize, minSegs, maxSegs int64, other *Knapsack[T]) ([]T, int64) {
	selection, left := c.tryPack(size, math.MaxInt64, 0, maxSegs)
	if selection.Count() == 0 {
		return nil, size
	}

	numPacked := int64(selection.Count())
	otherSelection, left := other.tryPack(left, maxLeftSize, minSegs-numPacked, maxSegs-numPacked)

	if otherSelection.Count() == 0 {
		if left < maxLeftSize && int64(selection.Count()) >= minSegs {
			return c.commit(selection), left
		}
		return nil, size
	}
	segs := c.commit(selection)
	otherSegs := other.commit(otherSelection)
	return append(segs, otherSegs...), left
}

func NewSegmentPacker(name string, candidates []*meta.SegmentInfo, compactTime *CompactTime) *Knapsack[*meta.SegmentInfo] {
	if compactTime != nil && compactTime.CollectionTTL > 0 {
		return newKnapsack(name, candidates, func(a, b *meta.SegmentInfo) bool {
			if a.GetEarliestTs() != b.GetEarliestTs() {
				return a.GetEarliestTs() < b.GetEarliestTs()
			} else if a.GetResidualSegmentSize() != b.GetResidualSegmentSize() {
				return a.GetResidualSegmentSize() > b.GetResidualSegmentSize()
			} else if a.GetSegmentSize() != b.GetSegmentSize() {
				return a.GetSegmentSize() > b.GetSegmentSize()
			}
			return a.GetID() < b.GetID()
		})
	}
	return newKnapsack(name, candidates, func(a, b *meta.SegmentInfo) bool {
		if a.GetResidualSegmentSize() != b.GetResidualSegmentSize() {
			return a.GetResidualSegmentSize() > b.GetResidualSegmentSize()
		} else if a.GetSegmentSize() != b.GetSegmentSize() {
			return a.GetSegmentSize() > b.GetSegmentSize()
		} else if a.GetEarliestTs() != b.GetEarliestTs() {
			return a.GetEarliestTs() < b.GetEarliestTs()
		}
		return a.GetID() < b.GetID()
	})
}
