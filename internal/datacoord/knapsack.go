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

package datacoord

import (
	"math"
	"sort"

	"github.com/bits-and-blooms/bitset"
)

type Sizable interface {
	getSegmentSize() int64
	GetID() int64
}

type Knapsack[T Sizable] struct {
	name       string
	candidates []T
}

func newKnapsack[T Sizable](name string, candidates []T) Knapsack[T] {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].getSegmentSize() != candidates[j].getSegmentSize() {
			return candidates[i].getSegmentSize() > candidates[j].getSegmentSize()
		}
		return candidates[i].GetID() < candidates[j].GetID()
	})
	return Knapsack[T]{
		name:       name,
		candidates: candidates,
	}
}

func (c *Knapsack[T]) tryPack(size, maxLeftSize, minSegs, maxSegs int64) (bitset.BitSet, int64) {
	selection := bitset.New(uint(len(c.candidates)))
	left := size
	for i, segment := range c.candidates {
		if maxSegs == 0 {
			break
		}
		if segment.getSegmentSize() <= left {
			selection.Set(uint(i))
			left -= segment.getSegmentSize()
			maxSegs--
		}
	}

	nSelections := selection.Count()
	var minUSegs uint
	if minSegs < 0 {
		minUSegs = 0
	} else {
		minUSegs = uint(minSegs)
	}
	if left > maxLeftSize || nSelections < uint(minUSegs) {
		selection.ClearAll()
		left = size
	}
	return *selection, left
}

func (c *Knapsack[T]) commit(selection bitset.BitSet) []T {
	var (
		candidates = make([]T, 0, len(c.candidates)-int(selection.Count()))
		returns    = make([]T, 0, int(selection.Count()))
	)

	for i, candidate := range c.candidates {
		if selection.Test(uint(i)) {
			returns = append(returns, candidate)
		} else {
			candidates = append(candidates, candidate)
		}
	}
	c.candidates = candidates
	return returns
}

// pack packs up to maxSegs segments into a single segment to match the total size given by size.
// If the remaining size is greater than maxLeftSize, or the number of segments is less than minSegs, return nil.
// returns the packed segments and the remaining size
func (c *Knapsack[T]) pack(size, maxLeftSize, minSegs, maxSegs int64) ([]T, int64) {
	selection, left := c.tryPack(size, maxLeftSize, minSegs, maxSegs)
	if selection.Count() == 0 {
		return nil, size
	}
	segs := c.commit(selection)
	return segs, left
}

func (c *Knapsack[T]) packWith(size, maxLeftSize, minSegs, maxSegs int64, other Knapsack[T]) ([]T, int64) {
	selection, left := c.tryPack(size, math.MaxInt64, 0, maxSegs)
	if selection.Count() == 0 {
		return nil, size
	}

	numPacked := int64(selection.Count())
	otherSelection, left := other.tryPack(left, maxLeftSize, minSegs-numPacked, maxSegs-numPacked)

	if otherSelection.Count() == 0 {
		// If the original selection already satisfied the requirements, return immediately
		if left < maxLeftSize && selection.Count() >= uint(minSegs) {
			return c.commit(selection), left
		}
		return nil, size
	}
	segs := c.commit(selection)
	otherSegs := other.commit(otherSelection)
	return append(segs, otherSegs...), left
}

func newSegmentPacker(name string, candidates []*SegmentInfo) Knapsack[*SegmentInfo] {
	return newKnapsack[*SegmentInfo](name, candidates)
}
