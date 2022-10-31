package allocator

import "sort"

func makeClone(s []int64) []int64 {
	clone := make([]int64, len(s))
	copy(clone, s)
	return clone
}

func NewAllocatorFromList(s []int64, useClone, deAsc bool) *AtomicAllocator {
	initialized, delta := int64(defaultInitializedValue), int64(defaultDelta)
	clone := s
	if useClone {
		clone = makeClone(s)
	}
	sort.Slice(clone, func(i, j int) bool { return clone[i] < clone[j] })
	l := len(clone)
	if l == 0 {
		// no change
	} else if deAsc {
		initialized, delta = clone[0], -1
	} else {
		initialized, delta = clone[l-1], 1
	}
	return NewAllocator(WithInitializedValue(initialized), WithDelta(delta))
}
