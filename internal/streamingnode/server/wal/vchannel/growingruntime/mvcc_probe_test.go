//go:build test && dynamic

package growingruntime

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRuntimeMayHaveVisibleGrowingSegmentsReturnsFalseWhenVisibleCandidateSetIsEmpty(t *testing.T) {
	runtime := newRuntime()
	segment := newGrowingSegment(nil, 10, 100)
	segment.segment = fakeCSegment{id: 10}
	runtime.addSegment(segment)
	runtime.markGrowingTimeTick(20)
	runtime.markTransformTimeTick(10)

	mayHave := runtime.MayHaveVisibleGrowingSegments(20, 10, []int64{200})

	assert.False(t, mayHave)
}

func TestRuntimeMayHaveVisibleGrowingSegmentsReturnsTrueWhenNotVisible(t *testing.T) {
	runtime := newRuntime()
	segment := newGrowingSegment(nil, 10, 100)
	segment.segment = fakeCSegment{id: 10}
	runtime.addSegment(segment)
	runtime.markGrowingTimeTick(19)
	runtime.markTransformTimeTick(10)

	mayHave := runtime.MayHaveVisibleGrowingSegments(20, 10, []int64{100})

	assert.True(t, mayHave)
}

func TestRuntimeMayHaveVisibleGrowingSegmentsReturnsTrueForMatchingPartition(t *testing.T) {
	runtime := newRuntime()
	segment := newGrowingSegment(nil, 10, 100)
	segment.segment = fakeCSegment{id: 10}
	runtime.addSegment(segment)
	runtime.markGrowingTimeTick(20)
	runtime.markTransformTimeTick(10)

	mayHave := runtime.MayHaveVisibleGrowingSegments(20, 10, []int64{100})

	assert.True(t, mayHave)
}
