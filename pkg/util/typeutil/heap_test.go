package typeutil

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinimumHeap(t *testing.T) {
	h := []int{4, 5, 2}
	heap := NewArrayBasedMinimumHeap(h)
	assert.Equal(t, 2, heap.Peek())
	assert.Equal(t, 3, heap.Len())
	heap.Push(3)
	assert.Equal(t, 2, heap.Peek())
	assert.Equal(t, 4, heap.Len())
	heap.Push(1)
	assert.Equal(t, 1, heap.Peek())
	assert.Equal(t, 5, heap.Len())
	for i := 1; i <= 5; i++ {
		assert.Equal(t, i, heap.Peek())
		assert.Equal(t, i, heap.Pop())
	}
}

func TestMaximumHeap(t *testing.T) {
	h := []int{4, 1, 2}
	heap := NewArrayBasedMaximumHeap(h)
	assert.Equal(t, 4, heap.Peek())
	assert.Equal(t, 3, heap.Len())
	heap.Push(3)
	assert.Equal(t, 4, heap.Peek())
	assert.Equal(t, 4, heap.Len())
	heap.Push(5)
	assert.Equal(t, 5, heap.Peek())
	assert.Equal(t, 5, heap.Len())
	for i := 5; i >= 1; i-- {
		assert.Equal(t, i, heap.Peek())
		assert.Equal(t, i, heap.Pop())
	}
}

type FooHeapObject struct {
	value int
}

func GetFooHeapObjectOrderFunc(obj *FooHeapObject) int {
	return obj.value
}

func TestMinimumObjectHeap(t *testing.T) {
	h := []*FooHeapObject{
		{value: 4},
		{value: 5},
		{value: 2},
	}
	heap := NewObjectArrayBasedMinimumHeap(h, GetFooHeapObjectOrderFunc)
	assert.Equal(t, 2, heap.Peek().value)
	assert.Equal(t, 3, heap.Len())
	heap.Push(&FooHeapObject{value: 3})
	assert.Equal(t, 2, heap.Peek().value)
	assert.Equal(t, 4, heap.Len())
	heap.Push(&FooHeapObject{value: 1})
	assert.Equal(t, 1, heap.Peek().value)
	assert.Equal(t, 5, heap.Len())
	for i := 1; i <= 5; i++ {
		assert.Equal(t, i, heap.Peek().value)
		assert.Equal(t, i, heap.Pop().value)
	}
}

func TestMaximumObjectHeap(t *testing.T) {
	h := []*FooHeapObject{
		{value: 4},
		{value: 1},
		{value: 2},
	}
	heap := NewObjectArrayBasedMaximumHeap(h, GetFooHeapObjectOrderFunc)
	assert.Equal(t, 4, heap.Peek().value)
	assert.Equal(t, 3, heap.Len())
	heap.Push(&FooHeapObject{value: 3})
	assert.Equal(t, 4, heap.Peek().value)
	assert.Equal(t, 4, heap.Len())
	heap.Push(&FooHeapObject{value: 5})
	assert.Equal(t, 5, heap.Peek().value)
	assert.Equal(t, 5, heap.Len())
	for i := 5; i >= 1; i-- {
		assert.Equal(t, i, heap.Peek().value)
		assert.Equal(t, i, heap.Pop().value)
	}
}

func TestFloatingHeaps(t *testing.T) {
	type namedFloat32 float32
	type namedFloat64 float64
	nan32 := []float32{math.Float32frombits(0x7fc00001), math.Float32frombits(0xffc00001), math.Float32frombits(0x7fc00002)}
	nan64 := []float64{math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0xfff8000000000001), math.Float64frombits(0x7ff8000000000002)}
	t.Run("float32", func(t *testing.T) { checkFloatingHeaps(t, nan32) })
	t.Run("float64", func(t *testing.T) { checkFloatingHeaps(t, nan64) })
	t.Run("namedFloat32", func(t *testing.T) {
		checkFloatingHeaps(t, []namedFloat32{namedFloat32(nan32[0]), namedFloat32(nan32[1]), namedFloat32(nan32[2])})
	})
	t.Run("namedFloat64", func(t *testing.T) {
		checkFloatingHeaps(t, []namedFloat64{namedFloat64(nan64[0]), namedFloat64(nan64[1]), namedFloat64(nan64[2])})
	})
}

func checkFloatingHeaps[T ~float32 | ~float64](t *testing.T, nans []T) {
	t.Helper()
	type object struct{ value T }
	negInf, posInf := T(math.Inf(-1)), T(math.Inf(1))
	negZero := T(math.Copysign(0, -1))
	cases := []struct {
		name   string
		values []T
		want   []T
	}{
		{
			"mixed",
			[]T{nans[0], 3, posInf, negZero, -2, nans[1], negInf, 0, 1, nans[2]},
			[]T{negInf, -2, negZero, 0, 1, 3, posInf, nans[0], nans[1], nans[2]},
		},
		{"NaNOnly", nans, nans},
		{"equal", []T{3, 3, 3, 3}, []T{3, 3, 3, 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, maximum := range []bool{false, true} {
				for _, initialCount := range []int{0, len(tc.values) / 2, len(tc.values)} {
					values := append([]T(nil), tc.values[:initialCount]...)
					objects := make([]object, initialCount)
					for i, value := range tc.values[:initialCount] {
						objects[i] = object{value}
					}
					var arrayHeap Heap[T]
					var objectHeap Heap[object]
					if maximum {
						arrayHeap = NewArrayBasedMaximumHeap(values)
						objectHeap = NewObjectArrayBasedMaximumHeap(objects, func(o object) T { return o.value })
					} else {
						arrayHeap = NewArrayBasedMinimumHeap(values)
						objectHeap = NewObjectArrayBasedMinimumHeap(objects, func(o object) T { return o.value })
					}
					for _, value := range tc.values[initialCount:] {
						arrayHeap.Push(value)
						objectHeap.Push(object{value})
					}
					assert.Equal(t, len(tc.want), arrayHeap.Len())
					assert.Equal(t, len(tc.want), objectHeap.Len())
					for i := range tc.want {
						want := tc.want[i]
						if maximum {
							want = tc.want[len(tc.want)-1-i]
						}
						for _, got := range []T{arrayHeap.Peek(), objectHeap.Peek().value, arrayHeap.Pop(), objectHeap.Pop().value} {
							if math.IsNaN(float64(want)) {
								assert.True(t, math.IsNaN(float64(got)), "maximum=%v initial=%d position=%d", maximum, initialCount, i)
							} else {
								assert.Equal(t, want, got, "maximum=%v initial=%d position=%d", maximum, initialCount, i)
							}
						}
					}
					assert.Zero(t, arrayHeap.Len())
					assert.Zero(t, objectHeap.Len())
				}
			}
		})
	}
	// Equal-key values must compare false in both directions, regardless of payload.
	for _, values := range [][]T{nans, {negZero, 0}} {
		h := heapArray[T](values)
		for i := range values {
			for j := range values {
				assert.False(t, h.Less(i, j))
			}
		}
	}
}

func TestHeapNonFloatingOrder(t *testing.T) {
	t.Run("uint64", func(t *testing.T) {
		h := NewArrayBasedMinimumHeap([]uint64{math.MaxUint64, 1<<53 + 1})
		h.Push(1 << 53)
		h.Push(0)
		for _, want := range []uint64{0, 1 << 53, 1<<53 + 1, math.MaxUint64} {
			assert.Equal(t, want, h.Pop())
		}
	})
	t.Run("string", func(t *testing.T) {
		h := NewArrayBasedMaximumHeap([]string{"a", "NaN"})
		h.Push("z")
		h.Push("")
		for _, want := range []string{"z", "a", "NaN", ""} {
			assert.Equal(t, want, h.Pop())
		}
	})
}
