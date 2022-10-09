package funcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
func TestMin(t *testing.T) {
	assert.Equal(t, uint(0), Min[uint]())
	assert.Equal(t, uint(1), Min[uint](100, 1))
	assert.Equal(t, uint(1), Min[uint](100, 1, 1000))

	assert.Equal(t, uint16(0), Min[uint16]())
	assert.Equal(t, uint16(1), Min[uint16](100, 1))
	assert.Equal(t, uint16(1), Min[uint16](100, 1, 1000))

	assert.Equal(t, uint32(0), Min[uint32]())
	assert.Equal(t, uint32(1), Min[uint32](100, 1))
	assert.Equal(t, uint32(1), Min[uint32](100, 1, 1000))

	assert.Equal(t, uint64(0), Min[uint64]())
	assert.Equal(t, uint64(1), Min[uint64](100, 1))
	assert.Equal(t, uint64(1), Min[uint64](100, 1, 1000))

	assert.Equal(t, 0, Min[int]())
	assert.Equal(t, 1, Min[int](100, 1))
	assert.Equal(t, 1, Min[int](100, 1, 1000))

	assert.Equal(t, int16(0), Min[int16]())
	assert.Equal(t, int16(1), Min[int16](100, 1))
	assert.Equal(t, int16(1), Min[int16](100, 1, 1000))

	assert.Equal(t, int32(0), Min[int32]())
	assert.Equal(t, int32(1), Min[int32](100, 1))
	assert.Equal(t, int32(1), Min[int32](100, 1, 1000))

	assert.Equal(t, int64(0), Min[int64]())
	assert.Equal(t, int64(1), Min[int64](100, 1))
	assert.Equal(t, int64(1), Min[int64](100, 1, 1000))
}

func TestMax(t *testing.T) {
	assert.Equal(t, uint(0), Max[uint]())
	assert.Equal(t, uint(100), Max[uint](100, 1))
	assert.Equal(t, uint(1000), Max[uint](100, 1, 1000))

	assert.Equal(t, uint16(0), Max[uint16]())
	assert.Equal(t, uint16(100), Max[uint16](100, 1))
	assert.Equal(t, uint16(1000), Max[uint16](100, 1, 1000))

	assert.Equal(t, uint32(0), Max[uint32]())
	assert.Equal(t, uint32(100), Max[uint32](100, 1))
	assert.Equal(t, uint32(1000), Max[uint32](100, 1, 1000))

	assert.Equal(t, uint64(0), Max[uint64]())
	assert.Equal(t, uint64(100), Max[uint64](100, 1))
	assert.Equal(t, uint64(1000), Max[uint64](100, 1, 1000))

	assert.Equal(t, 0, Max[int]())
	assert.Equal(t, 100, Max[int](100, 1))
	assert.Equal(t, 1000, Max[int](100, 1, 1000))

	assert.Equal(t, int16(0), Max[int16]())
	assert.Equal(t, int16(100), Max[int16](100, 1))
	assert.Equal(t, int16(1000), Max[int16](100, 1, 1000))

	assert.Equal(t, int32(0), Max[int32]())
	assert.Equal(t, int32(100), Max[int32](100, 1))
	assert.Equal(t, int32(1000), Max[int32](100, 1, 1000))

	assert.Equal(t, int64(0), Max[int64]())
	assert.Equal(t, int64(100), Max[int64](100, 1))
	assert.Equal(t, int64(1000), Max[int64](100, 1, 1000))
}

func TestSum(t *testing.T) {
	assert.Equal(t, uint(0), Sum[uint]())
	assert.Equal(t, uint(101), Sum[uint](100, 1))
	assert.Equal(t, uint(1101), Sum[uint](100, 1, 1000))

	assert.Equal(t, uint16(0), Sum[uint16]())
	assert.Equal(t, uint16(101), Sum[uint16](100, 1))
	assert.Equal(t, uint16(1101), Sum[uint16](100, 1, 1000))

	assert.Equal(t, uint32(0), Sum[uint32]())
	assert.Equal(t, uint32(101), Sum[uint32](100, 1))
	assert.Equal(t, uint32(1101), Sum[uint32](100, 1, 1000))

	assert.Equal(t, uint64(0), Sum[uint64]())
	assert.Equal(t, uint64(101), Sum[uint64](100, 1))
	assert.Equal(t, uint64(1101), Sum[uint64](100, 1, 1000))

	assert.Equal(t, 0, Sum[int]())
	assert.Equal(t, 101, Sum[int](100, 1))
	assert.Equal(t, 1101, Sum[int](100, 1, 1000))

	assert.Equal(t, int16(0), Sum[int16]())
	assert.Equal(t, int16(101), Sum[int16](100, 1))
	assert.Equal(t, int16(1101), Sum[int16](100, 1, 1000))

	assert.Equal(t, int32(0), Sum[int32]())
	assert.Equal(t, int32(101), Sum[int32](100, 1))
	assert.Equal(t, int32(1101), Sum[int32](100, 1, 1000))

	assert.Equal(t, int64(0), Sum[int64]())
	assert.Equal(t, int64(101), Sum[int64](100, 1))
	assert.Equal(t, int64(1101), Sum[int64](100, 1, 1000))
}

*/

func TestMin(t *testing.T) {
	type args struct {
		t1 uint64
		t2 uint64
	}
	tests := []struct {
		name string
		args args
		want uint64
	}{
		{
			args: args{t1: 100, t2: 1},
			want: 1,
		},
		{
			args: args{t1: 1, t2: 100},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Min(tt.args.t1, tt.args.t2), "Min(%v, %v)", tt.args.t1, tt.args.t2)
		})
	}
}
