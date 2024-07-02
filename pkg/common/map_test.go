package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCloneStr2Str(t *testing.T) {
	type args struct {
		m Str2Str
	}
	tests := []struct {
		name string
		args args
	}{
		{
			args: args{
				m: nil,
			},
		},
		{
			args: args{
				m: map[string]string{
					"k1": "v1",
					"k2": "v2",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CloneStr2Str(tt.args.m)
			assert.True(t, got.Equal(tt.args.m))
		})
	}
}

func TestMapEqual(t *testing.T) {
	{
		m1 := map[int64]int64{1: 11, 2: 22, 3: 33}
		m2 := map[int64]int64{1: 11, 2: 22, 3: 33}
		assert.True(t, MapEquals(m1, m2))
	}
	{
		m1 := map[int64]int64{1: 11, 2: 23, 3: 33}
		m2 := map[int64]int64{1: 11, 2: 22, 3: 33}
		assert.False(t, MapEquals(m1, m2))
	}
	{
		m1 := map[int64]int64{1: 11, 2: 23, 3: 33}
		m2 := map[int64]int64{1: 11, 2: 22}
		assert.False(t, MapEquals(m1, m2))
	}
	{
		m1 := map[int64]int64{1: 11, 2: 23, 3: 33}
		assert.False(t, MapEquals(m1, nil))
	}
}
