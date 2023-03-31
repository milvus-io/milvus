package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCloneByteSlice(t *testing.T) {
	type args struct {
		s ByteSlice
	}
	tests := []struct {
		name string
		args args
		want ByteSlice
	}{
		{
			args: args{s: []byte{0x0}},
			want: []byte{0x0},
		},
		{
			args: args{s: []byte{0xff}},
			want: []byte{0xff},
		},
		{
			args: args{s: []byte{0x0f}},
			want: []byte{0x0f},
		},
		{
			args: args{s: []byte{0xf0}},
			want: []byte{0xf0},
		}, {
			args: args{s: []byte{0x0, 0xff, 0x0f, 0xf0}},
			want: []byte{0x0, 0xff, 0x0f, 0xf0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, tt.want.Equal(tt.args.s))
		})
	}
}
