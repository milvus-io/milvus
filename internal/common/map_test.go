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
