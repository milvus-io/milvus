package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCloneStringList(t *testing.T) {
	type args struct {
		l StringList
	}
	tests := []struct {
		name string
		args args
	}{
		{
			args: args{
				l: nil,
			},
		},
		{
			args: args{
				l: []string{"s1", "s2"},
			},
		},
		{
			args: args{
				l: []string{"dup", "dup", "dup"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CloneStringList(tt.args.l)
			assert.True(t, got.Equal(tt.args.l))
		})
	}
}
