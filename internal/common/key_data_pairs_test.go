package common

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/stretchr/testify/assert"
)

func TestCloneKeyDataPairs(t *testing.T) {
	type args struct {
		pairs KeyDataPairs
	}
	tests := []struct {
		name string
		args args
	}{
		{
			args: args{
				pairs: nil,
			},
		},
		{
			args: args{
				pairs: []*commonpb.KeyDataPair{
					{Key: "k1", Data: []byte("v1")},
					{Key: "k2", Data: []byte("v2")},
					{Key: "k3", Data: []byte("v3")},
					{Key: "k4", Data: []byte("v4")},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clone := CloneKeyDataPairs(tt.args.pairs)
			assert.True(t, clone.Equal(tt.args.pairs))
		})
	}
}
