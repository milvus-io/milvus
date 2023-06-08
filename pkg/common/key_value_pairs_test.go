package common

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestCloneKeyValuePairs(t *testing.T) {
	type args struct {
		pairs KeyValuePairs
	}
	tests := []struct {
		name string
		args args
		want KeyValuePairs
	}{
		{
			args: args{
				pairs: nil,
			},
		},
		{
			args: args{
				pairs: []*commonpb.KeyValuePair{
					{Key: "k1", Value: "v1"},
					{Key: "k2", Value: "v2"},
					{Key: "k3", Value: "v3"},
					{Key: "k4", Value: "v4"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clone := CloneKeyValuePairs(tt.args.pairs)
			assert.True(t, clone.Equal(tt.args.pairs))
		})
	}
}
