package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func TestNewKvPairs(t *testing.T) {
	kvPairs := []*commonpb.KeyValuePair{
		{Key: common.DimKey, Value: "128"},
	}
	h := NewKvPairs(kvPairs)
	v, err := h.Get(common.DimKey)
	assert.NoError(t, err)
	assert.Equal(t, "128", v)
	_, err = h.Get("not_exist")
	assert.Error(t, err)
}
