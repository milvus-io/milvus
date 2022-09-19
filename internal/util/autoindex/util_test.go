package autoindex

import (
	"testing"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestAutoIndexUtil_getTopK(t *testing.T) {
	var params []*commonpb.KeyValuePair
	params = append(params, &commonpb.KeyValuePair{
		Key:   TopKKey,
		Value: "10",
	})

	topK, err := getTopK(params)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), topK)

	topK, err = getTopK(nil)
	assert.Error(t, err)
	assert.Equal(t, int64(0), topK)

	var params1 []*commonpb.KeyValuePair
	params1 = append(params1, &commonpb.KeyValuePair{
		Key:   TopKKey,
		Value: "x",
	})

	topK1, err := getTopK(params1)
	assert.Error(t, err)
	assert.Equal(t, int64(0), topK1)

}
