package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncoder(t *testing.T) {
	result, err := DecodeInt64(EncodeInt64(1))
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	result2, err := DecodeUint64(EncodeUint64(1))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), result2)

	result3, err := EncodeProto(&InsertMessageHeader{
		CollectionId: 1,
	})
	assert.NoError(t, err)

	var result4 InsertMessageHeader
	err = DecodeProto(result3, &result4)
	assert.NoError(t, err)
	assert.Equal(t, result4.CollectionId, int64(1))
}
