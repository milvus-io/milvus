package datanode

import (
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/assert"
)

func TestBloomFilterJsonSerde(t *testing.T) {
	serde := new(bloomFilterJSONSerde)
	bloomFilter := bloom.NewWithEstimates(1000, 0.01)
	rawData, err := serde.serialize(bloomFilter)
	assert.Nil(t, err)
	deserBloomFilter, err := serde.deserialize(rawData)
	assert.Nil(t, err)
	assert.EqualValues(t, bloomFilter, deserBloomFilter)
}
