package datanode

import "github.com/bits-and-blooms/bloom/v3"

// bloomFilterSerde provides methods to serialize and deserialize bloom filter
type bloomFilterSerde interface {
	serialize(bloomFilter *bloom.BloomFilter) ([]byte, error)
	deserialize(rawData []byte) (*bloom.BloomFilter, error)
}

type bloomFilterJSONSerde struct{}

func (serde *bloomFilterJSONSerde) serialize(bloomFilter *bloom.BloomFilter) ([]byte, error) {
	return bloomFilter.MarshalJSON()
}

func (serde *bloomFilterJSONSerde) deserialize(rawData []byte) (*bloom.BloomFilter, error) {
	bloomFilter := new(bloom.BloomFilter)
	err := bloomFilter.UnmarshalJSON(rawData)
	return bloomFilter, err
}
