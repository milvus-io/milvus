// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"encoding/json"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus/internal/common"
)

const (
	// TODO silverxia maybe need set from config
	bloomFilterSize       uint    = 100000
	maxBloomFalsePositive float64 = 0.005
)

// Int64Stats contains statistics data for int64 column
type Int64Stats struct {
	FieldID int64              `json:"fieldID"`
	Max     int64              `json:"max"`
	Min     int64              `json:"min"`
	BF      *bloom.BloomFilter `json:"bf"`
}

// StatsWriter writes stats to buffer
type StatsWriter struct {
	buffer []byte
}

// GetBuffer returns buffer
func (sw *StatsWriter) GetBuffer() []byte {
	return sw.buffer
}

// StatsInt64 writes Int64Stats from @msgs with @fieldID to @buffer
func (sw *StatsWriter) StatsInt64(fieldID int64, isPrimaryKey bool, msgs []int64) error {
	if len(msgs) < 1 {
		// return error: msgs must has one element at least
		return nil
	}

	stats := &Int64Stats{
		FieldID: fieldID,
		Max:     msgs[len(msgs)-1],
		Min:     msgs[0],
	}
	if isPrimaryKey {
		stats.BF = bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive)
		b := make([]byte, 8)
		for _, msg := range msgs {
			common.Endian.PutUint64(b, uint64(msg))
			stats.BF.Add(b)
		}
	}
	b, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	sw.buffer = b

	return nil
}

// StatsReader reads stats
type StatsReader struct {
	buffer []byte
}

// SetBuffer sets buffer
func (sr *StatsReader) SetBuffer(buffer []byte) {
	sr.buffer = buffer
}

// GetInt64Stats returns buffer as Int64Stats
func (sr *StatsReader) GetInt64Stats() (*Int64Stats, error) {
	stats := &Int64Stats{}
	err := json.Unmarshal(sr.buffer, &stats)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

// DeserializeStats deserialize @blobs as []*Int64Stats
func DeserializeStats(blobs []*Blob) ([]*Int64Stats, error) {
	results := make([]*Int64Stats, 0, len(blobs))
	for _, blob := range blobs {
		if blob.Value == nil {
			continue
		}
		sr := &StatsReader{}
		sr.SetBuffer(blob.Value)
		stats, err := sr.GetInt64Stats()
		if err != nil {
			return nil, err
		}
		results = append(results, stats)
	}
	return results, nil
}
