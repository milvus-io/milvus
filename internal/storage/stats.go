// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import (
	"encoding/binary"
	"encoding/json"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus/internal/common"
)

const (
	// TODO silverxia maybe need set from config
	bloomFilterSize       uint    = 100000
	maxBloomFalsePositive float64 = 0.005
)

type Stats interface {
}

type Int64Stats struct {
	FieldID int64              `json:"fieldID"`
	Max     int64              `json:"max"`
	Min     int64              `json:"min"`
	BF      *bloom.BloomFilter `json:"bf"`
}

type StatsWriter struct {
	buffer []byte
}

func (sw *StatsWriter) GetBuffer() []byte {
	return sw.buffer
}

func (sw *StatsWriter) StatsInt64(fieldID int64, msgs []int64) error {
	if len(msgs) < 1 {
		// return error: msgs must has one element at least
		return nil
	}

	stats := &Int64Stats{
		FieldID: fieldID,
		Max:     msgs[len(msgs)-1],
		Min:     msgs[0],
		BF:      bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive),
	}
	if fieldID == common.RowIDField {
		b := make([]byte, 8)
		for _, msg := range msgs {
			binary.LittleEndian.PutUint64(b, uint64(msg))
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

type StatsReader struct {
	buffer []byte
}

func (sr *StatsReader) SetBuffer(buffer []byte) {
	sr.buffer = buffer
}

func (sr *StatsReader) GetInt64Stats() (*Int64Stats, error) {
	stats := &Int64Stats{}
	err := json.Unmarshal(sr.buffer, &stats)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func DeserializeStats(blobs []*Blob) ([]*Int64Stats, error) {
	results := make([]*Int64Stats, len(blobs))
	for i, blob := range blobs {
		if blob.Value == nil {
			continue
		}
		sr := &StatsReader{}
		sr.SetBuffer(blob.Value)
		stats, err := sr.GetInt64Stats()
		if err != nil {
			return nil, err
		}
		results[i] = stats
	}
	return results, nil

}
