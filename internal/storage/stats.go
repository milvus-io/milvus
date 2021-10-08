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
	"encoding/json"
)

type Int64Stats struct {
	Max int64 `json:"max"`
	Min int64 `json:"min"`
}

type StatsWriter struct {
	buffer []byte
}

func (sw *StatsWriter) GetBuffer() []byte {
	return sw.buffer
}

func (sw *StatsWriter) StatsInt64(msgs []int64) error {
	if len(msgs) < 1 {
		// return error: msgs must has one element at least
		return nil
	}

	stats := &Int64Stats{
		Max: msgs[len(msgs)-1],
		Min: msgs[0],
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

func (sr *StatsReader) GetInt64Stats() Int64Stats {
	stats := Int64Stats{}
	json.Unmarshal(sr.buffer, &stats)
	return stats
}
