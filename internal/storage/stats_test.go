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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatsWriter_StatsInt64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	sw := &StatsWriter{}
	err := sw.StatsInt64(data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	assert.Equal(t, string(b), `{"max":9,"min":1}`)

	sr := &StatsReader{}
	sr.SetBuffer(b)
	stats := sr.GetInt64Stats()
	expectedStats := Int64Stats{
		Max: 9,
		Min: 1,
	}
	assert.Equal(t, stats, expectedStats)

	msgs := []int64{}
	err = sw.StatsInt64(msgs)
	assert.Nil(t, err)
}
