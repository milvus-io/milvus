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

package datanode

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferData(t *testing.T) {
	Params.DataNodeCfg.FlushInsertBufferSize = 16 * (1 << 20) // 16 MB

	tests := []struct {
		isValid bool

		indim         int64
		expectedLimit int64

		description string
	}{
		{true, 1, 4194304, "Smallest of the DIM"},
		{true, 128, 32768, "Normal DIM"},
		{true, 32768, 128, "Largest DIM"},
		{false, 0, 0, "Illegal DIM"},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			idata, err := newBufferData(test.indim)

			if test.isValid {
				assert.NoError(t, err)
				assert.NotNil(t, idata)

				assert.Equal(t, test.expectedLimit, idata.limit)
				assert.Zero(t, idata.size)

				capacity := idata.effectiveCap()
				assert.Equal(t, test.expectedLimit, capacity)
			} else {
				assert.Error(t, err)
				assert.Nil(t, idata)
			}
		})
	}
}

func TestBufferData_updateTimeRange(t *testing.T) {
	Params.DataNodeCfg.FlushInsertBufferSize = 16 * (1 << 20) // 16 MB

	type testCase struct {
		tag string

		trs        []TimeRange
		expectFrom Timestamp
		expectTo   Timestamp
	}

	cases := []testCase{
		{
			tag:        "no input range",
			expectTo:   0,
			expectFrom: math.MaxUint64,
		},
		{
			tag: "single range",
			trs: []TimeRange{
				{timestampMin: 100, timestampMax: 200},
			},
			expectFrom: 100,
			expectTo:   200,
		},
		{
			tag: "multiple range",
			trs: []TimeRange{
				{timestampMin: 150, timestampMax: 250},
				{timestampMin: 100, timestampMax: 200},
				{timestampMin: 50, timestampMax: 180},
			},
			expectFrom: 50,
			expectTo:   250,
		},
	}

	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			bd, err := newBufferData(16)
			require.NoError(t, err)
			for _, tr := range tc.trs {
				bd.updateTimeRange(tr)
			}

			assert.Equal(t, tc.expectFrom, bd.tsFrom)
			assert.Equal(t, tc.expectTo, bd.tsTo)
		})
	}
}
