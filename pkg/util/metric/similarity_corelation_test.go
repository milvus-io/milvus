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

package metric

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsBounded(t *testing.T) {
	cases := []struct {
		metricType string
		want       bool
	}{
		{COSINE, true},
		{MaxSimCosine, true},
		{"cosine", true},   // case-insensitive
		{"MAX_SIM_COSINE", true},
		{IP, false},
		{L2, false},
		{HAMMING, false},
		{JACCARD, false},
		{BM25, false},
		{MaxSim, false},
		{MaxSimIP, false},
		{MaxSimL2, false},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, IsBounded(c.metricType), "IsBounded(%q)", c.metricType)
	}
}

func TestClampCosineScores(t *testing.T) {
	t.Run("clamps values above 1.0 for COSINE", func(t *testing.T) {
		scores := []float32{1.0000001192092896, 0.5, 1.0}
		ClampCosineScores(scores, COSINE)
		assert.Equal(t, float32(1.0), scores[0])
		assert.Equal(t, float32(0.5), scores[1])
		assert.Equal(t, float32(1.0), scores[2])
	})

	t.Run("clamps values below -1.0 for COSINE", func(t *testing.T) {
		scores := []float32{-1.0000001192092896, -0.5, -1.0}
		ClampCosineScores(scores, COSINE)
		assert.Equal(t, float32(-1.0), scores[0])
		assert.Equal(t, float32(-0.5), scores[1])
		assert.Equal(t, float32(-1.0), scores[2])
	})

	t.Run("clamps values for MAX_SIM_COSINE", func(t *testing.T) {
		scores := []float32{1.0000001192092896, -1.0000001192092896}
		ClampCosineScores(scores, MaxSimCosine)
		assert.Equal(t, float32(1.0), scores[0])
		assert.Equal(t, float32(-1.0), scores[1])
	})

	t.Run("case-insensitive metric matching", func(t *testing.T) {
		scores := []float32{1.5}
		ClampCosineScores(scores, "cosine")
		assert.Equal(t, float32(1.0), scores[0])
	})

	t.Run("no-op for IP metric", func(t *testing.T) {
		scores := []float32{1.5, -1.5, 100.0}
		ClampCosineScores(scores, IP)
		assert.Equal(t, float32(1.5), scores[0])
		assert.Equal(t, float32(-1.5), scores[1])
		assert.Equal(t, float32(100.0), scores[2])
	})

	t.Run("no-op for L2 metric", func(t *testing.T) {
		scores := []float32{2.5, 0.0}
		ClampCosineScores(scores, L2)
		assert.Equal(t, float32(2.5), scores[0])
	})

	t.Run("no-op for empty slice", func(t *testing.T) {
		scores := []float32{}
		ClampCosineScores(scores, COSINE)
		assert.Empty(t, scores)
	})

	t.Run("boundary values exactly at -1 and 1 are unchanged", func(t *testing.T) {
		scores := []float32{1.0, -1.0, 0.0}
		ClampCosineScores(scores, COSINE)
		assert.Equal(t, float32(1.0), scores[0])
		assert.Equal(t, float32(-1.0), scores[1])
		assert.Equal(t, float32(0.0), scores[2])
	})

	t.Run("NaN values are passed through unchanged", func(t *testing.T) {
		scores := []float32{float32(math.NaN())}
		ClampCosineScores(scores, COSINE)
		assert.True(t, math.IsNaN(float64(scores[0])))
	})
}

func TestPositivelyRelated(t *testing.T) {
	cases := []struct {
		metricType string
		wanted     bool
	}{
		{
			IP,
			true,
		},
		{
			COSINE,
			true,
		},
		{
			L2,
			false,
		},
		{
			HAMMING,
			false,
		},
		{
			JACCARD,
			false,
		},
		{
			SUBSTRUCTURE,
			false,
		},
		{
			SUPERSTRUCTURE,
			false,
		},
		// MAX_SIM metrics - positively related (higher is better)
		{
			MaxSim,
			true,
		},
		{
			MaxSimIP,
			true,
		},
		{
			MaxSimCosine,
			true,
		},
		// MAX_SIM metrics - negatively related (lower is better)
		{
			MaxSimL2,
			false,
		},
		{
			MaxSimHamming,
			false,
		},
		{
			MaxSimJaccard,
			false,
		},
	}

	for idx := range cases {
		if got := PositivelyRelated(cases[idx].metricType); got != cases[idx].wanted {
			t.Errorf("PositivelyRelated(%v) = %v", cases[idx].metricType, cases[idx].wanted)
		}
	}
}
