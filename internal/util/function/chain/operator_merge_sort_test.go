/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

// sortAndExtractResults used to sort a []any with sort.SliceStable, looking each
// score up in a map inside the comparator. It now sorts a slice of structs that
// already carry the score. This pins the resulting order against the previous
// formulation, including the tie-break by ID and the behavior for scores that
// compare unequal in both directions (NaN).
func TestSortAndExtractResultsMatchesPreviousOrdering(t *testing.T) {
	legacyOrder := func(idScores map[any]float32, descending bool) []any {
		ids := make([]any, 0, len(idScores))
		for id := range idScores {
			ids = append(ids, id)
		}
		// Deterministic starting point: map iteration order is random, and
		// sort.SliceStable only preserves the order of elements it considers
		// equal, so both implementations must start from the same permutation
		// to be comparable.
		sort.Slice(ids, func(i, j int) bool { return compareIDs(ids[i], ids[j]) < 0 })
		sort.SliceStable(ids, func(i, j int) bool {
			scoreI := idScores[ids[i]]
			scoreJ := idScores[ids[j]]
			if scoreI != scoreJ {
				if descending {
					return scoreI > scoreJ
				}
				return scoreI < scoreJ
			}
			return compareIDs(ids[i], ids[j]) < 0
		})
		return ids
	}

	newOrder := func(idScores map[any]float32, descending bool) []any {
		entries := make([]scoredID, 0, len(idScores))
		for id, score := range idScores {
			key := candidateKey{}
			switch value := id.(type) {
			case int64:
				key.intID = value
			case string:
				key.kind = candidateIDString
				key.stringID = value
			}
			entries = append(entries, scoredID{key: key, score: score})
		}
		sort.Slice(entries, func(i, j int) bool { return compareCandidateKeys(entries[i].key, entries[j].key) < 0 })
		sortScoredIDs(entries, descending)
		ids := make([]any, len(entries))
		for i, e := range entries {
			if e.key.kind == candidateIDString {
				ids[i] = e.key.stringID
			} else {
				ids[i] = e.key.intID
			}
		}
		return ids
	}

	cases := []struct {
		name   string
		scores map[any]float32
	}{
		{"distinct scores", map[any]float32{
			int64(1): 0.9, int64(2): 0.5, int64(3): 0.7,
		}},
		{"many ties", map[any]float32{
			int64(1): 0.5, int64(2): 0.5, int64(3): 0.5, int64(4): 0.1, int64(5): 0.5,
		}},
		{"string ids", map[any]float32{
			"b": 0.5, "a": 0.5, "c": 0.9,
		}},
		{"nan score", map[any]float32{
			int64(1): float32(math.NaN()), int64(2): 0.5, int64(3): 0.9,
		}},
	}

	for _, c := range cases {
		for _, desc := range []bool{true, false} {
			assert.Equal(t, legacyOrder(c.scores, desc), newOrder(c.scores, desc),
				"case=%s descending=%v", c.name, desc)
		}
	}

	// Larger randomized case with heavy tie density.
	r := rand.New(rand.NewSource(7))
	big := make(map[any]float32, 500)
	for i := 0; i < 500; i++ {
		big[int64(i)] = float32(r.Intn(5)) / 4 // only 5 distinct scores -> many ties
	}
	for _, desc := range []bool{true, false} {
		assert.Equal(t, legacyOrder(big, desc), newOrder(big, desc), "randomized descending=%v", desc)
	}
}

// sortAndExtractResults must keep ids, scores and locs aligned.
func TestSortAndExtractResultsKeepsTuplesAligned(t *testing.T) {
	idScores := map[candidateKey]float32{{intID: 1}: 0.1, {intID: 2}: 0.9, {intID: 3}: 0.5}
	idLocs := map[candidateKey]idLocation{
		{intID: 1}: {inputIdx: 10, rowIdx: 100},
		{intID: 2}: {inputIdx: 20, rowIdx: 200},
		{intID: 3}: {inputIdx: 30, rowIdx: 300},
	}

	scores, locs := sortAndExtractResults(idScores, idLocs, true)
	assert.Equal(t, []float32{0.9, 0.5, 0.1}, scores)
	assert.Equal(t, []idLocation{{inputIdx: 20, rowIdx: 200}, {inputIdx: 30, rowIdx: 300}, {inputIdx: 10, rowIdx: 100}}, locs)
}
