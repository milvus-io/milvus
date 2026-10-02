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

package featureusage

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// segcoreFeatureBitHeader is the C++ side of the execution feature bit set.
const segcoreFeatureBitHeader = "../core/src/common/FeatureBits.h"

// The bit positions are a wire format between segcore and the Proxy. This
// parses segcore's FeatureBit enum and checks that every bit maps to the
// counter of the same position here, so reordering or inserting on one side
// fails the build instead of silently misreporting.
func TestExecBitsMatchSegcore(t *testing.T) {
	src, err := os.ReadFile(segcoreFeatureBitHeader)
	require.NoError(t, err)
	body := string(src)
	start := strings.Index(body, "enum class FeatureBit")
	require.GreaterOrEqual(t, start, 0)
	end := strings.Index(body[start:], "};")
	require.Greater(t, end, 0)
	enum := body[start : start+end]

	re := regexp.MustCompile(`(?m)^\s*([A-Za-z]+)\s*=\s*(\d+),`)
	matches := re.FindAllStringSubmatch(enum, -1)
	require.Len(t, matches, len(execBitFeatures), "segcore and Go disagree on the number of bits")

	// segcore's names are the Go feature names without the prefix; compare
	// case-insensitively, ignoring the underscores the report names carry.
	norm := func(s string) string { return strings.ToLower(strings.ReplaceAll(s, "_", "")) }
	goNames := map[Feature]string{
		FeatureFilterPathScalarIndex:    "FilterPathScalarIndex",
		FeatureFilterPathPkIndex:        "FilterPathPkIndex",
		FeatureFilterPathTextMatchIndex: "FilterPathTextMatchIndex",
		FeatureFilterPathJSONShredding:  "FilterPathJsonShredding",
		FeatureFilterPathNgramIndex:     "FilterPathNgramIndex",
		FeatureFilterPathBruteForce:     "FilterPathBruteForce",
		FeatureScalarIndexBitmap:        "ScalarIndexBitmap",
		FeatureScalarIndexStlSort:       "ScalarIndexStlSort",
		FeatureScalarIndexTrie:          "ScalarIndexTrie",
		FeatureScalarIndexInverted:      "ScalarIndexInverted",
		FeatureScalarIndexHybrid:        "ScalarIndexHybrid",
		FeatureScalarIndexRtree:         "ScalarIndexRtree",
		FeatureScalarIndexNgram:         "ScalarIndexNgram",
		FeatureScalarIndexJSONFlat:      "ScalarIndexJsonFlat",
		FeatureFilterIndexDeclined:      "FilterIndexDeclined",
		FeatureExprCacheHit:             "ExprCacheHit",
		FeatureInterimIndexSearch:       "InterimIndexSearch",
		FeatureStrictGroupSizeEffective: "StrictGroupSizeEffective",
		FeatureTieredStorageColdRead:    "TieredStorageColdRead",
		FeatureScalarIndexFmindex:       "ScalarIndexFmindex",
	}
	require.Len(t, goNames, len(execBitFeatures))
	for _, m := range matches {
		pos, err := strconv.Atoi(m[2])
		require.NoError(t, err)
		require.Less(t, pos, len(execBitFeatures), m[1])
		assert.Equal(t, norm(goNames[execBitFeatures[pos]]), norm(m[1]), "bit %d", pos)
	}
}

func TestSetExecBits(t *testing.T) {
	var set FeatureSet
	scalar, ok := ExecBit(FeatureFilterPathScalarIndex)
	require.True(t, ok)
	inverted, ok := ExecBit(FeatureScalarIndexInverted)
	require.True(t, ok)
	_, ok = ExecBit(FeatureGroupByField)
	assert.False(t, ok)

	set.SetExecBits(scalar | inverted | 1<<63)
	assert.Equal(t, []Feature{FeatureFilterPathScalarIndex, FeatureScalarIndexInverted}, set.Features())

	var nilSet *FeatureSet
	nilSet.SetExecBits(scalar)
}
