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
	open := strings.IndexByte(enum, '{')
	require.GreaterOrEqual(t, open, 0)
	enum = enum[open+1:]

	re := regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+),$`)

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
		FeatureScalarIndexRtree:         "ScalarIndexRtree",
		FeatureScalarIndexNgram:         "ScalarIndexNgram",
		FeatureFilterIndexDeclined:      "FilterIndexDeclined",
		FeatureExprCacheHit:             "ExprCacheHit",
		FeatureInterimIndexSearch:       "InterimIndexSearch",
		FeatureStrictGroupSizeEffective: "StrictGroupSizeEffective",
		FeatureTieredStorageColdRead:    "TieredStorageColdRead",
		FeatureScalarIndexFmindex:       "ScalarIndexFmindex",
		FeatureTwoStageSearch:           "TwoStageSearch",
		FeatureSegmentPrune:             "SegmentPrune",
	}
	retiredNames := map[int]string{10: "RetiredScalarIndexHybrid", 13: "RetiredScalarIndexJsonFlat"}
	require.Len(t, goNames, len(execBitFeatures)-len(retiredNames))
	// Retired positions remain reserved in Go even if C++ no longer names them.
	for pos := range retiredNames {
		require.Equal(t, execBitRetired, execBitFeatures[pos], "retired bit %d was reused in Go", pos)
	}
	seen := make(map[int]bool, len(execBitFeatures))
	for _, declaration := range strings.Split(enum, "\n") {
		declaration = strings.TrimSpace(strings.SplitN(declaration, "//", 2)[0])
		if declaration == "" {
			continue
		}
		m := re.FindStringSubmatch(declaration)
		require.Len(t, m, 3, "unrecognized FeatureBit declaration %q", declaration)
		pos, err := strconv.Atoi(m[2])
		require.NoError(t, err)
		require.Less(t, pos, len(execBitFeatures), m[1])
		require.False(t, seen[pos], "duplicate C++ bit %d", pos)
		seen[pos] = true
		if execBitFeatures[pos] == execBitRetired {
			assert.Equal(t, norm(retiredNames[pos]), norm(m[1]), "retired bit %d", pos)
		} else {
			assert.Equal(t, norm(goNames[execBitFeatures[pos]]), norm(m[1]), "bit %d", pos)
		}
	}
	// Every active bit must exist at the same position in both definitions;
	// omitted retired entries do not reduce that coverage requirement.
	for pos, feature := range execBitFeatures {
		if feature != execBitRetired {
			assert.True(t, seen[pos], "segcore is missing active bit %d (%s)", pos, goNames[feature])
		}
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

	set.SetExecBits(scalar | inverted | 1<<13 | 1<<63)
	assert.Equal(t, []Feature{FeatureFilterPathScalarIndex, FeatureScalarIndexInverted}, set.Features())

	var nilSet *FeatureSet
	nilSet.SetExecBits(scalar)
}
