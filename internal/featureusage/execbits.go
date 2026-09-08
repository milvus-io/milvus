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

// execBitFeatures maps a bit of the execution feature set segcore records to
// its counter: bit i is execBitFeatures[i]. The order is the wire format
// shared with segcore's FeatureBit enum in
// internal/core/src/common/FeatureBits.h; TestExecBitsMatchSegcore parses
// that header and fails when the two disagree. Append only: a bit keeps its
// meaning across versions, so a QueryNode and a Proxy of different builds
// still agree during a rolling upgrade.
var execBitFeatures = [...]Feature{
	FeatureFilterPathScalarIndex,
	FeatureFilterPathPkIndex,
	FeatureFilterPathTextMatchIndex,
	FeatureFilterPathJSONShredding,
	FeatureFilterPathNgramIndex,
	FeatureFilterPathBruteForce,
	FeatureScalarIndexBitmap,
	FeatureScalarIndexStlSort,
	FeatureScalarIndexTrie,
	FeatureScalarIndexInverted,
	FeatureScalarIndexHybrid,
	FeatureScalarIndexRtree,
	FeatureScalarIndexNgram,
	FeatureScalarIndexJSONFlat,
	FeatureFilterIndexDeclined,
	FeatureExprCacheHit,
	FeatureInterimIndexSearch,
	FeatureStrictGroupSizeEffective,
	// Set by the QueryNode in Go, from the storage cost of the request before
	// it is split across merged requests: the split rounds small byte counts
	// down to zero, so the Proxy cannot derive it reliably.
	FeatureTieredStorageColdRead,
	FeatureScalarIndexFmindex,
}

// SetExecBits marks the features of an execution feature bit set. Bits this
// build does not know, from a newer QueryNode, are ignored.
func (s *FeatureSet) SetExecBits(bits uint64) {
	if s == nil {
		return
	}
	for i, f := range execBitFeatures {
		if bits&(1<<uint(i)) != 0 {
			s.Set(f)
		}
	}
}

// ExecBit returns the bit that stands for f in the execution feature set,
// and false for a feature segcore does not report.
func ExecBit(f Feature) (uint64, bool) {
	for i, g := range execBitFeatures {
		if g == f {
			return 1 << uint(i), true
		}
	}
	return 0, false
}
