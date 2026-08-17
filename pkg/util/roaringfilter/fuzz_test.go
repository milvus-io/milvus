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

package roaringfilter

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// FuzzParse feeds arbitrary bytes to the envelope and body parsers. A blob
// arrives from a client, so no input may panic, and a rejection must be a typed
// parameter error rather than a runtime failure.
//
// Validate must additionally agree with Parse: anything Validate accepts must
// parse, and the cardinality it reports without building a bitmap must match
// the one the decoded bitmap has. That equivalence is what lets Proxy admit a
// request from Validate alone.
func FuzzParse(f *testing.F) {
	for _, values := range [][]int64{
		{},
		{0},
		{-1, 0, 1},
		{math.MinInt64, -1, 0, 1, 42, math.MaxInt64},
		{1, 5, 9, 4242},
	} {
		blob, err := Build(values)
		require.NoError(f, err)
		f.Add(blob)
	}
	// Structurally interesting non-blobs.
	f.Add([]byte{})
	f.Add([]byte("MRB1"))
	f.Add(make([]byte, HeaderSize))

	f.Fuzz(func(t *testing.T, blob []byte) {
		summary, validateErr := Validate(blob)
		filter, parseErr := Parse(blob)

		if validateErr != nil {
			// Validate is the admission gate; when it rejects, Parse must not
			// succeed, or Proxy would refuse a request QueryNode would accept.
			require.Error(t, parseErr, "Validate rejected but Parse accepted")
			require.Nil(t, filter)
			return
		}

		require.NoError(t, parseErr, "Validate accepted but Parse rejected")
		require.NotNil(t, filter)
		require.Equal(t, summary.Cardinality, filter.Cardinality(),
			"allocation-free cardinality disagrees with the decoded bitmap")
	})
}
