// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshotio

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSegmentManifestBoundsOCFMetadataMaps(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first int64
		next  int64
	}{
		{name: "single oversized block", next: 100},
		{name: "cumulative chunked blocks", first: 6, next: 14},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(ocfMagic)
			if tc.first > 0 {
				data = binary.AppendVarint(data, tc.first)
				for range tc.first {
					data = append(data, 0, 0) // Empty metadata key and bytes value.
				}
			}
			data = binary.AppendVarint(data, tc.next)
			_, err := ParseSegmentManifest(data, SelfDescribingManifestVersion)
			require.ErrorContains(t, err, "Config.MaxMapAllocSize")
		})
	}
}
