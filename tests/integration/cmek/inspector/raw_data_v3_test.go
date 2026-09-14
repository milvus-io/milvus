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

package inspector

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestManifestLocatorV3RejectsAmbiguousRevision(t *testing.T) {
	for _, input := range []string{
		`{}`, `null`, `{"base_path":"root/segment"}`,
		`{"base_path":"root/segment","ver":0}`,
		`{"base_path":"root/segment","ver":-1}`,
		`{"base_path":"root/../segment","ver":2}`,
		`{"base_path":"root/segment","ver":2,"ver":3}`,
		`{"base_path":"root/segment","ver":2,"key":"canary"}`,
		`{"base_path":"root/segment","ver":2} {}`,
	} {
		t.Run(input, func(t *testing.T) {
			_, err := ParseManifestLocatorV3(input)
			require.Error(t, err)
		})
	}
	locator, err := ParseManifestLocatorV3(`{"base_path":"root/segment","ver":7}`)
	require.NoError(t, err)
	require.Equal(t, "root/segment/_metadata/manifest-7.avro", locator.ObjectPath())
}

func TestLocateManifestsV3PreservesEverySegment(t *testing.T) {
	segments := []*datapb.SegmentInfo{
		{ID: 11, CollectionID: 7, NumOfRows: 4, StorageVersion: 3, ManifestPath: `{"base_path":"root/11","ver":3}`},
		{ID: 12, CollectionID: 7, NumOfRows: 4, StorageVersion: 3, ManifestPath: `{"base_path":"root/12","ver":9}`},
	}
	references, err := LocateManifestsV3(segments, 7)
	require.NoError(t, err)
	require.Len(t, references, 2)
	require.Equal(t, "root/11/_metadata/manifest-3.avro", references[0].Locator.ObjectPath())
	require.Equal(t, "root/12/_metadata/manifest-9.avro", references[1].Locator.ObjectPath())
	for _, input := range [][]*datapb.SegmentInfo{
		nil,
		{nil},
		{segments[0], segments[0]},
		{{ID: 11, CollectionID: 8, NumOfRows: 4, StorageVersion: 3, ManifestPath: segments[0].ManifestPath}},
		{{ID: 11, CollectionID: 7, NumOfRows: 4, StorageVersion: 2, ManifestPath: segments[0].ManifestPath}},
		{{ID: 11, CollectionID: 7, StorageVersion: 3, ManifestPath: segments[0].ManifestPath}},
	} {
		_, err := LocateManifestsV3(input, 7)
		require.Error(t, err)
	}
}
