// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestManifestIndexFileKeysRejectPathTraversal(t *testing.T) {
	_, ok := manifestIndexFilePathInfo(3, packed.ManifestIndexInfo{
		IndexName:             "index",
		IndexType:             "HNSW",
		Path:                  "index_v1/1/2/3/102/4",
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexFileKeys:         []string{"../other-build/file"},
	})
	require.False(t, ok)
}

// validateManifestIndexPublishable must apply exactly the reader predicate.
func TestValidateManifestIndexPublishable(t *testing.T) {
	good := packed.ManifestIndexInfo{
		IndexID:               500,
		BuildID:               5100,
		IndexName:             "idx",
		IndexType:             "HNSW",
		Path:                  "/tmp/test-reload/index_files/5100/1/10/5001",
		IndexFileKeys:         []string{"0"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	require.NoError(t, ValidateManifestIndexPublishable(5001, good))

	nameless := good
	nameless.IndexName = ""
	require.Error(t, ValidateManifestIndexPublishable(5001, nameless))

	pathless := good
	pathless.Path = ""
	require.Error(t, ValidateManifestIndexPublishable(5001, pathless))
}
