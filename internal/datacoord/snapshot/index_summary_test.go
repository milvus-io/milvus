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

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/require"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSummarySnapshotCompatibility(t *testing.T) {
	empty := false
	segment := &datapb.SegmentDescription{StorageVersion: storage.StorageV3, ManifestPath: "manifest", ManifestHasIndex: &empty}
	data := &snapshotstorage.SnapshotData{Segments: []*datapb.SegmentDescription{segment}}
	require.NoError(t, validateSummarySnapshot(data))
	segment.StorageVersion = storage.StorageV2
	require.ErrorIs(t, validateSummarySnapshot(data), merr.ErrServiceUnimplemented)
	segment.StorageVersion = storage.StorageV3
	segment.ManifestHasIndex = nil
	require.ErrorIs(t, validateSummarySnapshot(data), merr.ErrServiceUnimplemented)
	nonempty := true
	segment.ManifestHasIndex = &nonempty
	require.ErrorIs(t, validateSummarySnapshot(data), merr.ErrServiceUnimplemented)
	segment.IndexFiles = []*indexpb.IndexFilePathInfo{{IndexFilePaths: []string{"index"}}}
	require.ErrorIs(t, validateSummarySnapshot(data), merr.ErrServiceUnimplemented)
	segment.IndexFiles[0].IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	require.NoError(t, validateSummarySnapshot(data))
}
