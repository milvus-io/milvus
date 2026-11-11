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
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func validateSummarySnapshot(snapshot *snapshotstorage.SnapshotData) error {
	for _, segment := range snapshot.Segments {
		if segment.GetStorageVersion() != storage.StorageV3 || segment.GetManifestPath() == "" || segment.ManifestHasIndex == nil {
			return merr.Wrap(merr.ErrServiceUnimplemented, "snapshot is incompatible with segment index summary mode: StorageV3 and explicit manifest index inventory are required")
		}
		hasArtifact := false
		for _, index := range segment.GetIndexFiles() {
			if len(index.GetIndexFilePaths()) == 0 {
				continue
			}
			hasArtifact = true
			if index.GetIndexStorePathVersion() != indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED {
				return merr.Wrap(merr.ErrServiceUnimplemented, "snapshot is incompatible with segment index summary mode: indexes must use segment-rooted storage")
			}
		}
		if segment.GetManifestHasIndex() && !hasArtifact {
			return merr.Wrap(merr.ErrServiceUnimplemented, "snapshot is incompatible with segment index summary mode: explicit index files are missing")
		}
	}
	return nil
}
