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

package datacoord

import (
	"path"
	"path/filepath"
	"strings"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// normalizeLocalManifestPath resolves a persisted local StorageV3 manifest into
// the complete filesystem path used by the in-memory segment view. Callers must
// restrict this compatibility rule to locally owned data, not remote storage or
// self-contained / foreign snapshots. It performs no storage or catalog writes.
//
// Milvus 3.0.0 / 3.0.1 built the manifest base of a flushed segment from
// minio.rootPath (`files/insert_log/<c>/<p>/<s>`, #53052) and placed its files
// under <localStorage.path>/files/insert_log/... through a filesystem rooted at
// localStorage.path. Resolve that base to its existing absolute location,
// preserving the legacy prefix; these files are read in place, not relocated
// to the <localStorage.path>/insert_log/... layout used by new segments.
// Compaction-written manifests were already absolute and are left alone. A
// later normal segment update may persist the resolved path; otherwise each
// reload resolves the legacy value again without an eager metadata migration.
func normalizeLocalManifestPath(manifestPath, root, legacyPrefix string, collectionID, partitionID, segmentID int64) (string, error) {
	if manifestPath == "" {
		return manifestPath, nil
	}
	base, version, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", merr.WrapErrDataIntegrity(err, "segment %d has an invalid local manifest path %q", segmentID, manifestPath)
	}
	if path.IsAbs(base) {
		return manifestPath, nil
	}
	if !filepath.IsAbs(root) {
		return "", merr.WrapErrParameterInvalidMsg("localStorage.path %q must be an absolute path", root)
	}
	// minio.rootPath is normally normalized by paramtable. An empty or dot
	// prefix denotes the bucket root; no arbitrary relative-path fallback exists.
	if path.IsAbs(legacyPrefix) || strings.Contains(legacyPrefix, "://") {
		return "", merr.WrapErrDataIntegrityMsg("invalid legacy local manifest prefix %q", legacyPrefix)
	}
	suffix := path.Join(common.SegmentInsertLogPath, metautil.JoinIDPath(collectionID, partitionID, segmentID))
	if path.Clean(base) != path.Join(legacyPrefix, suffix) {
		return "", merr.WrapErrDataIntegrityMsg("segment %d legacy local manifest base %q does not match its storage prefix and segment identity", segmentID, base)
	}
	// Reconstruct from the validated prefix and trusted IDs. Keeping the prefix
	// is essential: the old rooted filesystem placed the files at root/base.
	newBase := filepath.Join(root, filepath.FromSlash(path.Join(legacyPrefix, suffix)))
	rel, err := filepath.Rel(root, newBase)
	if err != nil || rel == "." || !filepath.IsLocal(rel) {
		return "", merr.WrapErrDataIntegrityMsg("segment %d resolved local manifest base %q is outside storage root %q", segmentID, newBase, root)
	}
	return packed.MarshalManifestPath(filepath.ToSlash(newBase), version), nil
}
