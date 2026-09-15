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

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// normalizeLocalManifestPath resolves legacy minio-root-relative manifests to
// root/legacyPrefix/insert_log/<c>/<p>/<s>, preserving their existing location.
// Absolute paths are unchanged. This only updates the loaded view, without IO.
// Callers must restrict it to locally owned segments, not foreign snapshots.
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
	// Validate the relative prefix once, before joining it to the storage root.
	// Empty and dot prefixes both address the root directly.
	if !filepath.IsLocal(filepath.FromSlash(path.Clean(legacyPrefix))) {
		return "", merr.WrapErrDataIntegrityMsg("invalid legacy local manifest prefix %q", legacyPrefix)
	}
	expectedBase := path.Join(legacyPrefix, common.SegmentInsertLogPath, metautil.JoinIDPath(collectionID, partitionID, segmentID))
	if path.Clean(base) != expectedBase {
		return "", merr.WrapErrDataIntegrityMsg("segment %d legacy local manifest base %q does not match its storage prefix and segment identity", segmentID, base)
	}
	newBase := filepath.Join(root, filepath.FromSlash(expectedBase))
	return packed.MarshalManifestPath(filepath.ToSlash(newBase), version), nil
}
