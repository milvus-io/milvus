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

package storage

import (
	"net/url"
	"path"
	"strings"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// Copy-segment target path derivation shared by DataCoord and DataNode.
//
// A restore copies every object of a source segment to a target key that is a
// deterministic transform of the source key: the source storage root is
// rebased under the target storage root, then the collection/partition/segment
// IDs are replaced. DataNode applies this transform when it writes the objects;
// DataCoord applies it when it pre-registers the target segment so that the V3
// manifest base path is known — and reclaimable by dropped-segment GC — before
// the DataNode reports back. Both sides therefore MUST call the same function
// with the same inputs. Keep the transform here, and only here.

// RemapCopySegmentRootPath rebases sourcePath from sourceRootPath under
// targetRootPath.
//
// sourceRootPath is empty for a same-cluster restore, in which case the source
// path is returned unchanged. For an external restore it is the full URI of the
// snapshot's storage root (see DataCoord's deriveSnapshotSourceRootURI):
// sourcePath must be located under it — same bucket, endpoint and canonical
// scheme when both are complete URIs — and the part of sourcePath below the
// root is re-rooted under targetRootPath, the target cluster's local storage
// root.
func RemapCopySegmentRootPath(sourcePath, sourceRootPath, targetRootPath string) (string, error) {
	rawSourceRoot := strings.TrimSpace(sourceRootPath)
	if rawSourceRoot == "" {
		return sourcePath, nil
	}

	rootBucket, rootObject, rootEndpoint, err := ParseForeignRootURI(rawSourceRoot)
	if err != nil {
		return "", merr.Wrap(err, "invalid copy segment source root")
	}
	pathBucket, pathObject, pathEndpoint, err := ParseForeignURI(sourcePath)
	if err != nil {
		return "", merr.Wrap(err, "invalid snapshot file path")
	}

	rootURI, err := url.Parse(rawSourceRoot)
	if err != nil {
		return "", merr.WrapErrDataIntegrity(err, "invalid copy segment source root")
	}
	pathURI, err := url.Parse(sourcePath)
	if err != nil {
		return "", merr.WrapErrDataIntegrity(err, "invalid snapshot file path")
	}
	rootIsCompleteURI := rootURI.Scheme != "" && rootURI.Host != ""
	pathIsCompleteURI := pathURI.Scheme != "" && pathURI.Host != ""
	if rootIsCompleteURI && pathIsCompleteURI &&
		(CanonicalForeignScheme(rootURI.Scheme) != CanonicalForeignScheme(pathURI.Scheme) ||
			rootBucket != pathBucket ||
			!strings.EqualFold(rootEndpoint, pathEndpoint)) {
		return "", merr.WrapErrDataIntegrityMsg(
			"snapshot file URI %q does not match source root %q",
			RedactSnapshotObjectPath(sourcePath),
			RedactSnapshotObjectPath(rawSourceRoot),
		)
	}

	rootObject = strings.Trim(rootObject, "/")
	pathObject = strings.Trim(pathObject, "/")
	relativePath := pathObject
	if rootObject != "" {
		switch {
		case pathObject == rootObject:
			relativePath = ""
		case strings.HasPrefix(pathObject, rootObject+"/"):
			relativePath = strings.TrimPrefix(pathObject, rootObject+"/")
		default:
			return "", merr.WrapErrDataIntegrityMsg(
				"snapshot file path %q is outside source root %q",
				RedactSnapshotObjectPath(sourcePath),
				RedactSnapshotObjectPath(rawSourceRoot),
			)
		}
	}

	targetRoot := strings.Trim(targetRootPath, "/")
	if targetRoot == "" {
		return relativePath, nil
	}
	if relativePath == "" {
		return targetRoot, nil
	}
	return path.Join(targetRoot, relativePath), nil
}

// TransformCopySegmentPath derives the target object key of a copied
// segment-scoped file: the source root is remapped (RemapCopySegmentRootPath),
// then the collection/partition/segment IDs are replaced.
//
// Segment path format: {root}/{log_type}/{collectionID}/{partitionID}/{segmentID}/...
// Example: files/insert_log/111/222/333/444/555.log -> files/insert_log/aaa/bbb/ccc/444/555.log
func TransformCopySegmentPath(
	sourcePath, sourceRootPath, targetRootPath string,
	targetCollectionID, targetPartitionID, targetSegmentID int64,
) (string, error) {
	remapped, err := RemapCopySegmentRootPath(sourcePath, sourceRootPath, targetRootPath)
	if err != nil {
		return "", err
	}
	targetPath, err := metautil.ReplaceSegmentIDsInPath(remapped, targetCollectionID, targetPartitionID, targetSegmentID)
	if err != nil {
		return "", merr.WrapErrParameterInvalidMsg("%s", err.Error())
	}
	return targetPath, nil
}

// TransformCopySegmentManifestPath derives the target segment's V3 manifest
// path from the source segment's: the JSON is unmarshaled, its base_path is
// run through TransformCopySegmentPath, and the result is marshaled back with
// the same version.
func TransformCopySegmentManifestPath(
	manifestPath, sourceRootPath, targetRootPath string,
	targetCollectionID, targetPartitionID, targetSegmentID int64,
) (string, error) {
	basePath, version, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", merr.Wrap(err, "failed to unmarshal manifest path")
	}
	targetBasePath, err := TransformCopySegmentPath(basePath, sourceRootPath, targetRootPath,
		targetCollectionID, targetPartitionID, targetSegmentID)
	if err != nil {
		return "", merr.Wrap(err, "failed to generate target base path")
	}
	return packed.MarshalManifestPath(targetBasePath, version), nil
}
