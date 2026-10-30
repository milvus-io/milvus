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

package format

import (
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ParseObjectURI parses a snapshot object key or supported cloud URI.
func ParseObjectURI(raw string, allowEmptyObjectKey bool) (bucket, objectKey, endpointHost string, err error) {
	if strings.TrimSpace(raw) == "" {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri is empty")
	}
	if !strings.Contains(raw, "://") {
		if err := ValidateObjectKey(raw); err != nil {
			return "", "", "", err
		}
		return "", raw, "", nil
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", "", "", merr.WrapErrParameterInvalidMsg("invalid foreign_uri: %s", err.Error())
	}
	if u.User != nil {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must not embed credentials")
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must not include query parameters or fragments")
	}

	scheme := strings.ToLower(u.Scheme)
	if !isSupportedForeignScheme(scheme) {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri scheme %q is not supported", scheme)
	}
	if u.Host == "" {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include a bucket or endpoint host")
	}

	parts, err := cleanURIPathSegments(u.EscapedPath())
	if err != nil {
		return "", "", "", err
	}

	switch scheme {
	case "minio", "http", "https":
		minParts := 2
		if allowEmptyObjectKey {
			minParts = 1
		}
		if len(parts) < minParts {
			return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include bucket and object key")
		}
		return parts[0], joinURIPathSegments(parts[1:]), u.Host, nil
	case "az", "azure":
		minParts := 2
		if allowEmptyObjectKey {
			minParts = 1
		}
		if len(parts) < minParts {
			return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include container and object key")
		}
		return parts[0], joinURIPathSegments(parts[1:]), u.Host, nil
	default:
		if externalspec.IsCloudEndpointHost(u.Host) {
			minParts := 2
			if allowEmptyObjectKey {
				minParts = 1
			}
			if len(parts) < minParts {
				return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include bucket and object key")
			}
			return parts[0], joinURIPathSegments(parts[1:]), u.Host, nil
		}
		if len(parts) == 0 && !allowEmptyObjectKey {
			return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include object key")
		}
		return u.Host, joinURIPathSegments(parts), "", nil
	}
}

// SnapshotBundleAnchor extracts the terminal snapshot metadata anchor.
func SnapshotBundleAnchor(objectKey string) (root string, collectionID, snapshotID int64, ok bool) {
	hasLeadingSlash := strings.HasPrefix(objectKey, "/")
	segments := strings.Split(strings.Trim(objectKey, "/"), "/")
	if len(segments) < 4 {
		return "", 0, 0, false
	}
	anchor := len(segments) - 4
	if segments[anchor] != "snapshots" || segments[anchor+2] != "metadata" {
		return "", 0, 0, false
	}
	collectionID, err := strconv.ParseInt(segments[anchor+1], 10, 64)
	if err != nil || collectionID <= 0 {
		return "", 0, 0, false
	}
	metadataFile := segments[anchor+3]
	if !strings.HasSuffix(metadataFile, ".json") {
		return "", 0, 0, false
	}
	snapshotID, err = strconv.ParseInt(strings.TrimSuffix(metadataFile, ".json"), 10, 64)
	if err != nil || snapshotID <= 0 {
		return "", 0, 0, false
	}
	root = joinURIPathSegments(segments[:anchor])
	if hasLeadingSlash && root != "" {
		root = "/" + root
	}
	return root, collectionID, snapshotID, true
}

func joinURIPathSegments(segments []string) string {
	if len(segments) == 0 {
		return ""
	}
	return path.Join(segments...)
}

func isSupportedForeignScheme(scheme string) bool {
	switch scheme {
	case "s3", "minio", "gs", "gcs", "az", "azure", "http", "https":
		return true
	default:
		return false
	}
}

// ValidateObjectKey rejects traversal components without rewriting object keys.
func ValidateObjectKey(objectKey string) error {
	for _, part := range strings.Split(strings.Trim(objectKey, "/"), "/") {
		if part == "." || part == ".." {
			return merr.WrapErrParameterInvalidMsg("foreign_uri object key must not contain path traversal")
		}
	}
	return nil
}

func cleanURIPathSegments(escapedPath string) ([]string, error) {
	unescaped, err := url.PathUnescape(escapedPath)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("invalid foreign_uri path escape: %s", err.Error())
	}
	// Reject traversal on raw URI path segments before path.Join can normalize
	// them away and silently point the snapshot reader at a different object key.
	rawParts := strings.Split(strings.Trim(unescaped, "/"), "/")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		if part == "" {
			continue
		}
		if part == "." || part == ".." {
			return nil, merr.WrapErrParameterInvalidMsg("foreign_uri object key must not contain path traversal")
		}
		parts = append(parts, part)
	}
	return parts, nil
}

// NormalizeObjectPath validates a manifest path and removes its transport URI.
func NormalizeObjectPath(src string) (string, error) {
	if src == "" {
		return "", nil
	}
	_, key, _, err := ParseObjectURI(src, false)
	if err != nil {
		return "", merr.WrapErrDataIntegrityMsg("invalid snapshot object path")
	}
	return key, nil
}

// RebaseObjectPath relocates a normalized object key between bundle roots.
// Only a complete root prefix is replaced; the bundle's internal layout stays
// unchanged. An empty root denotes the bucket root. Callers normalize URIs and
// trim trailing slashes from roots before calling this shared reader rule.
func RebaseObjectPath(src, oldRoot, newRoot string) string {
	if oldRoot == newRoot {
		return src
	}
	if src == oldRoot {
		return newRoot
	}
	if oldRoot == "" {
		return newRoot + "/" + strings.TrimLeft(src, "/")
	}
	if strings.HasPrefix(src, oldRoot+"/") {
		relative := strings.TrimPrefix(src, oldRoot+"/")
		if newRoot == "" {
			return relative
		}
		return newRoot + "/" + relative
	}
	return src
}

// ExportedSnapshotObjectPath maps normalized source keys into a bundle's files directory.
func ExportedSnapshotObjectPath(sourceRoot, src, bundleRoot string) string {
	root := strings.TrimRight(sourceRoot, "/")
	relative := src
	if root != "" {
		if src == root {
			relative = ""
		} else if strings.HasPrefix(src, root+"/") {
			relative = strings.TrimPrefix(src, root+"/")
		}
	}
	return path.Join(bundleRoot, "files", relative)
}
