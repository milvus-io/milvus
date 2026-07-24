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
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ParseForeignURI accepts object keys, bucket-hosted URIs, and endpoint-style
// URIs. Endpoint-style forms such as minio://host/bucket/key or
// https://host/bucket/key keep the endpoint in endpointHost and take the
// bucket/container from the first path segment.
func ParseForeignURI(raw string) (bucket, objectKey, endpointHost string, err error) {
	return parseForeignURI(raw, false)
}

// ParseForeignRootURI is equivalent to ParseForeignURI, but complete URIs may
// identify the bucket/container root without an object key.
func ParseForeignRootURI(raw string) (bucket, objectKey, endpointHost string, err error) {
	return parseForeignURI(raw, true)
}

func parseForeignURI(raw string, allowEmptyObjectKey bool) (bucket, objectKey, endpointHost string, err error) {
	if strings.TrimSpace(raw) == "" {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri is empty")
	}
	if !hasURITransportScheme(raw) {
		if err := validateRawObjectKey(raw); err != nil {
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
	case "minio", "https":
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

func DeriveForeignRoot(direction Direction, objectKey string) (string, error) {
	objectKey = strings.Trim(objectKey, "/")
	if direction == DirectionExport || direction == DirectionCopySource {
		return objectKey, nil
	}

	// Restore has no extra API parameter for the bundle root. Derive it from
	// the canonical snapshots/{collectionID}/metadata/{snapshotID} anchor in
	// the metadata URI, and reject arbitrary layouts that do not expose it.
	root, _, _, ok := deriveSnapshotBundleAnchor(objectKey)
	if !ok {
		return "", merr.WrapErrParameterInvalidMsg(
			"restore metadata URI must end with snapshots/{collectionID}/metadata/{snapshotID}.json",
		)
	}
	return root, nil
}

func deriveSnapshotBundleAnchor(objectKey string) (root string, collectionID, snapshotID int64, ok bool) {
	hasLeadingSlash := strings.HasPrefix(objectKey, "/")
	segments := strings.Split(strings.Trim(objectKey, "/"), "/")
	if len(segments) < 4 {
		return "", 0, 0, false
	}
	anchor := len(segments) - 4
	if segments[anchor] != SnapshotRootPath || segments[anchor+2] != SnapshotMetadataSubPath {
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
	case "s3", "minio", "gs", "gcs", "az", "azure", "https":
		return true
	default:
		return false
	}
}

// CanonicalForeignScheme returns one stable identity for supported scheme aliases.
func CanonicalForeignScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "gcs":
		return "gs"
	case "az":
		return "azure"
	default:
		return strings.ToLower(strings.TrimSpace(scheme))
	}
}

func hasURITransportScheme(raw string) bool {
	return strings.Contains(raw, "://")
}

func validateRawObjectKey(objectKey string) error {
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
