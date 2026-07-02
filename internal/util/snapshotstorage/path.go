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

package snapshotstorage

import (
	"net/url"
	"path"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func ParseForeignURI(raw string) (bucket, objectKey, endpointHost string, err error) {
	if strings.TrimSpace(raw) == "" {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri is empty")
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", "", "", merr.WrapErrParameterInvalidMsg("invalid foreign_uri: %s", err.Error())
	}
	if u.User != nil {
		return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must not embed credentials")
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme == "" {
		parts, err := cleanURIPathSegments(u.EscapedPath())
		if err != nil {
			return "", "", "", err
		}
		if len(parts) == 0 {
			return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include object key")
		}
		return "", joinObjectKeySegments(parts), "", nil
	}
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
		if len(parts) < 2 {
			return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include bucket and object key")
		}
		return parts[0], joinObjectKeySegments(parts[1:]), u.Host, nil
	case "az", "azure":
		if len(parts) < 2 {
			return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include container and object key")
		}
		return parts[0], joinObjectKeySegments(parts[1:]), u.Host, nil
	default:
		if externalspec.IsCloudEndpointHost(u.Host) {
			if len(parts) < 2 {
				return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include bucket and object key")
			}
			return parts[0], joinObjectKeySegments(parts[1:]), u.Host, nil
		}
		if len(parts) == 0 {
			return "", "", "", merr.WrapErrParameterInvalidMsg("foreign_uri must include object key")
		}
		return u.Host, joinObjectKeySegments(parts), "", nil
	}
}

func DeriveForeignRoot(direction Direction, objectKey string) (string, error) {
	objectKey = strings.Trim(objectKey, "/")
	if direction == DirectionExport || direction == DirectionCopySource {
		return objectKey, nil
	}

	root, ok := deriveRestoreBundleRoot(objectKey)
	if !ok {
		return "", merr.WrapErrParameterInvalidMsg(
			"restore metadata URI must contain snapshots/{collectionID}/metadata/{snapshotID}",
		)
	}
	return root, nil
}

func deriveRestoreBundleRoot(objectKey string) (string, bool) {
	segments := strings.Split(strings.Trim(objectKey, "/"), "/")
	for i := 0; i < len(segments); i++ {
		if segments[i] != "snapshots" {
			continue
		}
		if i+3 >= len(segments) {
			continue
		}
		if segments[i+2] != "metadata" {
			continue
		}
		return joinObjectKeySegments(segments[:i]), true
	}
	return "", false
}

func joinObjectKeySegments(segments []string) string {
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

func cleanURIPathSegments(escapedPath string) ([]string, error) {
	unescaped, err := url.PathUnescape(escapedPath)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("invalid foreign_uri path escape: %s", err.Error())
	}
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
