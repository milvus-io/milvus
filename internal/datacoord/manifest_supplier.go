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
	"context"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetPublishedManifestPaths snapshots current segment manifest descriptors across
// all collections, including unloaded ones. It enumerates live segment references,
// not snapshot references (snapshot-protected dropped segments are filtered out).
// Callers resolve/stat descriptors only after releasing segMu.
func (s *Server) GetPublishedManifestPaths(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return nil, err
	}
	if s.meta == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("DataCoord metadata unavailable")
	}
	s.meta.segMu.RLock()
	defer s.meta.segMu.RUnlock()
	manifests := make([]string, 0)
	for _, segment := range s.meta.segments.GetSegments() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !isSegmentHealthy(segment) || segment.GetIsImporting() || segment.GetIsInvisible() || segment.GetManifestPath() == "" {
			continue
		}
		// Growing/Flushing and L0 segments can already have published manifests.
		// The resolver rejects the version-zero placeholder outside this lock.
		manifests = append(manifests, segment.GetManifestPath())
	}
	return manifests, nil
}
