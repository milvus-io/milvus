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

package coordinator

import (
	qcmeta "github.com/milvus-io/milvus/internal/querycoordv2/meta"
)

// Adapters bind the bump_defence core's small interfaces to the real coordinator
// subsystems. They are intentionally thin: the DistReader wraps querycoord's
// SegmentDistManager directly; the DataViewReader is satisfied structurally by datacoord's
// Server; the ProxyGatePusher is satisfied by mixCoordImpl itself (PushGatedFields in
// mix_coord.go).

// distReaderAdapter exposes the querynode dist report (querycoord) as a DistReader.
// The witness is read purely from dist: the embedded *datapb.SegmentInfo carries
// schema_version; the dist Segment carries the loaded manifest path (v3) / data version (v2).
type distReaderAdapter struct {
	dist qcmeta.SegmentDistManagerInterface
}

// NewDistReaderAdapter wraps a querycoord SegmentDistManager -- the only dist slice the
// readiness witness reads (no channel dist, no write methods).
func NewDistReaderAdapter(dist qcmeta.SegmentDistManagerInterface) DistReader {
	return &distReaderAdapter{dist: dist}
}

func (a *distReaderAdapter) SegmentDist(collectionID int64) []SegmentDistInfo {
	segs := a.dist.GetByFilter(qcmeta.WithCollectionID(collectionID))
	out := make([]SegmentDistInfo, 0, len(segs))
	for _, s := range segs {
		// Version and ManifestPath feed the version witness. Storage-v2 segments report a
		// numeric DataVersion; storage-v3 segments carry a manifest path (string, encoding
		// the version) instead. Both are reported by the querynode dist; the witness
		// dispatches on whichever the segment carries.
		var version int64
		if s.DataVersion != nil {
			version = int64(*s.DataVersion)
		}
		out = append(out, SegmentDistInfo{
			SegmentID:     s.GetID(),
			Version:       version,
			ManifestPath:  s.ManifestPath,
			SchemaVersion: s.GetSchemaVersion(),
		})
	}
	return out
}
