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
	"context"

	qcmeta "github.com/milvus-io/milvus/internal/querycoordv2/meta"
)

// Adapters bind the bump_defence core's small interfaces to the real coordinator
// subsystems. They are intentionally thin: the DistReader wraps querycoord's
// DistributionManager directly; the scope reader and proxy pusher take closures so this
// file does not import datacoord / proxyutil (mixCoord supplies those closures).

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

// funcScopeReader adapts a closure to SegmentScopeReader. mixCoord supplies a closure
// over datacoord meta (stale = schema_version < watermark).
type funcScopeReader struct {
	fn func(collectionID int64, watermark int32) []int64
}

// NewFuncScopeReader builds a SegmentScopeReader from a closure (nil-safe: a nil fn
// yields no stale segments).
func NewFuncScopeReader(fn func(collectionID int64, watermark int32) []int64) SegmentScopeReader {
	return &funcScopeReader{fn: fn}
}

func (a *funcScopeReader) StaleSegments(collectionID int64, watermark int32) []int64 {
	if a.fn == nil {
		return nil
	}
	return a.fn(collectionID, watermark)
}

// funcPusher adapts a closure to ProxyGatePusher. mixCoord supplies a closure over the
// proxy client manager (invalidate collection meta cache so proxies re-pull the gated
// field set).
type funcPusher struct {
	fn func(ctx context.Context, collectionID int64, fieldIDs []int64) error
}

// NewFuncPusher builds a ProxyGatePusher from a closure.
func NewFuncPusher(fn func(ctx context.Context, collectionID int64, fieldIDs []int64) error) ProxyGatePusher {
	return &funcPusher{fn: fn}
}

func (a *funcPusher) PushGatedFields(ctx context.Context, collectionID int64, fieldIDs []int64) error {
	if a.fn == nil {
		return nil
	}
	return a.fn(ctx, collectionID, fieldIDs)
}
