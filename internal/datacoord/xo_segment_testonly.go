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

//go:build test

package datacoord

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// setTestRowCount writes a segment's NumOfRows directly. The production
// per-field setters were retired with their last production callers; tests that
// simulate a growing segment filling up still need the poke, so it lives here
// rather than widening the production surface again.
func setTestRowCount(m *meta, segmentID UniqueID, rowCount int64) {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	m.segments.modifyProto(segmentID, func(seg *datapb.SegmentInfo) { seg.NumOfRows = rowCount })
}
