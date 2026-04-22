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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func TestCachedSegmentsInfo_TombstoneBlocksStaleWrite(t *testing.T) {
	cache := NewCachedSegmentsInfo()
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1}}

	cache.SetSegment(1, seg, 10)
	cache.DropSegment(1, 20)

	cache.SetSegment(1, seg, 15)
	assert.Nil(t, cache.GetSegment(1), "stale write below tombstone version must be rejected")
}

func TestCachedSegmentsInfo_PruneRemovesTombstone(t *testing.T) {
	cache := NewCachedSegmentsInfo()
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 2}}

	cache.SetSegment(2, seg, 10)
	cache.DropSegment(2, 20)
	cache.PruneSegment(2)

	// After prune, a fresh insert at any version succeeds — the tombstone is gone.
	cache.SetSegment(2, seg, 1)
	assert.NotNil(t, cache.GetSegment(2), "insert after prune must succeed, tombstone should be cleared")
}
