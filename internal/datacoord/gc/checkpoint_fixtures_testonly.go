//go:build test || dynamic

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

package gc

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore"
	kvstreamingnode "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func MockCheckpointCatalog(t *testing.T, tick uint64) metastore.StreamingNodeCataLog {
	t.Helper()
	catalog := kvstreamingnode.NewCataLog(nil)
	patch := mockey.Mock(mockey.GetMethod(catalog, "GetConsumeCheckpoint")).Return(&streamingpb.WALCheckpoint{TimeTick: tick}, nil).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return catalog
}

func (gc *GarbageCollector) CheckDroppedSegmentGCForTest(segment *meta.SegmentInfo, compactTo *meta.SegmentInfo, channelCP typeutil.UniqueSet, expire uint64) bool {
	return gc.checkDroppedSegmentGC(segment, compactTo, channelCP, expire)
}

func (gc *GarbageCollector) DataViewCompactionProtectionForTest(segments []*meta.SegmentInfo) map[int64]struct{} {
	return gc.dataViewCompactionProtection(segments)
}

func (gc *GarbageCollector) ContextForTest() context.Context { return gc.ctx }
