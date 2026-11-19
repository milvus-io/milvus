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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestAtomicAdoptionRejectsOversizedTxnBeforeWriting(t *testing.T) {
	for _, largePayload := range []bool{false, true} {
		t.Run(map[bool]string{false: "ops", true: "bytes"}[largePayload], func(t *testing.T) {
			kv := mocks.NewMetaKv(t)
			if !largePayload {
				kv.EXPECT().MaxTxnOps().Return(1).Maybe()
			}
			// No persistence expectations: a chunked write would fail this test.
			catalog := NewCatalog(kv, "", "")
			segment := &datapb.SegmentInfo{ID: 1, CollectionID: 2, PartitionID: 3, State: commonpb.SegmentState_Flushed, InsertChannel: "target"}
			if largePayload {
				segment.InsertChannel = strings.Repeat("x", 1<<20)
			}
			err := catalog.Update(context.Background(), metastore.UpdateSegment(segment), metastore.UpdateAction{Type: metastore.ActionAdd, Entry: metastore.DataViewEntry{Atomic: true, DataView: &viewpb.DataViewOfCollection{CollectionId: 2, DataVersion: &viewpb.DataVersion{CompactVersion: 1}}}})
			require.Error(t, err)
		})
	}
}
