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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestL0PublicationPreservesPrefixAcrossPartialWrites(t *testing.T) {
	for _, failureAt := range []int{1, 2, 3, 0} {
		t.Run(string(rune('0'+failureAt)), func(t *testing.T) {
			ctx := t.Context()
			kv := newCatalogCompactionTargetMemoryKV()
			catalog := NewCatalog(kv, "", "")
			target := &datapb.SegmentInfo{ID: 20, CollectionID: 100, PartitionID: 1, Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, TransformStartAfterTimetick: 50}
			input := &datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 1, Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed}
			require.NoError(t, catalog.AddSegment(ctx, target))
			require.NoError(t, catalog.AddSegment(ctx, input))
			target.TransformStartAfterTimetick = 200
			target.Deltalogs = []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 9001, EntriesNum: 1}}}}
			input.State = commonpb.SegmentState_Dropped
			input.Compacted = true
			limit := mockey.Mock(mockey.GetMethod(kv, "MaxTxnOps")).Return(1).Build()
			defer limit.UnPatch()
			failure := merr.WrapErrServiceUnavailableMsg("injected batch failure")
			writes := 0
			write := mockey.Mock((*memkv.MemoryKV).MultiSave).To(func(_ *memkv.MemoryKV, ctx context.Context, values map[string]string) error {
				writes++
				if failureAt != 0 && writes == failureAt {
					return failure
				}
				for key, value := range values {
					require.NoError(t, kv.Save(ctx, key, value))
				}
				return nil
			}).Build()
			defer write.UnPatch()
			err := catalog.AlterSegments(ctx, []*datapb.SegmentInfo{input, target}, metastore.BinlogsIncrement{Segment: target})
			if failureAt == 0 {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, failure)
			}
			// Read durable records directly, as a new Coordinator would after a
			// crash. Never infer success from this process's in-memory state.
			read := func(segment *datapb.SegmentInfo) *datapb.SegmentInfo {
				data, err := kv.Load(ctx, buildSegmentPath(100, 1, segment.GetID()))
				require.NoError(t, err)
				result := &datapb.SegmentInfo{}
				require.NoError(t, proto.Unmarshal([]byte(data), result))
				return result
			}
			published := read(target)
			if published.GetTransformStartAfterTimetick() == 200 {
				_, values, err := kv.LoadWithPrefix(ctx, buildFieldDeltalogPathPrefix(100, 1, 20))
				require.NoError(t, err)
				require.Len(t, values, 1, "new cursor must never precede its deltalog")
			}
			if read(input).GetState() == commonpb.SegmentState_Dropped {
				require.EqualValues(t, 200, published.GetTransformStartAfterTimetick(), "input retirement must follow every target")
			}
			write.UnPatch()
			require.NoError(t, catalog.AlterSegments(ctx, []*datapb.SegmentInfo{input, target}, metastore.BinlogsIncrement{Segment: target}))
			require.EqualValues(t, 200, read(target).GetTransformStartAfterTimetick())
			require.Equal(t, commonpb.SegmentState_Dropped, read(input).GetState())
		})
	}
}
