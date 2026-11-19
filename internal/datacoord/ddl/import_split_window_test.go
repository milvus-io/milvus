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

package ddl

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type splitWindowBroadcastAPI struct{ broadcaster.BroadcastAPI }

// An import this cluster originates is refused under the resource key of the
// collection while the collection has a shard split in its window, before
// anything is broadcast.
func TestBroadcastImportRefusedInASplitWindow(t *testing.T) {
	ctx := context.Background()
	patchValidate := mockey.Mock((*Callbacks).ValidateImportRequest).Return(nil).Build()
	defer patchValidate.UnPatch()
	patchReplication := mockey.Mock((*Callbacks).ValidateImportReplication).Return(nil).Build()
	defer patchReplication.UnPatch()
	patchPartitions := mockey.Mock((*Callbacks).ValidateSnapshotPartitionTargets).Return(nil).Build()
	defer patchPartitions.UnPatch()
	var order []string
	patchStart := mockey.Mock((*Callbacks).StartBroadcastWithCollectionID).To(
		func(*Callbacks, context.Context, int64) (broadcaster.BroadcastAPI, error) {
			order = append(order, "key")
			return &splitWindowBroadcastAPI{}, nil
		}).Build()
	defer patchStart.UnPatch()
	patchClose := mockey.Mock((*splitWindowBroadcastAPI).Close).Return().Build()
	defer patchClose.UnPatch()
	patchBroadcast := mockey.Mock((*splitWindowBroadcastAPI).Broadcast).To(
		func(*splitWindowBroadcastAPI, context.Context, message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			order = append(order, "broadcast")
			return &types.BroadcastAppendResult{}, nil
		}).Build()
	defer patchBroadcast.UnPatch()

	refusal := merr.WrapErrOperationNotSupportedMsg("Import is refused while a shard split is in its window")
	callbacks := &Callbacks{Deps: Deps{
		CheckSplitWindow: func(collectionID int64, operation string) error {
			order = append(order, "check")
			require.EqualValues(t, 100, collectionID)
			require.Equal(t, "Import", operation)
			return refusal
		},
	}}
	_, _, err := callbacks.BroadcastImport(ctx, 100, nil, nil, nil, 1, nil, "", internalpb.ImportVersion_ImportVersionV2)
	require.ErrorIs(t, err, merr.ErrOperationNotSupported)
	// The check runs under the key, and nothing is broadcast.
	require.Equal(t, []string{"key", "check"}, order)
}
