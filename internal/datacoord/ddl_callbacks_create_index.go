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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (s *DDLCallbacks) createIndexV2AckCallback(ctx context.Context, result message.BroadcastResultCreateIndexMessageV2) error {
	index := result.Message.MustBody().FieldIndex
	idx := model.UnmarshalIndexModel(index)
	if isPKIndex(idx) {
		// The state of a primary key index is the decision of every data
		// vchannel at the CreateIndex. The callback reads only the broadcast
		// result, so a retry writes the same state, on a secondary too.
		idx.State, idx.FailReason = pkIndexStateOf(result.Results)
	}
	if err := s.meta.indexMeta.CreateIndex(ctx, idx); err != nil {
		return err
	}
	if isPKIndex(idx) && idx.State == commonpb.IndexState_Failed {
		s.notifyPKIndexFailed(ctx, idx.CollectionID, idx.IndexID)
	}
	select {
	case s.notifyIndexChan <- index.IndexInfo.CollectionID:
	default:
	}
	return nil
}
