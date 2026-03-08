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

package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// alterLoadConfigV2AckCallback is called when the put load config message is acknowledged
func (s *Server) alterLoadConfigV2AckCallback(ctx context.Context, result message.BroadcastResultAlterLoadConfigMessageV2) error {
	// currently, we only sent the put load config message to the control channel
	// TODO: after we support query view in 3.0, we should broadcast the put load config message to all vchannels.
	job := job.NewLoadCollectionJob(ctx, result, s.dist, s.meta, s.broker, s.targetMgr, s.targetObserver, s.collectionObserver, s.checkerController, s.nodeMgr)
	if err := job.Execute(); err != nil {
		return err
	}
	meta.GlobalFailedLoadCache.Remove(result.Message.Header().GetCollectionId())
	return nil
}
