// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/querypb"

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
)

// use interface tsoAllocator to keep other components testable
// include: channelsTimeTickerImpl, baseTaskQueue, taskScheduler
type tsoAllocator interface {
	AllocOne() (Timestamp, error)
}

// use interface idAllocatorInterface to keep other components testable
// include: baseTaskQueue, taskScheduler
type idAllocatorInterface interface {
	AllocOne() (UniqueID, error)
}

// use timestampAllocatorInterface to keep other components testable
// include: TimestampAllocator
type timestampAllocatorInterface interface {
	AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error)
}

type getChannelsService interface {
	GetChannels(collectionID UniqueID) (map[vChan]pChan, error)
}

// createQueryChannelInterface defines CreateQueryChannel
type createQueryChannelInterface interface {
	CreateQueryChannel(ctx context.Context, request *querypb.CreateQueryChannelRequest) (*querypb.CreateQueryChannelResponse, error)
}
