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
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
)

type timestampAllocator struct {
	ctx    context.Context
	tso    timestampAllocatorInterface
	peerID UniqueID
}

func newTimestampAllocator(ctx context.Context, tso timestampAllocatorInterface, peerID UniqueID) (*timestampAllocator, error) {
	a := &timestampAllocator{
		ctx:    ctx,
		peerID: peerID,
		tso:    tso,
	}
	return a, nil
}

func (ta *timestampAllocator) alloc(count uint32) ([]Timestamp, error) {
	ctx, cancel := context.WithTimeout(ta.ctx, 5*time.Second)
	req := &rootcoordpb.AllocTimestampRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_RequestTSO,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  ta.peerID,
		},
		Count: count,
	}

	resp, err := ta.tso.AllocTimestamp(ctx, req)
	defer cancel()

	if err != nil {
		return nil, fmt.Errorf("syncTimestamp Failed:%w", err)
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, fmt.Errorf("syncTimeStamp Failed:%s", resp.Status.Reason)
	}
	start, cnt := resp.Timestamp, resp.Count
	var ret []Timestamp
	for i := uint32(0); i < cnt; i++ {
		ret = append(ret, start+uint64(i))
	}

	return ret, nil
}

func (ta *timestampAllocator) AllocOne() (Timestamp, error) {
	ret, err := ta.alloc(1)
	if err != nil {
		return 0, err
	}
	return ret[0], nil
}
