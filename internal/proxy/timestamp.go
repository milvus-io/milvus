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

package proxy

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

// timestampAllocator implements tsoAllocator.
type timestampAllocator struct {
	tso    timestampAllocatorInterface
	peerID UniqueID
}

// newTimestampAllocator creates a new timestampAllocator
func newTimestampAllocator(tso timestampAllocatorInterface, peerID UniqueID) (*timestampAllocator, error) {
	a := &timestampAllocator{
		peerID: peerID,
		tso:    tso,
	}
	return a, nil
}

func (ta *timestampAllocator) alloc(ctx context.Context, count uint32) ([]Timestamp, error) {
	tr := timerecord.NewTimeRecorder("applyTimestamp")
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req := &rootcoordpb.AllocTimestampRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestTSO),
			commonpbutil.WithSourceID(ta.peerID),
		),
		Count: count,
	}

	resp, err := ta.tso.AllocTimestamp(ctx, req)
	defer func() {
		metrics.ProxyApplyTimestampLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	}()

	if err != nil {
		return nil, fmt.Errorf("syncTimestamp Failed:%w", err)
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, fmt.Errorf("syncTimeStamp Failed:%s", resp.GetStatus().GetReason())
	}
	if resp == nil {
		return nil, fmt.Errorf("empty AllocTimestampResponse")
	}
	start, cnt := resp.GetTimestamp(), resp.GetCount()
	ret := make([]Timestamp, cnt)
	for i := uint32(0); i < cnt; i++ {
		ret[i] = start + uint64(i)
	}

	return ret, nil
}

// AllocOne allocates a timestamp.
func (ta *timestampAllocator) AllocOne(ctx context.Context) (Timestamp, error) {
	ret, err := ta.alloc(ctx, 1)
	if err != nil {
		return 0, err
	}
	return ret[0], nil
}
