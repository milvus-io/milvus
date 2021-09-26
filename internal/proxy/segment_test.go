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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

type mockDataCoord struct {
	expireTime Timestamp
}

func (mockD *mockDataCoord) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	assigns := make([]*datapb.SegmentIDAssignment, 0, len(req.SegmentIDRequests))
	maxPerCnt := 100
	for _, r := range req.SegmentIDRequests {
		totalCnt := uint32(0)
		for totalCnt != r.Count {
			cnt := uint32(rand.Intn(maxPerCnt))
			if totalCnt+cnt > r.Count {
				cnt = r.Count - totalCnt
			}
			totalCnt += cnt
			result := &datapb.SegmentIDAssignment{
				SegID:        1,
				ChannelName:  r.ChannelName,
				Count:        cnt,
				CollectionID: r.CollectionID,
				PartitionID:  r.PartitionID,
				ExpireTime:   mockD.expireTime,

				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
			}
			assigns = append(assigns, result)
		}
	}

	return &datapb.AssignSegmentIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SegIDAssignments: assigns,
	}, nil
}

type mockDataCoord2 struct {
	expireTime Timestamp
}

func (mockD *mockDataCoord2) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {

	return &datapb.AssignSegmentIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Just For Test",
		},
	}, nil
}

func getLastTick1() Timestamp {
	return 1000
}

func TestSegmentAllocator1(t *testing.T) {
	ctx := context.Background()
	dataCoord := &mockDataCoord{}
	dataCoord.expireTime = Timestamp(1000)
	segAllocator, err := newSegIDAssigner(ctx, dataCoord, getLastTick1)
	assert.Nil(t, err)
	wg := &sync.WaitGroup{}
	segAllocator.Start()

	wg.Add(1)
	go func(group *sync.WaitGroup) {
		defer group.Done()
		time.Sleep(100 * time.Millisecond)
		segAllocator.Close()
	}(wg)
	total := uint32(0)
	collNames := []string{"abc", "cba"}
	for i := 0; i < 10; i++ {
		colName := collNames[i%2]
		ret, err := segAllocator.GetSegmentID(1, 1, colName, 1, 1)
		assert.Nil(t, err)
		total += ret[1]
	}
	assert.Equal(t, uint32(10), total)

	ret, err := segAllocator.GetSegmentID(1, 1, "abc", segCountPerRPC-10, 999)
	assert.Nil(t, err)
	assert.Equal(t, uint32(segCountPerRPC-10), ret[1])

	_, err = segAllocator.GetSegmentID(1, 1, "abc", 10, 1001)
	assert.NotNil(t, err)
	wg.Wait()

}

var curLastTick2 = Timestamp(200)
var curLastTIck2Lock sync.Mutex

func getLastTick2() Timestamp {
	curLastTIck2Lock.Lock()
	defer curLastTIck2Lock.Unlock()
	curLastTick2 += 100
	return curLastTick2
}

func TestSegmentAllocator2(t *testing.T) {
	ctx := context.Background()
	dataCoord := &mockDataCoord{}
	dataCoord.expireTime = Timestamp(500)
	segAllocator, err := newSegIDAssigner(ctx, dataCoord, getLastTick2)
	assert.Nil(t, err)
	wg := &sync.WaitGroup{}
	segAllocator.Start()

	wg.Add(1)
	go func(group *sync.WaitGroup) {
		defer group.Done()
		time.Sleep(100 * time.Millisecond)
		segAllocator.Close()
	}(wg)
	total := uint32(0)
	for i := 0; i < 10; i++ {
		ret, err := segAllocator.GetSegmentID(1, 1, "abc", 1, 200)
		assert.Nil(t, err)
		total += ret[1]
	}
	assert.Equal(t, uint32(10), total)
	time.Sleep(50 * time.Millisecond)
	_, err = segAllocator.GetSegmentID(1, 1, "abc", segCountPerRPC-10, getLastTick2())
	assert.NotNil(t, err)
	wg.Wait()

}

func TestSegmentAllocator3(t *testing.T) {
	ctx := context.Background()
	dataCoord := &mockDataCoord2{}
	dataCoord.expireTime = Timestamp(500)
	segAllocator, err := newSegIDAssigner(ctx, dataCoord, getLastTick2)
	assert.Nil(t, err)
	wg := &sync.WaitGroup{}
	segAllocator.Start()

	wg.Add(1)
	go func(group *sync.WaitGroup) {
		defer group.Done()
		time.Sleep(100 * time.Millisecond)
		segAllocator.Close()
	}(wg)
	time.Sleep(50 * time.Millisecond)
	_, err = segAllocator.GetSegmentID(1, 1, "abc", 10, 100)
	assert.NotNil(t, err)
	wg.Wait()
}

type mockDataCoord3 struct {
	expireTime Timestamp
}

func (mockD *mockDataCoord3) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	assigns := make([]*datapb.SegmentIDAssignment, 0, len(req.SegmentIDRequests))
	for i, r := range req.SegmentIDRequests {
		errCode := commonpb.ErrorCode_Success
		reason := ""
		if i == 0 {
			errCode = commonpb.ErrorCode_UnexpectedError
			reason = "Just for test"
		}
		result := &datapb.SegmentIDAssignment{
			SegID:        1,
			ChannelName:  r.ChannelName,
			Count:        r.Count,
			CollectionID: r.CollectionID,
			PartitionID:  r.PartitionID,
			ExpireTime:   mockD.expireTime,

			Status: &commonpb.Status{
				ErrorCode: errCode,
				Reason:    reason,
			},
		}
		assigns = append(assigns, result)
	}

	return &datapb.AssignSegmentIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SegIDAssignments: assigns,
	}, nil
}

func TestSegmentAllocator4(t *testing.T) {
	ctx := context.Background()
	dataCoord := &mockDataCoord3{}
	dataCoord.expireTime = Timestamp(500)
	segAllocator, err := newSegIDAssigner(ctx, dataCoord, getLastTick2)
	assert.Nil(t, err)
	wg := &sync.WaitGroup{}
	segAllocator.Start()

	wg.Add(1)
	go func(group *sync.WaitGroup) {
		defer group.Done()
		time.Sleep(100 * time.Millisecond)
		segAllocator.Close()
	}(wg)
	time.Sleep(50 * time.Millisecond)
	_, err = segAllocator.GetSegmentID(1, 1, "abc", 10, 100)
	assert.NotNil(t, err)
	wg.Wait()
}

type mockDataCoord5 struct {
	expireTime Timestamp
}

func (mockD *mockDataCoord5) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {

	return &datapb.AssignSegmentIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Just For Test",
		},
	}, fmt.Errorf("Just for test")
}

func TestSegmentAllocator5(t *testing.T) {
	ctx := context.Background()
	dataCoord := &mockDataCoord5{}
	dataCoord.expireTime = Timestamp(500)
	segAllocator, err := newSegIDAssigner(ctx, dataCoord, getLastTick2)
	assert.Nil(t, err)
	wg := &sync.WaitGroup{}
	segAllocator.Start()

	wg.Add(1)
	go func(group *sync.WaitGroup) {
		defer group.Done()
		time.Sleep(100 * time.Millisecond)
		segAllocator.Close()
	}(wg)
	time.Sleep(50 * time.Millisecond)
	_, err = segAllocator.GetSegmentID(1, 1, "abc", 10, 100)
	assert.NotNil(t, err)
	wg.Wait()
}

func TestSegmentAllocator6(t *testing.T) {
	ctx := context.Background()
	dataCoord := &mockDataCoord{}
	dataCoord.expireTime = Timestamp(500)
	segAllocator, err := newSegIDAssigner(ctx, dataCoord, getLastTick2)
	assert.Nil(t, err)
	wg := &sync.WaitGroup{}
	segAllocator.Start()

	wg.Add(1)
	go func(group *sync.WaitGroup) {
		defer group.Done()
		time.Sleep(100 * time.Millisecond)
		segAllocator.Close()
	}(wg)
	success := true
	var sucLock sync.Mutex
	collNames := []string{"abc", "cba"}
	reqFunc := func(i int, group *sync.WaitGroup) {
		defer group.Done()
		sucLock.Lock()
		defer sucLock.Unlock()
		if !success {
			return
		}
		colName := collNames[i%2]
		count := uint32(10)
		if i == 0 {
			count = 0
		}
		_, err = segAllocator.GetSegmentID(1, 1, colName, count, 100)
		if err != nil {
			fmt.Println(err)
			success = false
		}
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go reqFunc(i, wg)
	}
	wg.Wait()
	assert.True(t, success)

}
