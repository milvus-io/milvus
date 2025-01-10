//go:build test
// +build test

package idalloc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

func NewMockRootCoordClient(t *testing.T) *mocks.MockRootCoordClient {
	counter := atomic.NewUint64(1)
	client := mocks.NewMockRootCoordClient(t)
	lastAllocate := atomic.NewInt64(0)
	client.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, atr *rootcoordpb.AllocTimestampRequest, co ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
			if atr.Count > 1000 {
				panic(fmt.Sprintf("count %d is too large", atr.Count))
			}
			now := time.Now()
			for {
				lastAllocateMilli := lastAllocate.Load()
				if now.UnixMilli() <= lastAllocateMilli {
					now = time.Now()
					continue
				}
				if lastAllocate.CompareAndSwap(lastAllocateMilli, now.UnixMilli()) {
					break
				}
			}
			return &rootcoordpb.AllocTimestampResponse{
				Status:    merr.Success(),
				Timestamp: tsoutil.ComposeTSByTime(now, 0),
				Count:     atr.Count,
			}, nil
		},
	).Maybe()
	client.EXPECT().AllocID(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, atr *rootcoordpb.AllocIDRequest, co ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
			if atr.Count > 1000 {
				panic(fmt.Sprintf("count %d is too large", atr.Count))
			}
			c := counter.Add(uint64(atr.Count))
			return &rootcoordpb.AllocIDResponse{
				Status: merr.Success(),
				ID:     int64(c - uint64(atr.Count)),
				Count:  atr.Count,
			}, nil
		},
	).Maybe()
	return client
}
