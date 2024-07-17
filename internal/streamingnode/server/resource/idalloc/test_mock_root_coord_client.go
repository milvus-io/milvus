//go:build test
// +build test

package idalloc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
)

func NewMockRootCoordClient(t *testing.T) *mocks.MockRootCoordClient {
	counter := atomic.NewUint64(1)
	client := mocks.NewMockRootCoordClient(t)
	client.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, atr *rootcoordpb.AllocTimestampRequest, co ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
			if atr.Count > 1000 {
				panic(fmt.Sprintf("count %d is too large", atr.Count))
			}
			c := counter.Add(uint64(atr.Count))
			return &rootcoordpb.AllocTimestampResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				Timestamp: c - uint64(atr.Count),
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
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				ID:    int64(c - uint64(atr.Count)),
				Count: atr.Count,
			}, nil
		},
	).Maybe()
	return client
}
