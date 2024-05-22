package timestamp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var errExhausted = errors.New("exhausted")

// newLocalAllocator creates a new local allocator.
func newLocalAllocator() *localAllocator {
	return &localAllocator{
		mu:          sync.Mutex{},
		nextStartID: 0,
		endStartID:  0,
	}
}

// localAllocator allocates timestamp locally.
type localAllocator struct {
	mu          sync.Mutex
	nextStartID uint64 // Allocate timestamp locally.
	endStartID  uint64
}

// AllocateOne allocates a timestamp.
func (a *localAllocator) allocateOne() (uint64, error) {
	if a.nextStartID < a.endStartID {
		id := a.nextStartID
		a.nextStartID++
		return id, nil
	}
	return 0, errExhausted
}

// update updates the local allocator.
func (a *localAllocator) update(start uint64, count int) {
	// local allocator can be only increasing.
	if start >= a.endStartID {
		a.nextStartID = start
		a.endStartID = start + uint64(count)
	}
}

// expire expires all id in the local allocator.
func (a *localAllocator) exhausted() {
	a.endStartID = a.nextStartID
}

// remoteAllocator allocate timestamp from remote root coordinator.
type remoteAllocator struct {
	rc     types.RootCoordClient
	nodeID int64
}

// newRemoteAllocator creates a new remote allocator.
// TODO: should be batch allocated on remote and cache locally.
func newRemoteAllocator(rc types.RootCoordClient) *remoteAllocator {
	a := &remoteAllocator{
		nodeID: paramtable.GetNodeID(),
		rc:     rc,
	}
	return a
}

func (ta *remoteAllocator) allocate(ctx context.Context, count uint32) (uint64, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req := &rootcoordpb.AllocTimestampRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestTSO),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(ta.nodeID),
		),
		Count: count,
	}

	resp, err := ta.rc.AllocTimestamp(ctx, req)
	if err != nil {
		return 0, 0, fmt.Errorf("syncTimestamp Failed:%w", err)
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return 0, 0, fmt.Errorf("syncTimeStamp Failed:%s", resp.GetStatus().GetReason())
	}
	if resp == nil {
		return 0, 0, fmt.Errorf("empty AllocTimestampResponse")
	}
	return resp.GetTimestamp(), int(resp.GetCount()), nil
}
