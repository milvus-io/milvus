package idalloc

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"

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
		nextStartID: 0,
		endStartID:  0,
	}
}

// localAllocator allocates timestamp locally.
type localAllocator struct {
	nextStartID int64 // Allocate timestamp locally.
	endStartID  int64
}

// AllocateOne allocates a timestamp.
func (a *localAllocator) allocateOne() (int64, error) {
	if a.nextStartID < a.endStartID {
		id := a.nextStartID
		a.nextStartID++
		return id, nil
	}
	return 0, errExhausted
}

// update updates the local allocator.
func (a *localAllocator) update(start int64, count int) {
	// local allocator can be only increasing.
	if start >= a.endStartID {
		a.nextStartID = start
		a.endStartID = start + int64(count)
	}
}

// expire expires all id in the local allocator.
func (a *localAllocator) exhausted() {
	a.nextStartID = a.endStartID
}

// tsoAllocator allocate timestamp from remote root coordinator.
type tsoAllocator struct {
	rc     types.RootCoordClient
	nodeID int64
}

// newTSOAllocator creates a new remote allocator.
func newTSOAllocator(rc types.RootCoordClient) *tsoAllocator {
	a := &tsoAllocator{
		nodeID: paramtable.GetNodeID(),
		rc:     rc,
	}
	return a
}

func (ta *tsoAllocator) batchAllocate(count uint32) (int64, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	return int64(resp.GetTimestamp()), int(resp.GetCount()), nil
}

// idAllocator allocate timestamp from remote root coordinator.
type idAllocator struct {
	rc     types.RootCoordClient
	nodeID int64
}

// newIDAllocator creates a new remote allocator.
func newIDAllocator(rc types.RootCoordClient) *idAllocator {
	a := &idAllocator{
		nodeID: paramtable.GetNodeID(),
		rc:     rc,
	}
	return a
}

func (ta *idAllocator) batchAllocate(count uint32) (int64, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &rootcoordpb.AllocIDRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestID),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(ta.nodeID),
		),
		Count: count,
	}

	resp, err := ta.rc.AllocID(ctx, req)
	if err != nil {
		return 0, 0, fmt.Errorf("AllocID Failed:%w", err)
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return 0, 0, fmt.Errorf("AllocID Failed:%s", resp.GetStatus().GetReason())
	}
	if resp == nil {
		return 0, 0, fmt.Errorf("empty AllocID")
	}
	if resp.GetID() < 0 {
		panic("get unexpected negative id")
	}
	return resp.GetID(), int(resp.GetCount()), nil
}
