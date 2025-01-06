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
	"github.com/milvus-io/milvus/pkg/util/syncutil"
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
	a.nextStartID = a.endStartID
}

// tsoAllocator allocate timestamp from remote root coordinator.
type tsoAllocator struct {
	rc     *syncutil.Future[types.RootCoordClient]
	nodeID int64
}

// newTSOAllocator creates a new remote allocator.
func newTSOAllocator(rc *syncutil.Future[types.RootCoordClient]) *tsoAllocator {
	a := &tsoAllocator{
		nodeID: paramtable.GetNodeID(),
		rc:     rc,
	}
	return a
}

func (ta *tsoAllocator) batchAllocate(ctx context.Context, count uint32) (uint64, int, error) {
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
	rc, err := ta.rc.GetWithContext(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("get root coordinator client timeout: %w", err)
	}

	resp, err := rc.AllocTimestamp(ctx, req)
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

// idAllocator allocate timestamp from remote root coordinator.
type idAllocator struct {
	rc     *syncutil.Future[types.RootCoordClient]
	nodeID int64
}

// newIDAllocator creates a new remote allocator.
func newIDAllocator(rc *syncutil.Future[types.RootCoordClient]) *idAllocator {
	a := &idAllocator{
		nodeID: paramtable.GetNodeID(),
		rc:     rc,
	}
	return a
}

func (ta *idAllocator) batchAllocate(ctx context.Context, count uint32) (uint64, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req := &rootcoordpb.AllocIDRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestID),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(ta.nodeID),
		),
		Count: count,
	}
	rc, err := ta.rc.GetWithContext(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("get root coordinator client timeout: %w", err)
	}

	resp, err := rc.AllocID(ctx, req)
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
	return uint64(resp.GetID()), int(resp.GetCount()), nil
}
