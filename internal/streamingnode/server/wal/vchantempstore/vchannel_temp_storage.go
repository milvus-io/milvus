package vchantempstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// ErrNotFound is returned when the vchannel is not found.
var ErrNotFound = errors.New("not found")

// NewVChannelTempStorage creates a new VChannelTempStorage.
func NewVChannelTempStorage(rc *syncutil.Future[types.RootCoordClient]) *VChannelTempStorage {
	return &VChannelTempStorage{
		rc:        rc,
		vchannels: make(map[int64]map[string]string),
	}
}

// VChannelTempStorage is a temporary storage for vchannel messages.
// It's used to make compatibility between old version and new version message.
// TODO: removed in 3.0.
type VChannelTempStorage struct {
	rc *syncutil.Future[types.RootCoordClient]

	mu        sync.Mutex
	vchannels map[int64]map[string]string
}

func (ts *VChannelTempStorage) GetVChannelByPChannelOfCollection(ctx context.Context, collectionID int64, pchannel string) (string, error) {
	if err := ts.updateVChannelByPChannelOfCollectionIfNotExist(ctx, collectionID); err != nil {
		return "", err
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()
	item, ok := ts.vchannels[collectionID]
	if !ok {
		return "", errors.Wrapf(ErrNotFound, "collection %d at pchannel %s", collectionID, pchannel)
	}
	v, ok := item[pchannel]
	if !ok {
		panic(fmt.Sprintf("pchannel not found for collection %d at pchannel %s", collectionID, pchannel))
	}
	return v, nil
}

func (ts *VChannelTempStorage) updateVChannelByPChannelOfCollectionIfNotExist(ctx context.Context, collectionID int64) error {
	ts.mu.Lock()
	if _, ok := ts.vchannels[collectionID]; ok {
		ts.mu.Unlock()
		return nil
	}
	ts.mu.Unlock()

	rc, err := ts.rc.GetWithContext(ctx)
	if err != nil {
		return err
	}

	return retry.Do(ctx, func() error {
		resp, err := rc.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			CollectionID: collectionID,
		})
		err = merr.CheckRPCCall(resp, err)
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return nil
		}
		if err == nil {
			ts.mu.Lock()
			if _, ok := ts.vchannels[collectionID]; !ok {
				ts.vchannels[collectionID] = make(map[string]string, len(resp.PhysicalChannelNames))
			}
			for idx, pchannel := range resp.PhysicalChannelNames {
				ts.vchannels[collectionID][pchannel] = resp.VirtualChannelNames[idx]
			}
			ts.mu.Unlock()
		}
		return err
	})
}
