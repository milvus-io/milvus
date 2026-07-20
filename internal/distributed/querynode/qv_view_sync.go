package grpcquerynode

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func registerQueryViewSyncServer(grpcServer *grpc.Server, segMgr qnview.SegmentManager) {
	registerQueryViewSyncHandler(grpcServer, qnview.NewQNQueryViewHandler(segMgr))
}

func registerQueryViewSyncHandler(grpcServer *grpc.Server, queryViewHandler *qnview.QNQueryViewHandler) {
	viewpb.RegisterViewSyncServiceServer(grpcServer, &queryNodeViewSyncServer{
		ViewSyncServer: handler.NewViewSyncServer(queryViewHandler),
	})
}

type queryNodeViewSyncServer struct {
	viewpb.UnimplementedViewSyncServiceServer
	*handler.ViewSyncServer
}

func (s *queryNodeViewSyncServer) SyncQueryView(stream viewpb.ViewSyncService_SyncQueryViewServer) error {
	return s.ViewSyncServer.SyncQueryView(stream)
}

type lazyQNSegmentManager struct {
	build func() qnview.SegmentManager

	mu      sync.Mutex
	manager qnview.SegmentManager
}

func (m *lazyQNSegmentManager) Acquire(req qnview.AcquireSegments) {
	manager := m.get()
	if manager == nil {
		go func() {
			if req.OnUnrecoverable != nil {
				req.OnUnrecoverable()
			}
		}()
		return
	}
	manager.Acquire(req)
}

func (m *lazyQNSegmentManager) Release(req qnview.ReleaseSegments) {
	manager := m.get()
	if manager == nil {
		go func() {
			if req.OnDropped != nil {
				req.OnDropped()
			}
		}()
		return
	}
	manager.Release(req)
}

func (m *lazyQNSegmentManager) AcquireSealedSegmentHandles(ctx context.Context, key qviews.QueryViewKey, view *viewpb.QueryViewOfQueryNode) ([]qnview.SealedSegmentHandle, error) {
	manager := m.get()
	if manager == nil {
		return nil, merr.WrapErrServiceUnavailable("query view segment manager is not initialized")
	}
	return manager.AcquireSealedSegmentHandles(ctx, key, view)
}

func (m *lazyQNSegmentManager) WaitTransformVisible(ctx context.Context, key qviews.QueryViewKey, timetick uint64) error {
	manager := m.get()
	if manager == nil {
		return merr.WrapErrServiceUnavailable("query view segment manager is not initialized")
	}
	return manager.WaitTransformVisible(ctx, key, timetick)
}

func (m *lazyQNSegmentManager) get() qnview.SegmentManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.manager == nil && m.build != nil {
		m.manager = m.build()
	}
	return m.manager
}

func queryViewTransformLogStreamManager() wal.TransformLogStreamManager {
	walAccesser := streaming.WAL()
	if walAccesser == nil {
		return nil
	}
	return walAccesser.TransformLogStreamManager()
}

type lazyQueryViewLoadMetadataProvider struct {
	mixCoord *syncutil.Future[types.MixCoordClient]
}

func (p *lazyQueryViewLoadMetadataProvider) client(ctx context.Context) (types.MixCoordClient, error) {
	if p.mixCoord == nil {
		return nil, merr.WrapErrServiceUnavailable("mixcoord client is not initialized")
	}
	return p.mixCoord.GetWithContext(ctx)
}

func (p *lazyQueryViewLoadMetadataProvider) DescribeCollection(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
	var resp *milvuspb.DescribeCollectionResponse
	err := retryQueryViewMetadataRPC(ctx, func(rpcCtx context.Context) error {
		client, err := p.client(rpcCtx)
		if err != nil {
			return merr.Wrapf(err, "describe collection %d for query view", collectionID)
		}
		var callErr error
		resp, callErr = client.DescribeCollection(rpcCtx, &milvuspb.DescribeCollectionRequest{
			Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection)),
			// Collection name alone is ambiguous after database support.
			CollectionID: collectionID,
		})
		if err := merr.CheckRPCCall(resp, callErr); err != nil {
			return merr.Wrapf(err, "describe collection %d for query view", collectionID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *lazyQueryViewLoadMetadataProvider) GetQueryViewLoadInfo(ctx context.Context, collectionID int64, version qnview.QueryViewLoadInfoVersion) (qnview.QueryViewLoadInfo, error) {
	var resp *querypb.GetQueryViewLoadInfoResponse
	err := retryQueryViewMetadataRPC(ctx, func(rpcCtx context.Context) error {
		client, err := p.client(rpcCtx)
		if err != nil {
			return merr.Wrapf(err, "get query view load info for collection %d", collectionID)
		}
		var callErr error
		resp, callErr = client.GetQueryViewLoadInfo(rpcCtx, &querypb.GetQueryViewLoadInfoRequest{
			CollectionID: collectionID,
			Version:      uint64(version),
		})
		if err := merr.CheckRPCCall(resp, callErr); err != nil {
			return merr.Wrapf(err, "get query view load info for collection %d", collectionID)
		}
		return nil
	})
	if err != nil {
		return qnview.QueryViewLoadInfo{}, err
	}
	return qnview.QueryViewLoadInfo{
		CollectionID: resp.GetCollectionID(),
		Version:      qnview.QueryViewLoadInfoVersionFromProto(resp.GetVersion()),
		PartitionIDs: append([]int64(nil), resp.GetPartitionIDs()...),
		LoadFields:   resp.GetLoadFields(),
		IndexInfos:   resp.GetIndexInfoList(),
	}, nil
}

func retryQueryViewMetadataRPC(ctx context.Context, fn func(context.Context) error) error {
	var lastErr error
	err := retry.Do(ctx, func() error {
		rpcCtx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
		defer cancel()

		err := fn(rpcCtx)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRecoverableQueryViewMetadataError(err) {
			return retry.Unrecoverable(err)
		}
		return err
	}, retry.AttemptAlways())
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err != nil && lastErr != nil {
		return lastErr
	}
	return err
}

func isRecoverableQueryViewMetadataError(err error) bool {
	if err == nil {
		return false
	}
	if merr.GetErrorType(err) == merr.InputError {
		return false
	}
	return !errors.Is(err, merr.ErrCollectionNotFound) &&
		!errors.Is(err, merr.ErrDatabaseNotFound) &&
		!errors.Is(err, merr.ErrPartitionNotFound) &&
		!errors.Is(err, merr.ErrSegmentNotFound) &&
		!errors.Is(err, merr.ErrIndexNotFound)
}
