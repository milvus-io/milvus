package grpcquerynode

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	qn "github.com/milvus-io/milvus/internal/querynodev2"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func registerQueryViewSyncServer(grpcServer *grpc.Server, segMgr qnview.SegmentManager) {
	viewpb.RegisterViewSyncServiceServer(grpcServer, &queryNodeViewSyncServer{
		ViewSyncServer: handler.NewViewSyncServer(qnview.NewQNQueryViewHandler(segMgr)),
	})
}

type queryNodeViewSyncServer struct {
	viewpb.UnimplementedViewSyncServiceServer
	*handler.ViewSyncServer
}

func (s *queryNodeViewSyncServer) SyncQueryView(stream viewpb.ViewSyncService_SyncQueryViewServer) error {
	return s.ViewSyncServer.SyncQueryView(stream)
}

func (s *Server) registerQueryViewSyncServer() {
	registerQueryViewSyncServer(s.grpcServer, &lazyQNSegmentManager{
		build: func() qnview.SegmentManager {
			qnImpl, ok := s.querynode.(*qn.QueryNode)
			if !ok {
				return nil
			}
			return qnImpl.NewQueryViewSegmentManager(
				&lazyQueryViewLoadMetadataProvider{mixCoord: s.mixCoord},
				queryViewTransformLogAccesser(),
			)
		},
	})
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

func (m *lazyQNSegmentManager) get() qnview.SegmentManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.manager == nil && m.build != nil {
		m.manager = m.build()
	}
	return m.manager
}

func queryViewTransformLogAccesser() wal.TransformLogAccesser {
	wal := streaming.WAL()
	if wal == nil {
		return nil
	}
	return wal.TransformLog()
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
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	client, err := p.client(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection)),
		// Collection name alone is ambiguous after database support.
		CollectionID: collectionID,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		mlog.Warn(ctx, "failed to describe collection for query view", mlog.Int64("collectionID", collectionID), mlog.Err(err))
		return nil, err
	}
	return resp, nil
}

func (p *lazyQueryViewLoadMetadataProvider) GetQueryViewSegmentLoadInfo(ctx context.Context, collectionID int64, segmentIDs ...int64) ([]*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	client, err := p.client(ctx)
	if err != nil {
		return nil, nil, err
	}
	resp, err := client.GetQueryViewSegmentLoadInfo(ctx, &querypb.GetQueryViewSegmentLoadInfoRequest{
		CollectionID: collectionID,
		SegmentIDs:   segmentIDs,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		mlog.Warn(ctx, "failed to get query view segment load info", mlog.Int64("collectionID", collectionID), mlog.Int64s("segmentIDs", segmentIDs), mlog.Err(err))
		return nil, nil, err
	}
	if len(resp.GetInfos()) == 0 && len(segmentIDs) > 0 {
		return nil, nil, merr.WrapErrSegmentNotFound(segmentIDs[0], "no such segment load info in DataCoord")
	}
	return resp.GetInfos(), resp.GetIndexInfoList(), nil
}
