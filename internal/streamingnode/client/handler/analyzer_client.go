package handler

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/analyzerservice"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// AnalyzerClient is the analyzer domain client under HandlerClient.
type AnalyzerClient interface {
	RunAnalyzer(context.Context, *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error)
}

type analyzerClient struct {
	owner *handlerClientImpl
}

func newAnalyzerClient(owner *handlerClientImpl) AnalyzerClient {
	return &analyzerClient{owner: owner}
}

func (hc *handlerClientImpl) AnalyzerClient() AnalyzerClient {
	return hc.analyzerClient
}

func (ac *analyzerClient) RunAnalyzer(ctx context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
	hc := ac.owner
	if !hc.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("handler client is closing")
	}
	defer hc.lifetime.Done()
	field := req.GetFieldAnalyzer()
	if field == nil && req.GetInlineAnalyzer() == nil {
		return nil, status.NewInvalidArgument("analyzer source is required")
	}
	if field != nil && field.GetVchannel() == "" {
		return nil, status.NewInvalidArgument("field analyzer vchannel is required")
	}
	var resp *streamingpb.StreamingNodeRunAnalyzerResponse
	err := retry.Handle(ctx, func() (bool, error) {
		client, err := hc.service.GetService(ctx)
		if err != nil {
			return analyzerservice.Retryable(err), err
		}
		if field == nil {
			// With no server ID, the existing picker round-robins ready SN connections.
			resp, err = client.RunAnalyzer(ctx, req)
			return analyzerservice.Retryable(err), err
		}
		assign := hc.watcher.Get(ctx, funcutil.ToPhysicalChannel(field.GetVchannel()))
		if assign == nil {
			return true, status.NewInner("analyzer assignment is not ready")
		}
		owned := proto.Clone(req).(*streamingpb.StreamingNodeRunAnalyzerRequest)
		owned.GetFieldAnalyzer().Pchannel = types.NewProtoFromPChannelInfo(assign.Channel)
		resp, err = client.RunAnalyzer(contextutil.WithPickServerID(ctx, assign.Node.ServerID), owned)
		if err != nil && isPermanentFailureUntilNewAssignment(err) {
			_ = hc.rebalanceTrigger.ReportAssignmentError(ctx, assign.Channel, err)
		}
		return analyzerservice.Retryable(err), err
	}, retry.Attempts(3))
	return resp, err
}
