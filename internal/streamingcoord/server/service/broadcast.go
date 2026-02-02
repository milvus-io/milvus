package service

import (
	"context"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// BroadcastService is the interface of the broadcast service.
type BroadcastService interface {
	streamingpb.StreamingCoordBroadcastServiceServer
}

// NewBroadcastService creates a new broadcast service.
func NewBroadcastService() BroadcastService {
	return &broadcastServceImpl{}
}

// broadcastServiceeeeImpl is the implementation of the broadcast service.
type broadcastServceImpl struct{}

// Broadcast broadcasts the message to all channels.
//
// Deprecated: This method is deprecated for Import operations. Import now calls
// DataCoord.ImportV2() directly, and DataCoord handles broadcasting internally using
// the local broadcaster. This follows the standard DDL/DCL pattern.
//
// This gRPC method is kept for backward compatibility during upgrades. It may still be
// used by old clients or other operations that haven't been migrated yet.
//
// For Import messages from old proxies, this method forwards the request to DataCoord.ImportV2
// to ensure proper validation and job creation.
func (s *broadcastServceImpl) Broadcast(ctx context.Context, req *streamingpb.BroadcastRequest) (*streamingpb.BroadcastResponse, error) {
	msg := message.NewBroadcastMutableMessageBeforeAppend(req.Message.Payload, req.Message.Properties)

	// Check if this is an import message from old proxy
	// If so, forward to DataCoord.ImportV2 for proper handling
	if msg.MessageType() == message.MessageTypeImport {
		return s.forwardImportToDataCoord(ctx, msg)
	}

	api, err := broadcast.StartBroadcastWithResourceKeys(ctx, msg.BroadcastHeader().ResourceKeys.Collect()...)
	if err != nil {
		return nil, err
	}
	defer api.Close()

	results, err := api.Broadcast(ctx, msg)
	if err != nil {
		return nil, err
	}
	protoResult := make(map[string]*streamingpb.ProduceMessageResponseResult, len(results.AppendResults))
	for vchannel, result := range results.AppendResults {
		protoResult[vchannel] = &streamingpb.ProduceMessageResponseResult{
			Id:              result.MessageID.IntoProto(),
			Timetick:        result.TimeTick,
			LastConfirmedId: result.LastConfirmedMessageID.IntoProto(),
		}
	}
	return &streamingpb.BroadcastResponse{
		BroadcastId: results.BroadcastID,
		Results:     protoResult,
	}, nil
}

// forwardImportToDataCoord forwards import messages from old proxies to DataCoord.ImportV2.
// This ensures backward compatibility during rolling upgrades where old proxy sends import
// via broadcast RPC but new DataCoord expects import via ImportV2 RPC.
func (s *broadcastServceImpl) forwardImportToDataCoord(ctx context.Context, msg message.BroadcastMutableMessage) (*streamingpb.BroadcastResponse, error) {
	// Parse the import message
	importMsg, err := message.AsBroadcastImportMessageV1(msg)
	if err != nil {
		return nil, err
	}
	body := importMsg.MustBody()

	log.Ctx(ctx).Info("forwarding import message from old proxy to DataCoord.ImportV2",
		zap.Int64("collectionID", body.GetCollectionID()),
		zap.String("collectionName", body.GetCollectionName()),
		zap.Int64s("partitionIDs", body.GetPartitionIDs()),
		zap.Int("fileCount", len(body.GetFiles())))

	// Convert msgpb.ImportFile to internalpb.ImportFile
	files := lo.Map(body.GetFiles(), func(f *msgpb.ImportFile, _ int) *internalpb.ImportFile {
		return &internalpb.ImportFile{
			Id:    f.GetId(),
			Paths: f.GetPaths(),
		}
	})

	// Build ImportRequestInternal from the broadcast message
	importReq := &internalpb.ImportRequestInternal{
		DbID:           0, // deprecated
		CollectionID:   body.GetCollectionID(),
		CollectionName: body.GetCollectionName(),
		PartitionIDs:   body.GetPartitionIDs(),
		ChannelNames:   msg.BroadcastHeader().VChannels,
		Schema:         body.GetSchema(),
		Files:          files,
		Options:        funcutil.Map2KeyValuePair(body.GetOptions()),
		DataTimestamp:  0, // Indicates this is from proxy, not from ack callback
		JobID:          body.GetJobID(),
	}

	// Get MixCoordClient to call DataCoord.ImportV2
	mixCoordClient, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, err
	}

	// Call DataCoord.ImportV2
	resp, err := mixCoordClient.ImportV2(ctx, importReq)
	if err != nil {
		return nil, err
	}
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}

	log.Ctx(ctx).Info("import request forwarded to DataCoord successfully",
		zap.String("jobID", resp.GetJobID()))

	// Return a response compatible with old proxy expectations
	// The old proxy doesn't really use the response content for import,
	// it just needs a successful response to know the import was accepted
	return &streamingpb.BroadcastResponse{
		BroadcastId: 0, // Not used for import
		Results:     make(map[string]*streamingpb.ProduceMessageResponseResult),
	}, nil
}

// Ack acknowledges the message at the specified vchannel.
func (s *broadcastServceImpl) Ack(ctx context.Context, req *streamingpb.BroadcastAckRequest) (*streamingpb.BroadcastAckResponse, error) {
	broadcaster, err := broadcast.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	// Once the ack is reached at streamingcoord, the ack operation should not be cancelable.
	ctx = context.WithoutCancel(ctx)
	if req.Message == nil {
		// before 2.6.1, the request don't have the message field, only have the broadcast id and vchannel.
		// so we need to use the legacy ack interface.
		if err := broadcaster.LegacyAck(ctx, req.BroadcastId, req.Vchannel); err != nil {
			return nil, err
		}
		return &streamingpb.BroadcastAckResponse{}, nil
	}
	if err := broadcaster.Ack(ctx, message.NewImmutableMesasge(
		message.MustUnmarshalMessageID(req.Message.Id),
		req.Message.Payload,
		req.Message.Properties,
	)); err != nil {
		return nil, err
	}
	return &streamingpb.BroadcastAckResponse{}, nil
}
