package handler

import (
	"context"
	"strconv"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

const (
	queryViewReplicaIDMetadataKey          = "milvus-query-view-wal-replica-id"
	queryViewPChannelNameMetadataKey       = "milvus-query-view-pchannel"
	queryViewPChannelTermMetadataKey       = "milvus-query-view-pchannel-term"
	queryViewPChannelAccessModeMetadataKey = "milvus-query-view-pchannel-access-mode"
)

func EncodeQueryViewPChannelToOutgoingContext(ctx context.Context, pchannel types.PChannelInfo) context.Context {
	fields := []string{queryViewPChannelNameMetadataKey, pchannel.Name, queryViewPChannelAccessModeMetadataKey, pchannel.AccessMode.String()}
	if pchannel.WALReplicaID > 0 {
		fields = append(fields, queryViewReplicaIDMetadataKey, strconv.FormatInt(pchannel.WALReplicaID, 10))
	}
	if pchannel.Term > 0 {
		fields = append(fields, queryViewPChannelTermMetadataKey, strconv.FormatInt(pchannel.Term, 10))
	}
	return metadata.AppendToOutgoingContext(ctx, fields...)
}

func DecodeQueryViewPChannelFromIncomingContext(ctx context.Context) (types.PChannelInfo, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return types.PChannelInfo{}, status.NewInvalidArgument("query view pchannel metadata is missing")
	}
	return decodeQueryViewPChannelMetadata(md)
}

func decodeQueryViewPChannelMetadata(md metadata.MD) (types.PChannelInfo, error) {
	name := firstQueryViewMetadataValue(md, queryViewPChannelNameMetadataKey)
	if name == "" {
		return types.PChannelInfo{}, status.NewInvalidArgument("query view pchannel name is missing")
	}
	channel := types.PChannelInfo{Name: name}
	for key, dest := range map[string]*int64{
		queryViewPChannelTermMetadataKey: &channel.Term,
		queryViewReplicaIDMetadataKey:    &channel.WALReplicaID,
	} {
		if value := md.Get(key); len(value) > 0 {
			id, err := strconv.ParseInt(value[0], 10, 64)
			if len(value) != 1 || err != nil || id <= 0 {
				return types.PChannelInfo{}, status.NewInvalidArgument("invalid query view metadata %s", key)
			}
			*dest = id
		}
	}
	if channel.Term == 0 && channel.WALReplicaID == 0 {
		return types.PChannelInfo{}, status.NewInvalidArgument("query view route has neither a replica ID nor a writer term")
	}
	accessModeValue := firstQueryViewMetadataValue(md, queryViewPChannelAccessModeMetadataKey)
	switch accessModeValue {
	case types.AccessModeRW.String():
		channel.AccessMode = types.AccessModeRW
	case types.AccessModeRO.String():
		channel.AccessMode = types.AccessModeRO
	default:
		return types.PChannelInfo{}, status.NewInvalidArgument("query view pchannel access mode is invalid: %s", accessModeValue)
	}
	return channel, nil
}

func firstQueryViewMetadataValue(md metadata.MD, key string) string {
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
