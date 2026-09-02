package handler

import (
	"context"
	"strconv"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

const (
	queryViewPChannelNameMetadataKey       = "milvus-query-view-pchannel"
	queryViewPChannelTermMetadataKey       = "milvus-query-view-pchannel-term"
	queryViewPChannelAccessModeMetadataKey = "milvus-query-view-pchannel-access-mode"
)

func EncodeQueryViewPChannelToOutgoingContext(ctx context.Context, pchannel types.PChannelInfo) context.Context {
	return metadata.AppendToOutgoingContext(
		ctx,
		queryViewPChannelNameMetadataKey, pchannel.Name,
		queryViewPChannelTermMetadataKey, strconv.FormatInt(pchannel.Term, 10),
		queryViewPChannelAccessModeMetadataKey, pchannel.AccessMode.String(),
	)
}

func DecodeQueryViewPChannelFromIncomingContext(ctx context.Context) (types.PChannelInfo, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return types.PChannelInfo{}, status.NewInvalidArgument("query view pchannel metadata is missing")
	}
	return decodeQueryViewPChannelMetadata(md)
}

func DecodeQueryViewPChannelFromOutgoingContext(ctx context.Context) (types.PChannelInfo, error) {
	md, ok := metadata.FromOutgoingContext(ctx)
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
	termValue := firstQueryViewMetadataValue(md, queryViewPChannelTermMetadataKey)
	if termValue == "" {
		return types.PChannelInfo{}, status.NewInvalidArgument("query view pchannel term is missing")
	}
	term, err := strconv.ParseInt(termValue, 10, 64)
	if err != nil || term <= 0 {
		return types.PChannelInfo{}, status.NewInvalidArgument("query view pchannel term is invalid: %s", termValue)
	}
	accessModeValue := firstQueryViewMetadataValue(md, queryViewPChannelAccessModeMetadataKey)
	switch accessModeValue {
	case types.AccessModeRW.String():
		return types.PChannelInfo{Name: name, Term: term, AccessMode: types.AccessModeRW}, nil
	case types.AccessModeRO.String():
		return types.PChannelInfo{Name: name, Term: term, AccessMode: types.AccessModeRO}, nil
	default:
		return types.PChannelInfo{}, status.NewInvalidArgument("query view pchannel access mode is invalid: %s", accessModeValue)
	}
}

func firstQueryViewMetadataValue(md metadata.MD, key string) string {
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
