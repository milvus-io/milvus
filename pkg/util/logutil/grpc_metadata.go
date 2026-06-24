package logutil

import (
	"context"
	"strconv"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v3/common"
)

func GetClientReqUnixmsecGrpc(ctx context.Context) (int64, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return -1, false
	}

	requestUnixmsecs := getMetadata(md, common.ClientRequestMsecKey)
	if len(requestUnixmsecs) < 1 {
		return -1, false
	}
	requestUnixmsec, err := strconv.ParseInt(requestUnixmsecs[0], 10, 64)
	if err != nil {
		return -1, false
	}
	return requestUnixmsec, true
}

func getMetadata(md metadata.MD, keys ...string) []string {
	var result []string
	for _, key := range keys {
		if values := md.Get(key); len(values) > 0 {
			result = append(result, values...)
		}
	}
	return result
}
