package proxy

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

func getIdentifierFromContext(ctx context.Context) (int64, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, fmt.Errorf("fail to get metadata from the context")
	}
	identifierContent, ok := md[util.IdentifierKey]
	if !ok || len(identifierContent) < 1 {
		return 0, fmt.Errorf("no identifier found in metadata")
	}
	identifier, err := strconv.ParseInt(identifierContent[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse identifier: %s, error: %s", identifierContent[0], err.Error())
	}
	return identifier, nil
}

func KeepActiveInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	// We shouldn't block the normal rpc. though this may be not very accurate enough.
	// On the other hand, too many goroutines will also influence the rpc.
	// Not sure which way is better, since actually we already make the `keepActive` asynchronous.
	go func() {
		identifier, err := getIdentifierFromContext(ctx)
		if err == nil && funcutil.CheckCtxValid(ctx) {
			GetConnectionManager().keepActive(identifier)
		}
	}()

	return handler(ctx, req)
}
