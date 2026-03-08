package connection

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

func ZapClientInfo(info *commonpb.ClientInfo) []zap.Field {
	fields := []zap.Field{
		zap.String("sdk_type", info.GetSdkType()),
		zap.String("sdk_version", info.GetSdkVersion()),
		zap.String("local_time", info.GetLocalTime()),
		zap.String("user", info.GetUser()),
		zap.String("host", info.GetHost()),
	}

	for k, v := range info.GetReserved() {
		fields = append(fields, zap.String(k, v))
	}

	return fields
}

func GetIdentifierFromContext(ctx context.Context) (int64, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, errors.New("fail to get metadata from the context")
	}
	identifierContent, ok := md[util.IdentifierKey]
	if !ok || len(identifierContent) < 1 {
		return 0, errors.New("no identifier found in metadata")
	}
	identifier, err := strconv.ParseInt(identifierContent[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse identifier: %s, error: %s", identifierContent[0], err.Error())
	}
	return identifier, nil
}

func KeepActiveInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	// We shouldn't block the normal rpc. though this may be not very accurate enough.
	// On the other hand, too many goroutines will also influence the rpc.
	// Not sure which way is better, since actually we already make the `keepActive` asynchronous.
	go func() {
		identifier, err := GetIdentifierFromContext(ctx)
		if err == nil && funcutil.CheckCtxValid(ctx) {
			GetManager().KeepActive(identifier)
		}
	}()

	return handler(ctx, req)
}
