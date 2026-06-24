package connection

import (
	"context"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func ClientInfoFields(info *commonpb.ClientInfo) []mlog.Field {
	fields := []mlog.Field{
		mlog.String("sdk_type", info.GetSdkType()),
		mlog.String("sdk_version", info.GetSdkVersion()),
		mlog.String("local_time", info.GetLocalTime()),
		mlog.String("user", info.GetUser()),
		mlog.String("host", info.GetHost()),
	}

	for k, v := range info.GetReserved() {
		fields = append(fields, mlog.String(k, v))
	}

	return fields
}

func GetIdentifierFromContext(ctx context.Context) (int64, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, merr.WrapErrParameterInvalidMsg("fail to get metadata from the context")
	}
	identifierContent, ok := md[util.IdentifierKey]
	if !ok || len(identifierContent) < 1 {
		return 0, merr.WrapErrParameterInvalidMsg("no identifier found in metadata")
	}
	identifier, err := strconv.ParseInt(identifierContent[0], 10, 64)
	if err != nil {
		return 0, merr.WrapErrParameterInvalidMsg("failed to parse identifier: %s, error: %s", identifierContent[0], err.Error())
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
