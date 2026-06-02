package proxy

import (
	"context"
	"fmt"
	"strings"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func parseMD(rawToken string) (username, password string) {
	secrets := strings.SplitN(rawToken, util.CredentialSeparator, 2)
	if len(secrets) < 2 {
		mlog.Warn(context.TODO(), "invalid token format, length of secrets less than 2")
		return username, password
	}
	username = secrets[0]
	password = secrets[1]
	return username, password
}

func GrpcAuthInterceptor(authFunc grpc_auth.AuthFunc) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		var newCtx context.Context
		var err error
		if overrideSrv, ok := info.Server.(grpc_auth.ServiceAuthFuncOverride); ok {
			newCtx, err = overrideSrv.AuthFuncOverride(ctx, info.FullMethod)
		} else {
			newCtx, err = authFunc(ctx)
		}
		if err != nil {
			hookutil.GetExtension().ReportAction(context.Background(), req, &milvuspb.BoolResponse{
				Status: merr.Status(err),
			}, err, info.FullMethod, hookutil.ActionAuthorize)
			return nil, err
		}
		return handler(newCtx, req)
	}
}

// AuthenticationInterceptor verify based on kv pair <"authorization": "token"> in header
func AuthenticationInterceptor(ctx context.Context) (context.Context, error) {
	// The keys within metadata.MD are normalized to lowercase.
	// See: https://godoc.org/google.golang.org/grpc/metadata#New
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, merr.WrapErrIoKeyNotFound("metadata", "auth check failure, due to occurs inner error: missing metadata")
	}
	if globalMetaCache == nil {
		return nil, merr.WrapErrServiceUnavailable("internal: Milvus Proxy is not ready yet. please wait")
	}
	// check rpc call from sdk
	if Params.CommonCfg.AuthorizationEnabled.GetAsBool() {
		authStrArr := md[strings.ToLower(util.HeaderAuthorize)]

		if len(authStrArr) < 1 {
			mlog.Warn(ctx, "key not found in header")
			return nil, status.Error(codes.Unauthenticated, "missing authorization in header")
		}

		// token format: base64<username:password>
		// token := strings.TrimPrefix(authorization[0], "Bearer ")
		token := authStrArr[0]
		rawToken, err := crypto.Base64Decode(token)
		if err != nil {
			mlog.Warn(ctx, "fail to decode the token", mlog.Err(err))
			return nil, status.Error(codes.Unauthenticated, "invalid token format")
		}

		if !strings.Contains(rawToken, util.CredentialSeparator) {
			user, err := VerifyAPIKey(rawToken)
			if err != nil {
				mlog.Warn(ctx, "fail to verify apikey", mlog.Err(err))
				return nil, status.Error(codes.Unauthenticated, "auth check failure, please check api key is correct")
			}
			metrics.UserRPCCounter.WithLabelValues(user).Inc()
			userToken := fmt.Sprintf("%s%s%s", user, util.CredentialSeparator, util.PasswordHolder)
			md[strings.ToLower(util.HeaderAuthorize)] = []string{crypto.Base64Encode(userToken)}
			md[util.HeaderToken] = []string{rawToken}
			ctx = metadata.NewIncomingContext(ctx, md)
		} else {
			// username+password authentication
			username, password := parseMD(rawToken)
			if !passwordVerify(ctx, username, password, privilege.GetPrivilegeCache()) {
				mlog.Warn(ctx, "fail to verify password", mlog.String("username", username))
				// NOTE: don't use the merr, because it will cause the wrong retry behavior in the sdk
				return nil, status.Error(codes.Unauthenticated, "auth check failure, please check username and password are correct")
			}
			metrics.UserRPCCounter.WithLabelValues(username).Inc()
		}
	}
	return ctx, nil
}
