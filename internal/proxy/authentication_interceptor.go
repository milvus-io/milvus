package proxy

import (
	"context"
	"fmt"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
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

// GrpcAuthStreamInterceptor is the streaming counterpart of GrpcAuthInterceptor.
// The external gRPC server only mounts UnaryInterceptor, so streaming RPCs such as
// CreateReplicateStream/DumpMessages would otherwise skip the authentication chain
// entirely. This interceptor authenticates streaming calls with the same logic as
// unary calls before the handler observes the stream, and propagates the
// authenticated context (carrying the resolved user/token) to the handler.
func GrpcAuthStreamInterceptor(authFunc grpc_auth.AuthFunc) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		var newCtx context.Context
		var err error
		if overrideSrv, ok := srv.(grpc_auth.ServiceAuthFuncOverride); ok {
			newCtx, err = overrideSrv.AuthFuncOverride(ss.Context(), info.FullMethod)
		} else {
			newCtx, err = authFunc(ss.Context())
		}
		if err != nil {
			hookutil.GetExtension().ReportAction(context.Background(), nil, &milvuspb.BoolResponse{
				Status: merr.Status(err),
			}, err, info.FullMethod, hookutil.ActionAuthorize)
			return err
		}
		// Propagate the authenticated context to the handler by wrapping the ServerStream.
		wrapped := grpc_middleware.WrapServerStream(ss)
		wrapped.WrappedContext = newCtx
		return handler(srv, wrapped)
	}
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

// AuthenticationInterceptorWithMetaCache returns an authentication interceptor
// that verifies request identity against the injected meta cache. It also acts
// as a readiness gate: until the proxy has published its meta cache (in
// Proxy.Init), all requests are rejected with ServiceUnavailable, mirroring the
// previous globalMetaCache == nil check that this PR's per-proxy cache removed.
func AuthenticationInterceptorWithMetaCache(getMetaCache func() Cache) grpc_auth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		// The keys within metadata.MD are normalized to lowercase.
		// See: https://godoc.org/google.golang.org/grpc/metadata#New
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, merr.WrapErrIoKeyNotFound("metadata", "auth check failure, due to occurs inner error: missing metadata")
		}
		if getMetaCache() == nil {
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
				// Extension seam, see extension_seam.go: false with no verifier
				// installed, so the native username and password path applies.
				if Params.CommonCfg.RequireAPIKey.GetAsBool() {
					mlog.Warn(ctx, "rejecting username and password authentication because the installed verifier requires api keys")
					return nil, status.Error(codes.Unauthenticated, "auth check failure, please check username and password are correct")
				}
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
}
