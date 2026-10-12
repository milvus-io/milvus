package proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// validAuth validates the authentication
func TestValidAuth(t *testing.T) {
	validAuth := func(ctx context.Context, authorization []string) bool {
		if len(authorization) < 1 {
			return false
		}
		token := authorization[0]
		rawToken, _ := crypto.Base64Decode(token)
		username, password := parseMD(rawToken)
		if username == "" || password == "" {
			return false
		}
		return passwordVerify(ctx, username, password, privilege.GetPrivilegeCache())
	}

	ctx := context.Background()
	// no metadata
	res := validAuth(ctx, nil)
	assert.False(t, res)
	// illegal metadata
	res = validAuth(ctx, []string{"xxx"})
	assert.False(t, res)
	// normal metadata
	mix := &MockMixCoordClientInterface{}
	_, err := initMetaCache(ctx, mix)
	assert.NoError(t, err)
	res = validAuth(ctx, []string{crypto.Base64Encode("mockUser:mockPass")})
	assert.True(t, res)

	res = validAuth(ctx, []string{crypto.Base64Encode("mock")})
	assert.False(t, res)
}

func TestAuthenticationInterceptor(t *testing.T) {
	ctx := context.Background()
	paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true") // mock authorization is turned on
	defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)  // mock authorization is turned on

	// proxy not ready: requests must be rejected with ServiceUnavailable
	notReady := AuthenticationInterceptorWithMetaCache(func() Cache { return nil })
	mdNoMeta := metadata.NewIncomingContext(ctx, metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockUser:mockPass")))
	_, err := notReady(mdNoMeta)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceUnavailable))

	// no metadata
	_, err = notReady(ctx)
	assert.Error(t, err)
	// mock metacache
	queryCoord := &MockMixCoordClientInterface{}
	cache, err := initMetaCache(ctx, queryCoord)
	assert.NoError(t, err)
	authInterceptor := AuthenticationInterceptorWithMetaCache(func() Cache { return cache })
	// with invalid metadata
	md := metadata.Pairs("xxx", "yyy")
	ctx = metadata.NewIncomingContext(ctx, md)
	_, err = authInterceptor(ctx)
	assert.Error(t, err)
	// with valid username/password
	md = metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockUser:mockPass"))
	ctx = metadata.NewIncomingContext(ctx, md)
	_, err = authInterceptor(ctx)
	assert.NoError(t, err)

	{
		// wrong authorization style
		md = metadata.Pairs(util.HeaderAuthorize, "123456")
		ctx = metadata.NewIncomingContext(ctx, md)
		_, err = authInterceptor(ctx)
		assert.Error(t, err)
	}

	{
		// invalid user
		md = metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockUser2:mockPass"))
		ctx = metadata.NewIncomingContext(ctx, md)
		_, err = authInterceptor(ctx)
		assert.Error(t, err)
	}

	{
		// default hook
		md = metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockapikey"))
		ctx = metadata.NewIncomingContext(ctx, md)
		_, err = authInterceptor(ctx)
		assert.Error(t, err)
	}

	{
		// verify apikey error
		hookutil.SetMockAPIHook("", errors.New("err"))
		md = metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockapikey"))
		ctx = metadata.NewIncomingContext(ctx, md)
		_, err = authInterceptor(ctx)
		assert.Error(t, err)
	}

	{
		hookutil.SetMockAPIHook("mockUser", nil)
		md = metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockapikey"))
		ctx = metadata.NewIncomingContext(ctx, md)
		authCtx, err := authInterceptor(ctx)
		assert.NoError(t, err)
		md, ok := metadata.FromIncomingContext(authCtx)
		assert.True(t, ok)
		authStrArr := md[strings.ToLower(util.HeaderAuthorize)]
		token := authStrArr[0]
		rawToken, err := crypto.Base64Decode(token)
		assert.NoError(t, err)
		user, _ := parseMD(rawToken)
		assert.Equal(t, "mockUser", user)
	}
	hookutil.SetTestHook(hookutil.DefaultHook{})
}

// streamForAuth is a minimal ServerStream carrying only the context the
// interceptor reads and rewrites.
type streamForAuth struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *streamForAuth) Context() context.Context { return s.ctx }

type ctxKeyAuthedUser struct{}

// The stream interceptor is the unary one's counterpart: a failed
// authentication ends the stream before the handler exists, and a successful
// one hands the handler the authenticated context through ss.Context() - the
// only channel a stream handler has - rather than the raw incoming one.
func TestGrpcAuthStreamInterceptor(t *testing.T) {
	info := &grpc.StreamServerInfo{FullMethod: "/milvus.proto.milvus.MilvusService/CreateReplicateStream"}

	t.Run("a refused credential ends the stream unhandled", func(t *testing.T) {
		intercept := GrpcAuthStreamInterceptor(func(ctx context.Context) (context.Context, error) {
			return nil, status.Error(codes.Unauthenticated, "no")
		})
		handled := false
		err := intercept(struct{}{}, &streamForAuth{ctx: context.Background()}, info,
			func(any, grpc.ServerStream) error { handled = true; return nil })
		assert.Error(t, err)
		assert.False(t, handled, "an unauthenticated stream must never reach its handler")
	})

	t.Run("the handler reads the authenticated context", func(t *testing.T) {
		intercept := GrpcAuthStreamInterceptor(func(ctx context.Context) (context.Context, error) {
			return context.WithValue(ctx, ctxKeyAuthedUser{}, "alice"), nil
		})
		var seen any
		err := intercept(struct{}{}, &streamForAuth{ctx: context.Background()}, info,
			func(_ any, ss grpc.ServerStream) error {
				seen = ss.Context().Value(ctxKeyAuthedUser{})
				return nil
			})
		assert.NoError(t, err)
		assert.Equal(t, "alice", seen,
			"the rewritten context is the only way user identity reaches a stream handler")
	})
}
