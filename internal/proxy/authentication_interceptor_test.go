package proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// mockServerStream is a minimal grpc.ServerStream implementation that only carries a
// context, which is all the stream authentication interceptor needs.
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

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
	err := InitMetaCache(ctx, mix)
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
	// no metadata
	_, err := AuthenticationInterceptor(ctx)
	assert.Error(t, err)
	// mock metacache
	queryCoord := &MockMixCoordClientInterface{}
	err = InitMetaCache(ctx, queryCoord)
	assert.NoError(t, err)
	// with invalid metadata
	md := metadata.Pairs("xxx", "yyy")
	ctx = metadata.NewIncomingContext(ctx, md)
	_, err = AuthenticationInterceptor(ctx)
	assert.Error(t, err)
	// with valid username/password
	md = metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockUser:mockPass"))
	ctx = metadata.NewIncomingContext(ctx, md)
	_, err = AuthenticationInterceptor(ctx)
	assert.NoError(t, err)

	{
		// wrong authorization style
		md = metadata.Pairs(util.HeaderAuthorize, "123456")
		ctx = metadata.NewIncomingContext(ctx, md)
		_, err = AuthenticationInterceptor(ctx)
		assert.Error(t, err)
	}

	{
		// invalid user
		md = metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockUser2:mockPass"))
		ctx = metadata.NewIncomingContext(ctx, md)
		_, err = AuthenticationInterceptor(ctx)
		assert.Error(t, err)
	}

	{
		// default hook
		md = metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockapikey"))
		ctx = metadata.NewIncomingContext(ctx, md)
		_, err = AuthenticationInterceptor(ctx)
		assert.Error(t, err)
	}

	{
		// verify apikey error
		hookutil.SetMockAPIHook("", errors.New("err"))
		md = metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockapikey"))
		ctx = metadata.NewIncomingContext(ctx, md)
		_, err = AuthenticationInterceptor(ctx)
		assert.Error(t, err)
	}

	{
		hookutil.SetMockAPIHook("mockUser", nil)
		md = metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockapikey"))
		ctx = metadata.NewIncomingContext(ctx, md)
		authCtx, err := AuthenticationInterceptor(ctx)
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

func TestGrpcAuthStreamInterceptor(t *testing.T) {
	paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true") // mock authorization is turned on
	defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	// mock metacache so AuthenticationInterceptor can verify credentials
	mix := &MockMixCoordClientInterface{}
	err := InitMetaCache(context.Background(), mix)
	assert.NoError(t, err)

	info := &grpc.StreamServerInfo{FullMethod: "/milvus.proto.milvus.MilvusService/CreateReplicateStream"}
	interceptor := GrpcAuthStreamInterceptor(AuthenticationInterceptor)

	t.Run("reject stream without authorization", func(t *testing.T) {
		called := false
		handler := func(srv interface{}, ss grpc.ServerStream) error {
			called = true
			return nil
		}
		ss := &mockServerStream{ctx: context.Background()}
		err := interceptor(nil, ss, info, handler)
		// authentication must fail and the handler must never be reached
		assert.Error(t, err)
		assert.False(t, called, "handler should not be invoked when authentication fails")
	})

	t.Run("reject stream with invalid credential", func(t *testing.T) {
		called := false
		handler := func(srv interface{}, ss grpc.ServerStream) error {
			called = true
			return nil
		}
		md := metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockUser:wrongPass"))
		ctx := metadata.NewIncomingContext(context.Background(), md)
		ss := &mockServerStream{ctx: ctx}
		err := interceptor(nil, ss, info, handler)
		assert.Error(t, err)
		assert.False(t, called, "handler should not be invoked when credential is invalid")
	})

	t.Run("accept stream with valid credential and propagate context", func(t *testing.T) {
		called := false
		var handlerCtx context.Context
		handler := func(srv interface{}, ss grpc.ServerStream) error {
			called = true
			handlerCtx = ss.Context()
			return nil
		}
		md := metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockUser:mockPass"))
		ctx := metadata.NewIncomingContext(context.Background(), md)
		ss := &mockServerStream{ctx: ctx}
		err := interceptor(nil, ss, info, handler)
		assert.NoError(t, err)
		assert.True(t, called, "handler should be invoked for an authenticated stream")
		// the wrapped stream must expose the authenticated context to the handler
		assert.NotNil(t, handlerCtx)
	})
}
