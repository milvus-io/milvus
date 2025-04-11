package proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
		return passwordVerify(ctx, username, password, globalMetaCache)
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
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, mix, mgr)
	assert.NoError(t, err)
	res = validAuth(ctx, []string{crypto.Base64Encode("mockUser:mockPass")})
	assert.True(t, res)

	res = validAuth(ctx, []string{crypto.Base64Encode("mock")})
	assert.False(t, res)
}

func TestValidSourceID(t *testing.T) {
	ctx := context.Background()
	// no metadata
	res := validSourceID(ctx, nil)
	assert.False(t, res)
	// illegal metadata
	res = validSourceID(ctx, []string{"invalid_sourceid"})
	assert.False(t, res)
	// normal sourceId
	res = validSourceID(ctx, []string{crypto.Base64Encode(util.MemberCredID)})
	assert.True(t, res)
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
	mgr := newShardClientMgr()
	err = InitMetaCache(ctx, queryCoord, mgr)
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
	// with valid sourceId
	md = metadata.Pairs("sourceid", crypto.Base64Encode(util.MemberCredID))
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
