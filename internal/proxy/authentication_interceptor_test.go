package proxy

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/util"

	"github.com/milvus-io/milvus/internal/util/crypto"
	"github.com/stretchr/testify/assert"
)

// validAuth validates the authentication
func TestValidAuth(t *testing.T) {
	ctx := context.Background()
	// no metadata
	res := validAuth(ctx, nil)
	assert.False(t, res)
	// illegal metadata
	res = validAuth(ctx, []string{"xxx"})
	assert.False(t, res)
	// normal metadata
	client := &MockRootCoordClientInterface{}
	err := InitMetaCache(client)
	assert.Nil(t, err)
	res = validAuth(ctx, []string{crypto.Base64Encode("mockUser:mockPass")})
	assert.True(t, res)
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
	// no metadata
	_, err := AuthenticationInterceptor(ctx)
	assert.NotNil(t, err)
	// mock metacache
	client := &MockRootCoordClientInterface{}
	err = InitMetaCache(client)
	assert.Nil(t, err)
	// with invalid metadata
	md := metadata.Pairs("xxx", "yyy")
	ctx = metadata.NewIncomingContext(ctx, md)
	_, err = AuthenticationInterceptor(ctx)
	assert.NotNil(t, err)
	// with valid username/password
	md = metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockUser:mockPass"))
	ctx = metadata.NewIncomingContext(ctx, md)
	_, err = AuthenticationInterceptor(ctx)
	assert.Nil(t, err)
	// with valid sourceId
	md = metadata.Pairs("sourceid", crypto.Base64Encode(util.MemberCredID))
	ctx = metadata.NewIncomingContext(ctx, md)
	_, err = AuthenticationInterceptor(ctx)
	assert.Nil(t, err)
}
