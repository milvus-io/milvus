package contextutil

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/crypto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

// Test UserInGrpcMetadata related function
func Test_UserInGrpcMetadata(t *testing.T) {
	testUser := "test_user_131"
	ctx := context.Background()
	assert.Equal(t, GetUserFromGrpcMetadata(ctx), "")

	ctx = WithUserInGrpcMetadata(ctx, testUser)
	assert.Equal(t, GetUserFromGrpcMetadata(ctx), testUser)

	md := metadata.Pairs(util.HeaderUser, crypto.Base64Encode(testUser))
	ctx = metadata.NewIncomingContext(context.Background(), md)
	assert.Equal(t, getUserFromGrpcMetadataAux(ctx, metadata.FromIncomingContext), testUser)

	ctx = PassthroughUserInGrpcMetadata(ctx)
	assert.Equal(t, getUserFromGrpcMetadataAux(ctx, metadata.FromOutgoingContext), testUser)
	assert.Equal(t, GetUserFromGrpcMetadata(ctx), testUser)
}
