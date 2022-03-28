package proxy

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus/internal/util/crypto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// valid validates the authorization
func valid(ctx context.Context, authorization []string) bool {
	if len(authorization) < 1 {
		return false
	}
	// token format: base64<username:password>
	//token := strings.TrimPrefix(authorization[0], "Bearer ")
	token := authorization[0]
	rawToken, err := crypto.Base64Decode(token)
	if err != nil {
		return false
	}
	secrets := strings.SplitN(rawToken, ":", 2)
	username := secrets[0]
	password := secrets[1]

	credInfo, err := globalMetaCache.GetCredentialInfo(ctx, username)
	if err != nil {
		return false
	}

	return crypto.PasswordVerify(password, credInfo.password)
}

// AuthInterceptor ensures a valid token exists within a request's metadata
func AuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, ErrMissingMetadata()
	}
	// The keys within metadata.MD are normalized to lowercase.
	// See: https://godoc.org/google.golang.org/grpc/metadata#New
	if !valid(ctx, md["authorization"]) {
		return nil, ErrUnauthenticated()
	}
	// Continue execution of handler after ensuring a valid token.
	return handler(ctx, req)
}
