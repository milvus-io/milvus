package proxy

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus/internal/util"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/crypto"

	"google.golang.org/grpc/metadata"
)

// valid validates the authorization
func valid(ctx context.Context, authorization []string) bool {
	if len(authorization) < 1 {
		log.Error("key 'authorization' not found in header")
		return false
	}
	// token format: base64<username:password>
	//token := strings.TrimPrefix(authorization[0], "Bearer ")
	token := authorization[0]
	rawToken, err := crypto.Base64Decode(token)
	if err != nil {
		return false
	}
	secrets := strings.SplitN(rawToken, util.CredentialSeperator, 2)
	username := secrets[0]
	password := secrets[1]

	// if rpc call from a member (like index/query/data component)
	if username == util.MemberCredUsername && password == util.MemberCredPassword {
		return true
	}

	// if rpc call from sdk
	credInfo, err := globalMetaCache.GetCredentialInfo(ctx, username)
	if err != nil {
		log.Error("found no credential", zap.String("username", username), zap.Error(err))
		return false
	}

	return crypto.PasswordVerify(password, credInfo.password)
}

// AuthenticationInterceptor verify based on kv pair <"authorization": "token"> in header
func AuthenticationInterceptor(ctx context.Context) (context.Context, error) {
	usernames, _ := globalMetaCache.GetCredUsernames(ctx)
	if usernames != nil && len(usernames) > 0 {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, ErrMissingMetadata()
		}
		// The keys within metadata.MD are normalized to lowercase.
		// See: https://godoc.org/google.golang.org/grpc/metadata#New
		if !valid(ctx, md["authorization"]) {
			return nil, ErrUnauthenticated()
		}
	}
	return ctx, nil
}
