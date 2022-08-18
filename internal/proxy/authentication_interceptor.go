package proxy

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/util"

	"github.com/milvus-io/milvus/internal/util/crypto"
)

// validAuth validates the authentication
func validAuth(ctx context.Context, authorization []string) bool {
	if len(authorization) < 1 {
		//log.Warn("key not found in header", zap.String("key", headerAuthorize))
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

	return passwordVerify(ctx, username, password, globalMetaCache)
}

func validSourceID(ctx context.Context, authorization []string) bool {
	if len(authorization) < 1 {
		//log.Warn("key not found in header", zap.String("key", util.HeaderSourceID))
		return false
	}
	// token format: base64<sourceID>
	token := authorization[0]
	sourceID, err := crypto.Base64Decode(token)
	if err != nil {
		return false
	}
	return sourceID == util.MemberCredID
}

// AuthenticationInterceptor verify based on kv pair <"authorization": "token"> in header
func AuthenticationInterceptor(ctx context.Context) (context.Context, error) {
	// The keys within metadata.MD are normalized to lowercase.
	// See: https://godoc.org/google.golang.org/grpc/metadata#New
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, ErrMissingMetadata()
	}
	if globalMetaCache == nil {
		return nil, ErrProxyNotReady()
	}
	// check:
	//	1. if rpc call from a member (like index/query/data component)
	// 	2. if rpc call from sdk
	if Params.CommonCfg.AuthorizationEnabled {
		if !validSourceID(ctx, md[strings.ToLower(util.HeaderSourceID)]) &&
			!validAuth(ctx, md[strings.ToLower(util.HeaderAuthorize)]) {
			return nil, ErrUnauthenticated()
		}
	}
	return ctx, nil
}
