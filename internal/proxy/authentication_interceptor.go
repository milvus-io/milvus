package proxy

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func parseMD(authorization []string) (username, password string) {
	if len(authorization) < 1 {
		log.Warn("key not found in header")
		return
	}
	// token format: base64<username:password>
	// token := strings.TrimPrefix(authorization[0], "Bearer ")
	token := authorization[0]
	rawToken, err := crypto.Base64Decode(token)
	if err != nil {
		log.Warn("fail to decode the token", zap.Error(err))
		return
	}
	secrets := strings.SplitN(rawToken, util.CredentialSeperator, 2)
	if len(secrets) < 2 {
		log.Warn("invalid token format, length of secrets less than 2")
		return
	}
	username = secrets[0]
	password = secrets[1]
	return
}

func validSourceID(ctx context.Context, authorization []string) bool {
	if len(authorization) < 1 {
		// log.Warn("key not found in header", zap.String("key", util.HeaderSourceID))
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
		return nil, merr.WrapErrIoKeyNotFound("metadata", "auth check failure, due to occurs inner error: missing metadata")
	}
	if globalMetaCache == nil {
		return nil, merr.WrapErrServiceUnavailable("internal: Milvus Proxy is not ready yet. please wait")
	}
	// check:
	//	1. if rpc call from a member (like index/query/data component)
	// 	2. if rpc call from sdk
	if Params.CommonCfg.AuthorizationEnabled.GetAsBool() {
		if !validSourceID(ctx, md[strings.ToLower(util.HeaderSourceID)]) {
			username, password := parseMD(md[strings.ToLower(util.HeaderAuthorize)])
			if !passwordVerify(ctx, username, password, globalMetaCache) {
				msg := fmt.Sprintf("username: %s, password: %s", username, password)
				return nil, merr.WrapErrParameterInvalid("vaild username and password", msg, "auth check failure, please check username and password are correct")
			}
			metrics.UserRPCCounter.WithLabelValues(username).Inc()
		}
	}
	return ctx, nil
}
