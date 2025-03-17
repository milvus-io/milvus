// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package info

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/atomic"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

var ClusterPrefix atomic.String

func getCurUserFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("fail to get md from the context")
	}
	authorization, ok := md[strings.ToLower(util.HeaderAuthorize)]
	if !ok || len(authorization) < 1 {
		return "", fmt.Errorf("fail to get authorization from the md, authorize:[%s]", util.HeaderAuthorize)
	}
	token := authorization[0]
	rawToken, err := crypto.Base64Decode(token)
	if err != nil {
		return "", fmt.Errorf("fail to decode the token, token: %s", token)
	}
	secrets := strings.SplitN(rawToken, util.CredentialSeperator, 2)
	if len(secrets) < 2 {
		return "", fmt.Errorf("fail to get user info from the raw token, raw token: %s", rawToken)
	}
	username := secrets[0]
	return username, nil
}

func getSdkVersionByUserAgent(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Unknown
	}
	UserAgent, ok := md[util.HeaderUserAgent]
	if !ok {
		return Unknown
	}

	SdkType, ok := getSdkTypeByUserAgent(UserAgent)
	if !ok {
		return Unknown
	}

	return SdkType + "-" + Unknown
}

func getSdkTypeByUserAgent(userAgents []string) (string, bool) {
	if len(userAgents) == 0 {
		return "", false
	}

	userAgent := userAgents[0]
	switch {
	case strings.HasPrefix(userAgent, "grpc-node-js"):
		return "nodejs", true
	case strings.HasPrefix(userAgent, "grpc-python"):
		return "Python", true
	case strings.HasPrefix(userAgent, "grpc-go"):
		return "Golang", true
	case strings.HasPrefix(userAgent, "grpc-java"):
		return "Java", true
	default:
		return "", false
	}
}

func getAnnsFieldFromKvs(kvs []*commonpb.KeyValuePair) string {
	field, err := funcutil.GetAttrByKeyFromRepeatedKV("anns_field", kvs)
	if err != nil {
		return "default"
	}
	return field
}

func listToString(strs []string) string {
	result := "["
	for i, str := range strs {
		if i != 0 {
			result += ", "
		}
		result += "\"" + str + "\""
	}
	return result + "]"
}
