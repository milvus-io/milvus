// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package requestcontext

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util"
)

type authenticatedUserKey struct{}

// WithAuthenticatedUser records only a principal verified by server-side
// authentication. Request metadata and UDF params do not populate this key.
func WithAuthenticatedUser(ctx context.Context, user string) context.Context {
	return context.WithValue(ctx, authenticatedUserKey{}, user)
}

// AuthenticatedUser returns empty for anonymous/internal calls. It does not
// decode authorization headers or infer an identity from a global cache.
func AuthenticatedUser(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	user, _ := ctx.Value(authenticatedUserKey{}).(string)
	return user
}

// FunctionScope snapshots the resolved target and verified caller. Database
// and collection are supplied by endpoint adapters after metadata resolution;
// ResourceName is supplied by the chain for each operator invocation.
func FunctionScope(ctx context.Context, database, collection string) types.RequestContext {
	if database == "" {
		database = util.DefaultDBName
	}
	return types.RequestContext{Database: database, Collection: collection, User: AuthenticatedUser(ctx)}
}

func WithFunctionScope(ctx context.Context, database, collection string) context.Context {
	return types.WithRequestContext(ctx, FunctionScope(ctx, database, collection))
}
