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

package ddl

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
)

// GetContext builds an incoming context carrying the given authorization
// value, mirroring the root-package test helper of the same name.
func GetContext(ctx context.Context, originValue string) context.Context {
	authKey := strings.ToLower(util.HeaderAuthorize)
	authValue := crypto.Base64Encode(originValue)
	contextMap := map[string]string{
		authKey: authValue,
	}
	md := metadata.New(contextMap)
	return metadata.NewIncomingContext(ctx, md)
}

// mustNewSchemaInfo builds a schemaInfo for tests, panicking on the schema-helper
// error. Test schemas are valid so it never fires; it keeps call sites a
// single-value expression usable inside struct literals.
func mustNewSchemaInfo(schema *schemapb.CollectionSchema) *schemaInfo {
	si, err := metacache.NewSchemaInfo(schema)
	if err != nil {
		panic(err)
	}
	return si
}

// initMetaCache builds a meta cache over the given mixCoord client and seeds
// the privilege cache exactly like the root initializer, so tests that expect
// a ListPolicy call during cache construction behave identically. The root-only
// http registration is skipped.
func initMetaCache(ctx context.Context, mixCoord types.MixCoordClient) (Cache, error) {
	cache, err := metacache.NewMetaCache(mixCoord)
	if err != nil {
		return nil, err
	}
	if err := privilege.InitPrivilegeCache(ctx, mixCoord); err != nil {
		return nil, err
	}
	return cache, nil
}

// mustInitMetaCacheForTest builds a meta cache over the given mixCoord client,
// panicking on error.
func mustInitMetaCacheForTest(ctx context.Context, mixCoord types.MixCoordClient) Cache {
	cache, err := initMetaCache(ctx, mixCoord)
	if err != nil {
		panic(err)
	}
	return cache
}
