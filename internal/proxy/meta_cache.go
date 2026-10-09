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

package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	internalhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/adminauth"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Cache is the interface for system metadata cache.
//
//go:generate mockery --name=Cache --filename=mock_cache_test.go --outpkg=proxy --output=. --inpackage --structname=MockCache --with-expecter
type Cache = metacache.Cache

type (
	MetaCache      = metacache.MetaCache
	collectionInfo = metacache.CollectionInfo
	databaseInfo   = metacache.DatabaseInfo
	schemaInfo     = metacache.SchemaInfo
	partitionInfo  = metacache.PartitionInfo
	partitionInfos = metacache.PartitionInfos
)

func NewMetaCache(mixCoord types.MixCoordClient) (*MetaCache, error) {
	return metacache.NewMetaCache(mixCoord)
}

func newSchemaInfo(schema *schemapb.CollectionSchema) (*schemaInfo, error) {
	return metacache.NewSchemaInfo(schema)
}

func normalizeDBName(database string) string {
	return metacache.NormalizeDBName(database)
}

func buildPartitionCacheKey(collectionID UniqueID) string {
	return metacache.BuildPartitionCacheKey(collectionID)
}

func parsePartitionsInfo(infos []*partitionInfo, hasPartitionKey bool) *partitionInfos {
	return metacache.ParsePartitionsInfo(infos, hasPartitionKey)
}

func initMetaCache(ctx context.Context, mixCoord types.MixCoordClient) (Cache, error) {
	metaCache, err := metacache.NewMetaCache(mixCoord)
	if err != nil {
		return nil, err
	}

	err = privilege.InitPrivilegeCache(ctx, mixCoord)
	if err != nil {
		mlog.Error(context.TODO(), "failed to init privilege cache", mlog.Err(err))
		return nil, err
	}

	internalhttp.RegisterPasswordVerifyFunc(PasswordVerify)
	internalhttp.RegisterGetUserRoleFunc(GetRole)

	return metaCache, nil
}

// newManagementRootVerifier builds the proxy's management-plane credential
// verifier. It is a plain constructor: Proxy.Init holds the result and
// registers it, so nothing here outlives a Proxy.
//
// It deliberately does not reuse the proxy's privilege cache: that cache only
// stores a credential after a password has already matched, so a failed
// attempt is never cached and every wrong password would issue a GetCredential
// RPC that the coordinator serves with an unconditional metastore read. The
// gated endpoints answer unauthenticated callers, which would make that an
// anonymous etcd amplifier.
func newManagementRootVerifier(mixCoord types.MixCoordClient) *adminauth.CachedRootVerifier {
	return adminauth.NewCachedRootVerifier(func(ctx context.Context) (string, error) {
		resp, err := mixCoord.GetCredential(ctx, &rootcoordpb.GetCredentialRequest{
			Username: util.UserRoot,
		})
		if err != nil {
			return "", merr.Wrap(err, "GetCredential failed")
		}
		return adminauth.RootHashFromResponse(resp)
	})
}
