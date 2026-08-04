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

package rootcoord

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (c *Core) RootCoordCatalogCutover(ctx context.Context, req *rootcoordpb.RootCoordCatalogCutoverRequest) (*rootcoordpb.RootCoordCatalogCutoverResponse, error) {
	resp := &rootcoordpb.RootCoordCatalogCutoverResponse{}
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	if req == nil {
		resp.Status = merr.Status(merr.WrapErrParameterMissing("RootCoordCatalogCutoverRequest"))
		return resp, nil
	}
	if req.GetCatalogServiceAddress() == "" {
		resp.Status = merr.Status(merr.WrapErrParameterInvalidMsg("catalog service address is required"))
		return resp, nil
	}

	drainCtx := ctx
	cancel := func() {}
	if req.GetDrainTimeoutMs() > 0 {
		drainCtx, cancel = context.WithTimeout(ctx, time.Duration(req.GetDrainTimeoutMs())*time.Millisecond)
	}
	defer cancel()

	c.startCatalogMigrationDraining()
	defer c.resumeCatalogMigrationWrites()
	if err := c.waitCatalogMigrationDrained(drainCtx); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	namespace := req.GetCatalogServiceNamespace()
	if namespace == "" {
		namespace = paramtable.Get().CommonCfg.ClusterName.GetValue()
	}
	targetCatalog, targetConn, err := c.newCatalogServiceRootCoordCatalogWithConfig(ctx, req.GetCatalogServiceAddress(), namespace)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	targetConnCommitted := false
	defer func() {
		if !targetConnCommitted {
			_ = targetConn.Close()
		}
	}()

	cutoverTs := typeutil.Timestamp(req.GetCutoverTs())
	if cutoverTs == 0 {
		if c.tsoAllocator == nil {
			resp.Status = merr.Status(merr.WrapErrParameterInvalidMsg("cutover ts is required when tso allocator is unavailable"))
			return resp, nil
		}
		generated, err := c.tsoAllocator.GenerateTSO(1)
		if err != nil {
			resp.Status = merr.Status(err)
			return resp, nil
		}
		cutoverTs = generated
	}

	meta, ok := c.meta.(*MetaTable)
	if !ok {
		resp.Status = merr.Status(merr.WrapErrServiceInternalMsg("rootcoord meta table does not support online catalog cutover"))
		return resp, nil
	}
	result, err := meta.CutoverCatalog(ctx, targetCatalog, cutoverTs)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	oldConn := c.catalogServiceConn
	c.catalogServiceConn = targetConn
	targetConnCommitted = true
	if oldConn != nil && oldConn != targetConn {
		_ = oldConn.Close()
	}

	cacheExpireTs := typeutil.Timestamp(req.GetCacheExpireTs())
	if cacheExpireTs == 0 {
		cacheExpireTs = cutoverTs
	}
	if err := c.expireRootCoordCatalogCutoverMetaCache(ctx, cacheExpireTs); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.Status = merr.Success()
	resp.Databases = int32(result.Databases)
	resp.Collections = int32(result.Collections)
	resp.Aliases = int32(result.Aliases)
	resp.FileResources = int32(result.FileResources)
	return resp, nil
}

func (c *Core) expireRootCoordCatalogCutoverMetaCache(ctx context.Context, ts typeutil.Timestamp) error {
	if c.proxyClientManager == nil || c.session == nil || c.meta == nil {
		return nil
	}
	dbs, err := c.meta.ListDatabases(ctx, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	for _, db := range dbs {
		if db == nil {
			continue
		}
		collections, err := c.meta.ListCollections(ctx, db.Name, typeutil.MaxTimestamp, false)
		if err != nil {
			return err
		}
		for _, coll := range collections {
			if coll == nil {
				continue
			}
			aliases, err := c.meta.ListAliases(ctx, db.Name, coll.Name, typeutil.MaxTimestamp)
			if err != nil {
				return err
			}
			if err := c.ExpireTransferMetaCache(ctx, db.Name, coll.Name, aliases, coll.CollectionID, ts); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ interface {
	RootCoordCatalogCutover(context.Context, *rootcoordpb.RootCoordCatalogCutoverRequest) (*rootcoordpb.RootCoordCatalogCutoverResponse, error)
} = (*Core)(nil)
