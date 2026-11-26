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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func importPrivilegeDenied() error {
	// Do not reveal whether a job exists or the database/collection that owns it.
	return merr.WrapErrPrivilegeNotPermitted("not allowed to access import job")
}

// resolveImportResource uses the persisted ID, not a caller-selected database or
// a job's historical name.
func (node *Proxy) resolveImportResource(ctx context.Context, collectionID int64) (*collectionResource, error) {
	resource, err := node.resolveCollectionResource(ctx, collectionID, "import resource")
	if err == nil && resource == nil {
		return nil, importPrivilegeDenied()
	}
	return resource, err
}

func importSubject(ctx context.Context) (rbacSubject, error) {
	return resourceSubject(ctx)
}

func (node *Proxy) authorizeImportResource(ctx context.Context, collectionID int64, resource *collectionResource, subject rbacSubject) error {
	allowed, err := node.authorizeCollectionResource(ctx, collectionID, resource, subject, commonpb.ObjectPrivilege_PrivilegeImport.String(), "import resource")
	if err != nil {
		return err
	}
	if !allowed {
		return importPrivilegeDenied()
	}
	return nil
}

func (node *Proxy) filterImportJobs(ctx context.Context, dbName string, collectionID int64, resp *internalpb.ListImportsResponse) (*internalpb.ListImportsResponse, error) {
	n := len(resp.GetJobIDs())
	if len(resp.GetCollectionIds()) != n || len(resp.GetCollectionNames()) != n || len(resp.GetStates()) != n || len(resp.GetReasons()) != n || len(resp.GetProgresses()) != n {
		return nil, merr.WrapErrServiceUnavailable("import job response lacks aligned resource identities")
	}
	subject, err := importSubject(ctx)
	if err != nil {
		return nil, err
	}
	filtered := &internalpb.ListImportsResponse{Status: merr.Success()}
	if n == 0 {
		return filtered, nil
	}
	var selectedIDs map[int64]struct{}
	if collectionID == 0 {
		// DataCoord retains jobs across all databases, including deleted
		// collections. Select live IDs in this database in one metadata call
		// before doing per-resource lookups, rather than resolving every
		// historical collection in the cluster.
		collections, err := node.mixCoord.ShowCollections(describeCollectionMetadataContext(ctx), &milvuspb.ShowCollectionsRequest{
			Base:   commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections)),
			DbName: dbName,
		})
		if err := merr.CheckRPCCall(collections, err); err != nil {
			return nil, merr.Wrap(err, "select import resources")
		}
		selectedIDs = make(map[int64]struct{}, len(collections.GetCollectionIds()))
		for _, id := range collections.GetCollectionIds() {
			selectedIDs[id] = struct{}{}
		}
	}
	type decision struct {
		resource *collectionResource
		err      error
	}
	// Several jobs commonly belong to one collection. Resolve and authorize it
	// once per request, without retaining decisions across revocations or DDL.
	decisions := make(map[int64]decision)
	for i, id := range resp.GetCollectionIds() {
		if collectionID != 0 && id != collectionID {
			continue
		}
		if selectedIDs != nil {
			if _, selected := selectedIDs[id]; !selected {
				continue
			}
		}
		d, ok := decisions[id]
		if !ok {
			d.resource, d.err = node.resolveImportResource(ctx, id)
			if d.err == nil && d.resource.dbName == dbName {
				d.err = node.authorizeImportResource(ctx, id, d.resource, subject)
			}
			decisions[id] = d
		}
		if errors.Is(d.err, merr.ErrPrivilegeNotPermitted) {
			continue
		}
		if d.err != nil {
			return nil, d.err
		}
		if d.resource.dbName != dbName {
			continue
		}
		filtered.JobIDs = append(filtered.JobIDs, resp.GetJobIDs()[i])
		filtered.CollectionIds = append(filtered.CollectionIds, id)
		filtered.CollectionNames = append(filtered.CollectionNames, d.resource.collectionName)
		filtered.States = append(filtered.States, resp.GetStates()[i])
		filtered.Reasons = append(filtered.Reasons, resp.GetReasons()[i])
		filtered.Progresses = append(filtered.Progresses, resp.GetProgresses()[i])
	}
	return filtered, nil
}
