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

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type rootCoordCatalogMigrationResult struct {
	Databases     int
	Collections   int
	Aliases       int
	FileResources int
}

func migrateRootCoordCatalogSnapshot(ctx context.Context, source metastore.RootCoordCatalog, target metastore.RootCoordCatalog, ts typeutil.Timestamp) (rootCoordCatalogMigrationResult, error) {
	var result rootCoordCatalogMigrationResult

	dbs, err := source.ListDatabases(ctx, typeutil.MaxTimestamp)
	if err != nil {
		return result, err
	}
	for _, db := range dbs {
		if err := target.CreateDatabase(ctx, db, ts); err != nil {
			return result, err
		}
		result.Databases++

		collections, err := source.ListCollections(ctx, db.ID, typeutil.MaxTimestamp)
		if err != nil {
			return result, err
		}
		for _, coll := range collections {
			if err := target.CreateCollection(ctx, coll, ts); err != nil {
				return result, err
			}
			result.Collections++
		}

		aliases, err := source.ListAliases(ctx, db.ID, typeutil.MaxTimestamp)
		if err != nil {
			return result, err
		}
		for _, alias := range aliases {
			if err := target.CreateAlias(ctx, alias, ts); err != nil {
				return result, err
			}
			result.Aliases++
		}
	}

	legacyCollections, err := source.ListCollections(ctx, 0, typeutil.MaxTimestamp)
	if err != nil {
		return result, err
	}
	for _, coll := range legacyCollections {
		if err := target.CreateCollection(ctx, coll, ts); err != nil {
			return result, err
		}
		result.Collections++
	}
	legacyAliases, err := source.ListAliases(ctx, 0, typeutil.MaxTimestamp)
	if err != nil {
		return result, err
	}
	for _, alias := range legacyAliases {
		if err := target.CreateAlias(ctx, alias, ts); err != nil {
			return result, err
		}
		result.Aliases++
	}

	resources, version, err := source.ListFileResource(ctx)
	if err != nil {
		return result, err
	}
	for _, resource := range resources {
		if err := target.SaveFileResource(ctx, resource, version); err != nil {
			return result, err
		}
		result.FileResources++
	}
	return result, nil
}
