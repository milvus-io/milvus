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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type renameCollectionTask struct {
	baseTask
	Req *milvuspb.RenameCollectionRequest
}

func (t *renameCollectionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_RenameCollection); err != nil {
		return err
	}

	// Check if renaming across databases with encryption enabled
	if t.Req.GetNewDBName() != "" && t.Req.GetNewDBName() != t.Req.GetDbName() {
		if err := t.validateEncryption(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (t *renameCollectionTask) Execute(ctx context.Context) error {
	// 1. Use defaultDB if oldDBName is empty
	// If DBName changed:
	//  1.1 Check target DB exsits
	//  1.2 Check that new or old DBs are not encrypted
	// If CollectionName changed:
	// 2. Check Old Collection Exists
	// 3. Check both new and old collections are not aliases
	// 4. Check no-alias for old collection
	// 5. Check New Collection for target DB doesn't exsits
	if t.core.meta.IsAlias(ctx, t.Req.GetDbName(), t.Req.GetOldName()) {
		return fmt.Errorf("unsupported use an alias to rename collection, alias: %s", t.Req.GetOldName())
	}

	// Get collection ID
	collID := t.core.meta.GetCollectionID(ctx, t.Req.GetDbName(), t.Req.GetOldName())
	if collID == 0 {
		return fmt.Errorf("collection %s not found in database %s", t.Req.GetOldName(), t.Req.GetDbName())
	}

	targetDB := t.Req.GetNewDBName()
	if targetDB == "" {
		targetDB = t.Req.GetDbName()
	}

	// Check if new name is an existing alias in target database
	if t.core.meta.IsAlias(ctx, targetDB, t.Req.GetNewName()) {
		return fmt.Errorf("cannot rename collection to an existing alias: %s", t.Req.GetNewName())
	}

	// Check if new name already exists as a collection in target database
	if existingCollID := t.core.meta.GetCollectionID(ctx, targetDB, t.Req.GetNewName()); existingCollID != 0 {
		return fmt.Errorf("duplicated new collection name %s:%s with other collection name or alias", targetDB, t.Req.GetNewName())
	}

	aliases := t.core.meta.ListAliasesByID(ctx, collID)

	// Check if renaming across databases with aliases
	if len(aliases) > 0 && targetDB != t.Req.GetDbName() {
		return fmt.Errorf("fail to rename collection to different database, must drop all aliases of collection %s before rename", t.Req.GetOldName())
	}

	ts := t.GetTs()

	redoTask := newBaseRedoTask(t.core.stepExecutor)

	// Step 1: Rename collection in metadata catalog
	redoTask.AddSyncStep(&renameCollectionStep{
		baseStep:  baseStep{core: t.core},
		dbName:    t.Req.GetDbName(),
		oldName:   t.Req.GetOldName(),
		newDBName: t.Req.GetNewDBName(),
		newName:   t.Req.GetNewName(),
		ts:        ts,
	})

	// Step 2: Expire cache for old collection name
	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: t.core},
		dbName:          t.Req.GetDbName(),
		collectionNames: append(aliases, t.Req.GetOldName()),
		collectionID:    collID,
		ts:              ts,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_RenameCollection)},
	})

	// Step 3: If renamed to different database, expire cache for new name in target database
	if t.Req.GetNewDBName() != "" && t.Req.GetNewDBName() != t.Req.GetDbName() {
		redoTask.AddSyncStep(&expireCacheStep{
			baseStep:        baseStep{core: t.core},
			dbName:          t.Req.GetNewDBName(),
			collectionNames: []string{t.Req.GetNewName()},
			collectionID:    collID,
			ts:              ts,
			opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_RenameCollection)},
		})
	}

	return redoTask.Execute(ctx)
}

func (t *renameCollectionTask) validateEncryption(ctx context.Context) error {
	oldDBName := t.Req.GetDbName()
	newDBName := t.Req.GetNewDBName()

	// Get original database
	originalDB, err := t.core.meta.GetDatabaseByName(ctx, oldDBName, 0)
	if err != nil {
		if merr.IsErrDatabaseNotFound(err) && oldDBName == "" {
			// Use default database if not specified
			originalDB, err = t.core.meta.GetDatabaseByName(ctx, "default", 0)
			if err != nil {
				return fmt.Errorf("failed to get original database: %w", err)
			}
		} else {
			return fmt.Errorf("failed to get original database: %w", err)
		}
	}

	// Get target database
	targetDB, err := t.core.meta.GetDatabaseByName(ctx, newDBName, 0)
	if err != nil {
		return fmt.Errorf("target database %s not found: %w", newDBName, err)
	}

	// Check if either database has encryption enabled
	if hookutil.IsDBEncryptionEnabled(originalDB.Properties) || hookutil.IsDBEncryptionEnabled(targetDB.Properties) {
		return fmt.Errorf("deny to change collection databases due to at least one database enabled encryption")
	}

	return nil
}

func (t *renameCollectionTask) GetLockerKey() LockerKey {
	return NewLockerKeyChain(
		NewClusterLockerKey(true),
	)
}
