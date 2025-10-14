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
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type renameCollectionTask struct {
	baseTask
	Req *milvuspb.RenameCollectionRequest
}

func (t *renameCollectionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_RenameCollection); err != nil {
		return err
	}

	if t.Req.GetDbName() == "" {
		t.Req.DbName = util.DefaultDBName
	}

	if t.Req.GetNewDBName() == "" {
		t.Req.NewDBName = t.Req.GetDbName()
	}

	return nil
}

func (t *renameCollectionTask) Execute(ctx context.Context) error {
	// Check if renaming across databases with encryption enabled
	if t.Req.GetNewDBName() != t.Req.GetDbName() {
		if err := t.validateEncryption(ctx); err != nil {
			return err
		}
	}

	targetDB := t.Req.GetNewDBName()

	// check old collection isn't alias and exists in old db
	if t.core.meta.IsAlias(ctx, t.Req.GetDbName(), t.Req.GetOldName()) {
		return fmt.Errorf("unsupported use an alias to rename collection, alias:%s", t.Req.GetOldName())
	}

	collInfo, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetOldName(), typeutil.MaxTimestamp)
	if err != nil {
		return fmt.Errorf("collection not found in database, collection: %s, database: %s", t.Req.GetOldName(), t.Req.GetDbName())
	}

	// check old collection doesn't have aliases if renaming databases
	aliases := t.core.meta.ListAliasesByID(ctx, collInfo.CollectionID)
	if len(aliases) > 0 && targetDB != t.Req.GetDbName() {
		return fmt.Errorf("fail to rename collection to different database, must drop all aliases of collection %s before rename", t.Req.GetOldName())
	}

	// check new collection isn't alias and not exists in new db
	if t.core.meta.IsAlias(ctx, targetDB, t.Req.GetNewName()) {
		return fmt.Errorf("cannot rename collection to an existing alias: %s", t.Req.GetNewName())
	}

	_, err = t.core.meta.GetCollectionByName(ctx, targetDB, t.Req.GetNewName(), typeutil.MaxTimestamp)
	if err == nil {
		return fmt.Errorf("duplicated new collection name %s in database %s with other collection name or alias", t.Req.GetNewName(), targetDB)
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
		collectionID:    collInfo.CollectionID,
		ts:              ts,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_RenameCollection)},
	})

	return redoTask.Execute(ctx)
}

func (t *renameCollectionTask) validateEncryption(ctx context.Context) error {
	// old and new DB names are filled in Prepare, shouldn't be empty here
	oldDBName := t.Req.GetDbName()
	newDBName := t.Req.GetNewDBName()

	originalDB, err := t.core.meta.GetDatabaseByName(ctx, oldDBName, 0)
	if err != nil {
		return fmt.Errorf("failed to get original database: %w", err)
	}

	targetDB, err := t.core.meta.GetDatabaseByName(ctx, newDBName, 0)
	if err != nil {
		return fmt.Errorf("target database %s not found: %w", newDBName, err)
	}

	// Check if either database has encryption enabled
	if hookutil.IsDBEncryptionEnabled(originalDB.Properties) || hookutil.IsDBEncryptionEnabled(targetDB.Properties) {
		return fmt.Errorf("deny to change collection databases due to at least one database enabled encryption, original DB: %s, target DB: %s", oldDBName, newDBName)
	}

	return nil
}

func (t *renameCollectionTask) GetLockerKey() LockerKey {
	return NewLockerKeyChain(
		NewClusterLockerKey(true),
	)
}
