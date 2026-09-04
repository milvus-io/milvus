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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// This file is the single home of the DDL/import mutual exclusion for
// schema-version-advancing DDLs (issue #52154), enforced under the exclusive
// collection resource key BEFORE the AlterCollection message is broadcast
// (no WAL / catalog / meta trace on rejection):
//   - DDL side (here): fail when the collection has a non-terminal import job;
//   - import side (datacoord broadcastImport): fail when the job's schema
//     snapshot version lags the collection.

// nextSchemaVersion returns the version a schema-advancing DDL must stamp on
// the schema it broadcasts. Every bump site goes through here so that the
// checkNoInFlightImportJob call sites stay auditable (pinned by the guard
// test in ddl_import_mutex_test.go).
func nextSchemaVersion(coll *model.Collection) int32 {
	return coll.SchemaVersion + 1
}

// inFlightImportJobLister is the state-only import lookup the in-process
// coordinator provides (see mixCoordImpl.FirstInFlightImportJob); unlike
// ListImports it materializes no per-job progress, so it is safe to call while
// holding the collection resource key.
type inFlightImportJobLister interface {
	FirstInFlightImportJob(ctx context.Context, collectionID int64) (int64, internalpb.ImportJobState, bool, error)
}

// checkNoInFlightImportJob fails a schema-version-advancing DDL while the
// collection has any non-terminal import job. The entry-point check must run
// under the exclusive collection resource key: import jobs are created inside
// the import broadcast's ack callback, which completes before that broadcast
// releases the key, so an in-flight import is always visible here.
// classifyCollectionLockBusy also calls it lock-free, where it is only a
// best-effort diagnosis of an already-failed acquisition.
func (c *Core) checkNoInFlightImportJob(ctx context.Context, collectionName string, collectionID int64) error {
	if lister, ok := c.mixCoord.(inFlightImportJobLister); ok {
		jobID, state, found, err := lister.FirstInFlightImportJob(ctx, collectionID)
		if err != nil {
			return merr.Wrap(err, "failed to list import jobs for the ddl/import mutual exclusion check")
		}
		if found {
			return merr.WrapErrCollectionDDLImportConflict(collectionName,
				"import job %d in state %s is in flight, retry the ddl after it finishes",
				jobID, state.String())
		}
		return nil
	}
	resp, err := c.mixCoord.ListImports(ctx, &internalpb.ListImportsRequestInternal{
		CollectionID: collectionID,
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return merr.Wrap(err, "failed to list import jobs for the ddl/import mutual exclusion check")
	}
	for i, state := range resp.GetStates() {
		if state == internalpb.ImportJobState_Completed || state == internalpb.ImportJobState_Failed {
			continue
		}
		return merr.WrapErrCollectionDDLImportConflict(collectionName,
			"import job %s in state %s is in flight, retry the ddl after it finishes",
			resp.GetJobIDs()[i], state.String())
	}
	return nil
}

// classifyCollectionLockBusy distinguishes why the collection broadcast lock
// is contended: an in-flight import job yields the non-retriable conflict,
// anything else (another DDL broadcast, an import whose job is not yet
// visible) is a retriable transient.
func (c *Core) classifyCollectionLockBusy(ctx context.Context, coll *model.Collection) error {
	if err := c.checkNoInFlightImportJob(ctx, coll.Name, coll.CollectionID); err != nil &&
		errors.Is(err, merr.ErrCollectionDDLImportConflict) {
		return err
	}
	return merr.WrapErrCollectionDDLImportBusy(coll.Name,
		"another broadcast holds the collection lock, retry later")
}

// tryStartBroadcastWithCollectionLock is the fail-fast variant of
// startBroadcastWithCollectionLock for DDLs participating in the DDL/import
// mutual exclusion; see Broadcaster.TryWithResourceKeys for why waiting on
// the collection key is not an option.
func (c *Core) tryStartBroadcastWithCollectionLock(ctx context.Context, dbName string, coll *model.Collection) (broadcaster.BroadcastAPI, error) {
	api, err := broadcast.TryStartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(dbName),
		message.NewExclusiveCollectionNameResourceKey(dbName, coll.Name),
	)
	if err != nil {
		if broadcaster.IsFastLockFailed(err) {
			return nil, c.classifyCollectionLockBusy(ctx, coll)
		}
		return nil, merr.Wrap(err, "failed to start broadcast with collection lock")
	}
	return api, nil
}

// tryStartBroadcastWithAliasOrCollectionLock is the fail-fast counterpart of
// startBroadcastWithAliasOrCollectionLock. The post-lock re-check closes the
// resolve-then-lock race: an alias repoint (or rename) completing between
// resolution and lock would leave the DDL holding a key for a collection the
// request no longer designates.
func (c *Core) tryStartBroadcastWithAliasOrCollectionLock(ctx context.Context, dbName string, collectionNameOrAlias string) (broadcaster.BroadcastAPI, error) {
	coll, err := c.meta.GetCollectionByName(ctx, dbName, collectionNameOrAlias, typeutil.MaxTimestamp, true)
	if err != nil {
		return nil, merr.Wrap(err, "failed to get collection by name")
	}
	api, err := c.tryStartBroadcastWithCollectionLock(ctx, dbName, coll)
	if err != nil {
		return nil, err
	}
	if err := c.checkLockedCollectionName(ctx, dbName, collectionNameOrAlias, coll.Name); err != nil {
		api.Close()
		return nil, err
	}
	return api, nil
}

// checkLockedCollectionName re-resolves the request name under the lock and
// rejects when it no longer maps to the locked collection. The check is
// conclusive: alias and rename DDLs both need the exclusive DB key, which the
// held shared DB key blocks, so the name mapping cannot change afterwards.
func (c *Core) checkLockedCollectionName(ctx context.Context, dbName string, collectionNameOrAlias string, lockedName string) error {
	coll, err := c.meta.GetCollectionByName(ctx, dbName, collectionNameOrAlias, typeutil.MaxTimestamp, true)
	if err != nil {
		return merr.Wrap(err, "failed to re-resolve collection under the lock")
	}
	if coll.Name != lockedName {
		return merr.WrapErrCollectionDDLImportBusy(collectionNameOrAlias,
			"collection name resolution changed concurrently (locked %s, now %s), retry", lockedName, coll.Name)
	}
	return nil
}
