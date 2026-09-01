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

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// This file is the single home of the DDL/import mutual exclusion for
// schema-version-advancing DDLs (issue #52154), enforced under the exclusive
// collection resource key BEFORE the AlterCollection message is broadcast
// (no WAL / catalog / meta trace on rejection):
//   - DDL side (here): fail when the collection has a non-terminal import job;
//   - import side (datacoord broadcastImport): validate the request schema
//     against current metadata under the same key before creating the job.

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
		return merr.WrapErrCollectionDDLImportConflict(collectionNameOrAlias,
			"collection name resolution changed concurrently (locked %s, now %s), retry", lockedName, coll.Name)
	}
	return nil
}
