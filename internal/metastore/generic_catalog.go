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

package metastore

import (
	"context"
)

// GenericCatalog is the generic surface of a catalog
// (docs/design-docs/design_docs/20261110-unified-datacoord-catalog.md). A
// record is a value of a registered ModelType (see RegisterModelType),
// named by the type and the record's coordinate. Reads pass the type and a
// coordinate or prefix; writes pass the value itself, whose Go type
// identifies its ModelType.
type GenericCatalog interface {
	// Update applies typed actions as one write: atomic when the op count fits
	// the backend's txn limit, otherwise ordered chunks (see kv/txn.Commit).
	// An Update with an UnchangedSince action is a read-modify-write: it is
	// always atomic, and fails with an error matching predicates.ErrNotMet, writing
	// nothing, when a record it writes changed after the read revision.
	Update(ctx context.Context, actions ...UpdateAction) error

	// Get decodes the record of type t at coordinate, which has the type's
	// Depth components, and returns the revision it was read at (0 when the
	// backend reports none); pass it to UnchangedSince to write the record
	// back only if nothing changed it since. A missing record fails with an
	// error matching merr.ErrIoKeyNotFound.
	Get(ctx context.Context, t *ModelType, coordinate []string) (any, Revision, error)

	// List decodes the records of type t under coordinate, a prefix with
	// fewer components than the type's Depth (none for every record), in key
	// order, and returns those opts.Filter matches. A record that fails to
	// decode stops the list with an error naming its key.
	//
	// With opts.Limit 0 it returns every match, read from one snapshot when
	// the backend supports it; Revision is then that snapshot's revision,
	// and a Watch with AfterRevision = Revision misses no change and repeats
	// none. If the backend keeps compacting the snapshot, it reads without
	// one and returns Revision 0. With a positive Limit it returns one page of at most Limit
	// matches and the cursor of the next page; see ListOptions. Revision is
	// 0 when the backend reports none.
	List(ctx context.Context, t *ModelType, coordinate []string, opts ListOptions) (ListResult, error)

	// Watch calls fn with the changes to the records of type t under
	// coordinate, a prefix as in List, that commit after opts.AfterRevision, one EventBatch per
	// revision, in revision order. It blocks until the watch ends and
	// returns:
	//   - nil when ctx is done;
	//   - the error fn returns, which stops the watch;
	//   - an error matching kv.ErrCompacted (pkg/kv) when the backend no longer retains
	//     AfterRevision: List again and watch from the new revision;
	//   - any other watch failure, including a record that fails to decode
	//     (the error names its key).
	Watch(ctx context.Context, t *ModelType, coordinate []string, opts WatchOptions, fn func(EventBatch) error) error
}

// ListOptions shapes one List.
type ListOptions struct {
	// Filter is the predicate a decoded record must match to be returned, as
	// a value of the listed type's Go type. nil matches every record. It runs
	// in the catalog client today; a catalog server will need it in a
	// serializable form to run it remotely.
	Filter func(v any) bool

	// Limit caps the matches one List returns; 0 returns them all. A paged
	// list passes the previous result's Next as After and its Revision as
	// Revision, so every page reads the first page's snapshot. If the
	// backend compacts that snapshot before the last page, List fails with
	// an error matching kv.ErrCompacted (pkg/kv): start over from the first
	// page.
	Limit int
	// After continues a paged list after this cursor (a ListResult.Next),
	// passed back unchanged.
	After string
	// Revision reads at this revision; 0 reads the latest. Pass the first
	// page's Revision to the later pages.
	Revision Revision
	// KeysOnly returns the coordinates of the records in
	// ListResult.Coordinates, without decoding their values, so a record
	// whose value is corrupt is still listed (for example to delete it). A
	// key that is not a record of the type's depth still fails the List.
	// It cannot be combined with Filter.
	KeysOnly bool
}

// ListResult is the outcome of one List.
type ListResult struct {
	// Values are the matching records, values of the listed type's Go type;
	// nil for a KeysOnly List.
	Values []any
	// Coordinates are the coordinates of the records of a KeysOnly List.
	Coordinates [][]string
	Revision    Revision
	// Next is the After of the next page, "" when the list is complete. It
	// can be set when no match remains: the next page is then empty.
	Next string
}

// Revision is a point in the backend's commit order: every committed write
// gets a greater one, and all records of one atomic write share it. It is
// global to the store (one etcd cluster today; one catalog service later, or
// a TiKV commit timestamp), not per record or per type, and it is not
// contiguous: compare revisions, never do arithmetic on them. 0 means
// "unknown" or "latest", depending on where it is used.
//
// A record's last change is also a point on this order, so a revision read
// with a record guards a later write of it (see UnchangedSince).
type Revision int64

// WatchOptions shapes one Watch.
type WatchOptions struct {
	// AfterRevision delivers changes with a revision greater than it. 0
	// starts at the current revision. Start from a List revision for a
	// gap-free list-then-watch, and resume from the Revision of the last
	// EventBatch delivered.
	AfterRevision Revision
}

// EventBatch holds every event of one committed write (one atomic Update, or
// one chunk of a chunked Update). All events share Revision.
type EventBatch struct {
	Revision Revision
	Events   []Event
}

// EventType is the kind of a watch event.
type EventType int

const (
	// EventPut reports a created or updated record.
	EventPut EventType = iota + 1
	// EventDelete reports a deleted record. A prefix delete reports one event
	// per removed record.
	EventDelete
)

// Event is one record change.
type Event struct {
	Type EventType
	// Coordinate is the changed record's coordinate, set for both event
	// types.
	Coordinate []string
	// Value is the record's new value, of the watched type's Go type. It is
	// nil for EventDelete.
	Value any
}
