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

package datacoord

import (
	"context"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// These tests run the record primitives against etcd, like the etcd KV suite
// (internal/kv/etcd): they need the etcd from the test configuration.

// newEtcdRecordCatalog returns a catalog on a fresh etcd root and a function
// that reads the store's current revision.
func newEtcdRecordCatalog(t *testing.T) (*Catalog, func() metastore.Revision) {
	params := paramtable.Get()
	cli, err := etcd.GetEtcdClient(
		params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		params.EtcdCfg.Endpoints.GetAsStrings(),
		params.EtcdCfg.EtcdTLSCert.GetValue(),
		params.EtcdCfg.EtcdTLSKey.GetValue(),
		params.EtcdCfg.EtcdTLSCACert.GetValue(),
		params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	root := path.Join("unittest/catalog-record", funcutil.RandomString(8))
	metakv := etcdkv.NewEtcdKV(cli, root)
	t.Cleanup(func() {
		_ = metakv.RemoveWithPrefix(context.Background(), "")
		cli.Close()
	})
	currentRevision := func() metastore.Revision {
		resp, err := cli.Get(context.Background(), "revision-probe")
		require.NoError(t, err)
		return metastore.Revision(resp.Header.Revision)
	}
	return NewCatalog(metakv, "", root), currentRevision
}

func etcdTestGroup(collectionID, groupID int64) *model.SegmentChangeGroup {
	return &model.SegmentChangeGroup{
		GroupID:       groupID,
		Source:        model.SegmentChangeSourceMixCompaction,
		CollectionID:  collectionID,
		State:         model.SegmentChangeStateStaged,
		NewSegmentIDs: []int64{1000 + groupID},
	}
}

// startWatch runs catalog.Watch in the background, forwarding its batches.
// Canceling the returned context ends the watch; Watch's result is then
// sent on the error channel.
func startWatch(t *testing.T, catalog *Catalog, opts metastore.WatchOptions) (
	context.CancelFunc, <-chan metastore.EventBatch, <-chan error,
) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	batches := make(chan metastore.EventBatch, 16)
	done := make(chan error, 1)
	go func() {
		done <- catalog.Watch(ctx, metastore.SegmentGroupType, nil, opts, func(b metastore.EventBatch) error {
			batches <- b
			return nil
		})
	}()
	return cancel, batches, done
}

func nextBatch(t *testing.T, batches <-chan metastore.EventBatch, done <-chan error) metastore.EventBatch {
	t.Helper()
	select {
	case batch := <-batches:
		return batch
	case err := <-done:
		require.FailNow(t, "watch ended early", "%v", err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timed out waiting for a watch batch")
	}
	return metastore.EventBatch{}
}

// batchIDs returns the batch's group coordinates, "/"-joined and sorted.
func batchIDs(b metastore.EventBatch) []string {
	ids := make([]string, 0, len(b.Events))
	for _, ev := range b.Events {
		ids = append(ids, path.Join(ev.Coordinate...))
	}
	sort.Strings(ids)
	return ids
}

// TestCatalog_GenericCatalogOnEtcd covers List and its revision, a gap-free
// typed watch from it, one batch per atomic Update, Get, a point delete and
// a collection's deletes in one Update.
func TestCatalog_GenericCatalogOnEtcd(t *testing.T) {
	ctx := context.Background()
	catalog, currentRevision := newEtcdRecordCatalog(t)
	for id := int64(1); id <= 3; id++ {
		require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(etcdTestGroup(10, id))))
	}
	groups, err := listTestGroups(ctx, catalog)
	require.NoError(t, err)
	require.Len(t, groups, 3)
	listed, err := catalog.List(ctx, metastore.SegmentGroupType, []string{"10"}, metastore.ListOptions{
		Filter: func(v any) bool { return v.(*model.SegmentChangeGroup).GroupID != 2 },
	})
	require.NoError(t, err)
	require.Len(t, listed.Values, 2)
	require.Equal(t, currentRevision(), listed.Revision, "the list read the latest snapshot")
	rev := listed.Revision

	// List-then-watch: a watch after the list revision delivers a write made
	// after the list exactly once, then later writes.
	require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(etcdTestGroup(10, 4))))
	stop, batches, done := startWatch(t, catalog, metastore.WatchOptions{AfterRevision: rev})
	next := func() metastore.EventBatch { return nextBatch(t, batches, done) }

	b := next()
	require.Equal(t, []string{"10/4"}, batchIDs(b))
	require.Equal(t, metastore.EventPut, b.Events[0].Type)
	require.Equal(t, int64(4), b.Events[0].Value.(*model.SegmentChangeGroup).GroupID)

	// One atomic Update is one batch.
	require.NoError(t, catalog.Update(ctx,
		metastore.SaveSegmentChangeGroup(etcdTestGroup(10, 5)),
		metastore.SaveSegmentChangeGroup(etcdTestGroup(10, 6))))
	b = next()
	require.Equal(t, []string{"10/5", "10/6"}, batchIDs(b))

	// Point read of a typed record.
	got, rev, err := catalog.Get(ctx, metastore.SegmentGroupType, []string{"10", "1"})
	require.NoError(t, err)
	require.Positive(t, rev)
	require.Equal(t, []int64{1001}, got.(*model.SegmentChangeGroup).NewSegmentIDs)

	require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(etcdTestGroup(20, 1))))
	b = next()
	require.Equal(t, []string{"20/1"}, batchIDs(b))

	// A point delete is one delete event naming its record.
	require.NoError(t, catalog.Update(ctx, metastore.DeleteSegmentChangeGroup(20, 1)))
	b = next()
	require.Equal(t, []string{"20/1"}, batchIDs(b))
	require.Equal(t, metastore.EventDelete, b.Events[0].Type)
	require.Nil(t, b.Events[0].Value)

	// The deletes of one Update (collection drop) are one etcd txn: one batch
	// with one delete event per removed record.
	var drops []metastore.UpdateAction
	for groupID := int64(1); groupID <= 6; groupID++ {
		drops = append(drops, metastore.DeleteSegmentChangeGroup(10, groupID))
	}
	require.NoError(t, catalog.Update(ctx, drops...))
	b = next()
	require.Equal(t, []string{"10/1", "10/2", "10/3", "10/4", "10/5", "10/6"}, batchIDs(b))
	for _, ev := range b.Events {
		require.Equal(t, metastore.EventDelete, ev.Type)
		require.Nil(t, ev.Value)
	}
	groups, err = listTestGroups(ctx, catalog)
	require.NoError(t, err)
	require.Empty(t, groups)

	stop()
	select {
	case err := <-done:
		require.NoError(t, err, "canceling ctx ends the watch without an error")
	case <-time.After(10 * time.Second):
		require.FailNow(t, "watch did not end after its context was canceled")
	}
}

// TestCatalog_UnchangedSinceOnEtcd runs a read-modify-write on etcd: a write
// guarded by the Get revision applies once, a second write with the same
// revision conflicts, and a record deleted after the read is not written
// back.
func TestCatalog_UnchangedSinceOnEtcd(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newEtcdRecordCatalog(t)
	require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(etcdTestGroup(10, 1))))

	v, rev, err := catalog.Get(ctx, metastore.SegmentGroupType, []string{"10", "1"})
	require.NoError(t, err)
	require.Positive(t, rev)
	g := v.(*model.SegmentChangeGroup)
	g.State = model.SegmentChangeStateCommitted
	require.NoError(t, catalog.Update(ctx, metastore.UnchangedSince(rev), metastore.SaveSegmentChangeGroup(g)))

	// the record changed after rev (the write above): conflict, nothing written
	g.State = model.SegmentChangeStateStaged
	err = catalog.Update(ctx, metastore.UnchangedSince(rev), metastore.SaveSegmentChangeGroup(g))
	require.True(t, errors.Is(err, predicates.ErrNotMet), "%v", err)
	v, _, err = catalog.Get(ctx, metastore.SegmentGroupType, []string{"10", "1"})
	require.NoError(t, err)
	require.Equal(t, model.SegmentChangeStateCommitted, v.(*model.SegmentChangeGroup).State)

	// deleted after the read: not written back
	_, rev, err = catalog.Get(ctx, metastore.SegmentGroupType, []string{"10", "1"})
	require.NoError(t, err)
	require.NoError(t, catalog.Update(ctx, metastore.DeleteSegmentChangeGroup(10, 1)))
	err = catalog.Update(ctx, metastore.UnchangedSince(rev), metastore.SaveSegmentChangeGroup(g))
	require.True(t, errors.Is(err, predicates.ErrNotMet), "%v", err)
	_, _, err = catalog.Get(ctx, metastore.SegmentGroupType, []string{"10", "1"})
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
}
