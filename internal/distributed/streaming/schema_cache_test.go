package streaming

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func testSchema(version int32) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Name: "c", Version: version, Fields: []*schemapb.FieldSchema{
		{Name: common.RowIDFieldName, FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
		{Name: "id", FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
}

func TestSchemaClientSharedBodyAndCachedWriteVersion(t *testing.T) {
	var calls atomic.Int32
	var current atomic.Int32
	p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, _ context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
		calls.Add(1)
		version := req.GetSchemaVersion()
		if req.GetCurrentWrite() {
			version = current.Load()
		}
		return testSchema(version), nil
	}).Build()
	defer p.UnPatch()
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	write, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
	require.NoError(t, err)
	read, err := c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v1", Version: 0})
	require.NoError(t, err)
	require.Same(t, write, read)
	require.EqualValues(t, 1, calls.Load())
	require.Len(t, write.Fields, 2)
	system, err := write.SchemaHelper.GetFieldFromID(0)
	require.NoError(t, err)
	require.Same(t, write.Fields[0], system)
	current.Store(1)
	for range 100 {
		hit, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v1"})
		require.NoError(t, err)
		require.Same(t, write, hit)
	}
	require.EqualValues(t, 1, calls.Load(), "hot writes must not issue schema RPCs")
	w := &walAccesserImpl{schemaClient: c}
	w.onSchemaVersionMismatch("p_1v1")
	_, ok := c.shard(1).writeVersions[1]
	require.False(t, ok)
	next, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
	require.NoError(t, err)
	require.EqualValues(t, 1, next.Version)
	retained, err := c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: 1})
	require.NoError(t, err)
	require.Equal(t, next.Version, retained.Version)
	require.Zero(t, read.Version)
	require.EqualValues(t, 2, calls.Load())
}

func TestSchemaClientWriteCacheExpiry(t *testing.T) {
	for _, expired := range []string{"body", "disabled"} {
		t.Run(expired, func(t *testing.T) {
			ttl := time.Hour
			if expired == "disabled" {
				ttl = 0
			}
			c := newSchemaClient(nil, ttl)
			defer c.close()
			var calls atomic.Int32
			p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(*schemaClient, context.Context, *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
				return testSchema(calls.Add(1)), nil
			}).Build()
			defer p.UnPatch()
			_, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
			require.NoError(t, err)
			c.shard(1).mu.Lock()
			switch expired {
			case "body":
				c.shard(1).bodies.expire(time.Now().Add(2 * time.Hour))
			}
			c.shard(1).mu.Unlock()
			fresh, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
			require.NoError(t, err)
			require.EqualValues(t, 2, fresh.Version)
			require.EqualValues(t, 2, calls.Load())
		})
	}
}

func TestSchemaCacheIdleExpiryRemovesMatchingSelector(t *testing.T) {
	c := newSchemaClient(nil, 20*time.Millisecond)
	defer c.close()
	body := testSchema(0)
	seedTestSchema(t, c, 1, body)
	c.shard(1).mu.Lock()
	c.shard(1).writeVersions[1] = &cachedWriteVersion{version: 0}
	c.shard(1).bodies.expire(time.Now().Add(time.Hour))
	_, ok := c.shard(1).bodies.load(schemaIdentity{1, 0})
	require.False(t, ok)
	version, ok := c.shard(1).writeVersions[1]
	c.shard(1).mu.Unlock()
	require.False(t, ok)
	require.Nil(t, version)
	seedTestSchema(t, c, 1, body)
	require.Eventually(t, func() bool {
		c.shard(1).mu.Lock()
		defer c.shard(1).mu.Unlock()
		return len(c.shard(1).bodies.entries) == 0
	}, time.Second, time.Millisecond)
	require.Equal(t, "c", body.Name)
	disabled := newSchemaCache[int, int](0)
	disabled.store(1, 1)
	_, ok = disabled.load(1)
	require.False(t, ok)
	renewed := newSchemaCache[int, int](time.Hour)
	renewed.store(1, 1)
	e := renewed.entries[1]
	e.accessed = time.Now().Add(-50 * time.Minute)
	renewed.entries[1] = e
	_, ok = renewed.load(1)
	require.True(t, ok)
	renewed.expire(time.Now().Add(20 * time.Minute))
	_, ok = renewed.load(1)
	require.True(t, ok)
}

func TestSchemaClientShardIsolation(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	info := seedTestSchema(t, c, 2, testSchema(0))
	blocked := c.shard(1)
	require.NotSame(t, blocked, c.shard(2))
	blocked.mu.Lock()
	var once sync.Once
	defer once.Do(blocked.mu.Unlock)
	done := make(chan error, 1)
	go func() {
		// Expiry acquires only the target shard's lock.
		c.shard(2).expire(time.Now())
		got, err := c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_2v0", Version: 0})
		if got != info && err == nil {
			err = merr.ErrServiceInternal
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		once.Do(blocked.mu.Unlock)
		<-done
		t.Fatal("one cache shard blocked another collection's lookup or expiry")
	}
}

func TestSchemaClientInvalidationFencesInFlightWrite(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, ctx context.Context, _ *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
		v := calls.Add(1)
		if v == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return testSchema(v), nil
	}).Build()
	defer p.UnPatch()
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	done := make(chan error, 1)
	go func() {
		_, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
		done <- err
	}()
	<-started
	c.invalidateWriteSchema("p_1v0")
	fresh, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
	require.NoError(t, err)
	require.EqualValues(t, 2, fresh.Version)
	close(release)
	require.NoError(t, <-done)
	version, ok := c.shard(1).writeVersions[1]
	require.True(t, ok)
	require.EqualValues(t, 2, version.version)
}

func TestSchemaClientFailuresAndShutdown(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	_, err := c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "invalid", Version: 0})
	require.Error(t, err)
	p := mockey.Mock((*schemaClient).getCollectionSchema).Return(testSchema(1), nil).Build()
	_, err = c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: 0})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	p.UnPatch()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.GetCurrentWriteSchema(ctx, GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
	require.ErrorIs(t, err, context.Canceled)
	c.close()
	_, err = c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: 0})
	require.ErrorIs(t, err, ErrWALAccesserClosed)
}

func TestSchemaClientFailureDoesNotPoisonCache(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	p := mockey.Mock((*schemaClient).getCollectionSchema).Return(nil, merr.ErrServiceUnavailable).Build()
	_, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	_, err = c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: 0})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	pending, ok := c.shard(1).bodies.load(schemaIdentity{1, 0})
	require.True(t, ok)
	require.Nil(t, pending.info)
	write, ok := c.shard(1).writeVersions[1]
	require.True(t, ok)
	require.Equal(t, unknownWriteSchemaVersion, write.version)
	p.UnPatch()
	p = mockey.Mock((*schemaClient).getCollectionSchema).Return(testSchema(0), nil).Build()
	defer p.UnPatch()
	_, err = c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: 0})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.GetSchemaVersion(ctx, GetSchemaVersionRequest{VChannel: "p_1v0", Version: 0})
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "invalid"})
	require.Error(t, err)
	c.invalidateWriteSchema("invalid")
	bad := testSchema(0)
	bad.Fields = append(bad.Fields, bad.Fields[1])
	p.UnPatch()
	p = mockey.Mock((*schemaClient).getCollectionSchema).Return(bad, nil).Build()
	defer p.UnPatch()
	_, err = c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0", BypassCache: true})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	write, ok = c.shard(1).writeVersions[1]
	require.True(t, ok)
	require.Nil(t, c.shard(1).cachedWriteSchemaLocked(1, write), "invalid schemas must not publish a usable write selector")
}

func TestSchemaClientCloseFencesPendingFill(t *testing.T) {
	for _, write := range []bool{false, true} {
		c := newSchemaClient(nil, time.Hour)
		p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(*schemaClient, context.Context, *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
			c.close()
			return testSchema(0), nil
		}).Build()
		var err error
		if write {
			_, err = c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
		} else {
			_, err = c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: 0})
		}
		require.ErrorIs(t, err, ErrWALAccesserClosed)
		require.Empty(t, c.shard(1).bodies.entries)
		require.Empty(t, c.shard(1).writeVersions)
		_, err = c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
		require.ErrorIs(t, err, ErrWALAccesserClosed)
		p.UnPatch()
	}
}

func seedTestSchema(t *testing.T, c *schemaClient, id int64, raw *schemapb.CollectionSchema) *schemautil.SchemaInfo {
	t.Helper()
	c.shard(id).mu.Lock()
	entry := c.shard(id).schemaEntryLocked(schemaIdentity{id, raw.Version}, false)
	c.shard(id).mu.Unlock()
	info, err := c.shard(id).cacheSchema(schemaIdentity{id, raw.Version}, raw, entry)
	require.NoError(t, err)
	return info
}

func TestSchemaCacheExpiresIdlePrefixInBatches(t *testing.T) {
	c := newSchemaCache[int, int](time.Hour)
	now := time.Now()
	for i := range schemaExpireBatchSize*2 + 1 {
		c.store(i, i)
		c.entries[i].accessed = now.Add(-2 * time.Hour)
	}
	// Renew the oldest entry by store, moving it behind the expired prefix.
	c.store(0, 100)
	require.True(t, c.expire(now))
	require.Len(t, c.entries, schemaExpireBatchSize+1)
	require.False(t, c.expire(now))
	require.Len(t, c.entries, 1)
	value, ok := c.load(0)
	require.True(t, ok)
	require.Equal(t, 100, value)
	require.Equal(t, 1, c.idle.Len())
	c.erase(0)
	require.Zero(t, c.idle.Len())
	require.False(t, c.expire(now))
	c.store(1, 1)
	c.clear()
	require.Empty(t, c.entries)
	require.Zero(t, c.idle.Len())
}

func TestSchemaCacheVersionExpiryRemovesOnlyMatchingWriteIndex(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	old := seedTestSchema(t, c, 1, testSchema(0))
	current := seedTestSchema(t, c, 1, testSchema(1))
	shard := c.shard(1)
	shard.mu.Lock()
	shard.writeVersions[1] = &cachedWriteVersion{version: 1}
	shard.bodies.entries[schemaIdentity{1, 0}].accessed = time.Now().Add(-2 * time.Hour)
	shard.mu.Unlock()
	shard.expire(time.Now())
	hit, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
	require.NoError(t, err)
	require.Same(t, current, hit)
	shard.expire(time.Now().Add(2 * time.Hour))
	shard.mu.Lock()
	defer shard.mu.Unlock()
	require.Empty(t, shard.bodies.entries)
	require.NotContains(t, shard.writeVersions, int64(1), "evicting the selected version also removes its write index")
	require.Zero(t, old.Version, "eviction leaves previously returned immutable schemas usable")
}

func TestSchemaCacheLazyExpiryRemovesMatchingWriteIndex(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	seedTestSchema(t, c, 1, testSchema(0))
	shard := c.shard(1)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.writeVersions[1] = &cachedWriteVersion{version: 0}
	shard.bodies.entries[schemaIdentity{1, 0}].accessed = time.Now().Add(-2 * time.Hour)
	_, ok := shard.bodies.load(schemaIdentity{1, 0})
	require.False(t, ok)
	require.NotContains(t, shard.writeVersions, int64(1))
}
