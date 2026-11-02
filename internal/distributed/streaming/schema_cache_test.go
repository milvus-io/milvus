package streaming

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func testSchema(version int32) *SchemaSnapshot {
	return &SchemaSnapshot{Schema: &schemapb.CollectionSchema{Name: "c", Version: version, Fields: []*schemapb.FieldSchema{
		{Name: common.RowIDFieldName, FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
		{Name: "id", FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}}
}

func TestSchemaClientSharedBodyAndFreshWriteVersion(t *testing.T) {
	var calls atomic.Int32
	var current atomic.Int32
	p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, _ context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*SchemaSnapshot, error) {
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
	write, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{"p_1v0"})
	require.NoError(t, err)
	read, err := c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{"p_1v1", 0})
	require.NoError(t, err)
	require.Same(t, write, read)
	require.EqualValues(t, 1, calls.Load())
	user, err := write.UserSchema()
	require.NoError(t, err)
	require.Len(t, user.Fields, 1)
	require.Len(t, write.Schema.Fields, 2)
	require.Same(t, write.Schema.Fields[1], user.Fields[0])
	again, err := read.UserSchema()
	require.NoError(t, err)
	require.Same(t, user, again)
	current.Store(1)
	next, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{"p_1v0"})
	require.NoError(t, err)
	require.EqualValues(t, 1, next.Schema.Version)
	c.invalidateWriteSchema("p_1v1")
	_, ok := c.writeVersions.load(1)
	require.False(t, ok)
	retained, err := c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{"p_1v0", 1})
	require.NoError(t, err)
	require.Same(t, next, retained)
	require.Zero(t, read.Schema.Version)
}

func TestSchemaCacheIdleExpiryAndIndependentSelectors(t *testing.T) {
	c := newSchemaClient(nil, 20*time.Millisecond)
	defer c.close()
	body := testSchema(0)
	c.bodies.store(schemaIdentity{1, 0}, body)
	c.writeVersions.store(1, 0)
	c.bodies.expire(time.Now().Add(time.Hour))
	_, ok := c.bodies.load(schemaIdentity{1, 0})
	require.False(t, ok)
	version, ok := c.writeVersions.load(1)
	require.True(t, ok)
	require.Zero(t, version)
	c.bodies.store(schemaIdentity{1, 0}, body)
	require.Eventually(t, func() bool { c.bodies.mu.Lock(); defer c.bodies.mu.Unlock(); return len(c.bodies.entries) == 0 }, time.Second, time.Millisecond)
	require.Equal(t, "c", body.Schema.Name)
	disabled := newSchemaCache[int, int](0)
	disabled.store(1, 1)
	require.Equal(t, 2, disabled.loadOrStore(1, 2))
	_, ok = disabled.load(1)
	require.False(t, ok)
	renewed := newSchemaCache[int, int](time.Hour)
	renewed.store(1, 1)
	renewed.mu.Lock()
	e := renewed.entries[1]
	e.accessed = time.Now().Add(-50 * time.Minute)
	renewed.entries[1] = e
	renewed.mu.Unlock()
	_, ok = renewed.load(1)
	require.True(t, ok)
	renewed.expire(time.Now().Add(20 * time.Minute))
	_, ok = renewed.load(1)
	require.True(t, ok)
}

func TestSchemaClientInvalidationFencesInFlightWrite(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, ctx context.Context, _ *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*SchemaSnapshot, error) {
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
		_, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{"p_1v0"})
		done <- err
	}()
	<-started
	c.invalidateWriteSchema("p_1v0")
	fresh, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{"p_1v0"})
	require.NoError(t, err)
	require.EqualValues(t, 2, fresh.Schema.Version)
	close(release)
	require.NoError(t, <-done)
	version, ok := c.writeVersions.load(1)
	require.True(t, ok)
	require.EqualValues(t, 2, version)
}

func TestSchemaClientFailuresAndShutdown(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	_, err := c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{"invalid", 0})
	require.Error(t, err)
	p := mockey.Mock((*schemaClient).getCollectionSchema).Return(testSchema(1), nil).Build()
	_, err = c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{"p_1v0", 0})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	p.UnPatch()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.GetCurrentWriteSchema(ctx, GetCurrentWriteSchemaRequest{"p_1v0"})
	require.ErrorIs(t, err, context.Canceled)
	c.close()
	_, err = c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{"p_1v0", 0})
	require.ErrorIs(t, err, ErrWALAccesserClosed)
}

func TestSchemaClientFailureDoesNotPoisonCache(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	p := mockey.Mock((*schemaClient).getCollectionSchema).Return(nil, merr.ErrServiceUnavailable).Build()
	_, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{"p_1v0"})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	_, err = c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{"p_1v0", 0})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Empty(t, c.bodies.entries)
	require.Empty(t, c.writeVersions.entries)
	p.UnPatch()
	p = mockey.Mock((*schemaClient).getCollectionSchema).Return(testSchema(0), nil).Build()
	defer p.UnPatch()
	_, err = c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{"p_1v0", 0})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.GetSchemaVersion(ctx, GetSchemaVersionRequest{"p_1v0", 0})
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{"invalid"})
	require.Error(t, err)
	c.invalidateWriteSchema("invalid")
	bad := testSchema(0)
	bad.Schema.Fields = append(bad.Schema.Fields, bad.Schema.Fields[1])
	_, err = bad.UserSchema()
	require.Error(t, err)
}

func TestSchemaClientCloseFencesPendingFill(t *testing.T) {
	for _, write := range []bool{false, true} {
		c := newSchemaClient(nil, time.Hour)
		p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(*schemaClient, context.Context, *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*SchemaSnapshot, error) {
			c.close()
			return testSchema(0), nil
		}).Build()
		var err error
		if write {
			_, err = c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{"p_1v0"})
		} else {
			_, err = c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{"p_1v0", 0})
		}
		require.ErrorIs(t, err, ErrWALAccesserClosed)
		require.Empty(t, c.bodies.entries)
		require.Empty(t, c.writeVersions.entries)
		_, err = c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{"p_1v0"})
		require.ErrorIs(t, err, ErrWALAccesserClosed)
		p.UnPatch()
	}
}
