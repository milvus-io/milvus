package streaming

import (
	"context"
	"fmt"
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

func fetchTestSchema(ctx context.Context, c *schemaClient, channel string, write, bypass bool) (*schemautil.SchemaInfo, error) {
	if write {
		return c.GetCurrentWriteSchema(ctx, GetCurrentWriteSchemaRequest{VChannel: channel, BypassCache: bypass})
	}
	return c.GetSchemaVersion(ctx, GetSchemaVersionRequest{VChannel: channel, Version: 0, BypassCache: bypass})
}

func TestSchemaClientRefreshSameVersion(t *testing.T) {
	for _, write := range []bool{false, true} {
		t.Run(fmt.Sprint("write=", write), func(t *testing.T) {
			c := newSchemaClient(nil, time.Hour)
			defer c.close()
			var calls int
			p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(*schemaClient, context.Context, *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
				calls++
				body := testSchema(0)
				body.Name = fmt.Sprint("name-", calls)
				body.Properties = common.NewKeyValuePairs(map[string]string{common.TimezoneKey: "Asia/Shanghai"})
				return body, nil
			}).Build()
			defer p.UnPatch()
			old, err := fetchTestSchema(t.Context(), c, "p_1v0", write, false)
			require.NoError(t, err)
			oldUser := old
			fresh, err := fetchTestSchema(t.Context(), c, "p_1v0", write, true)
			require.NoError(t, err)
			require.Equal(t, 2, calls)
			require.NotSame(t, old, fresh)
			require.Equal(t, "name-1", old.Name)
			require.Equal(t, "name-2", fresh.Name)
			freshUser := fresh
			require.NotSame(t, oldUser, freshUser)
			require.Equal(t, "name-1", oldUser.Name)
			require.Equal(t, "name-2", freshUser.Name)
			require.Equal(t, "Asia/Shanghai", freshUser.SchemaHelper.GetTimezone())
			cached, err := fetchTestSchema(t.Context(), c, "p_1v1", false, false)
			require.NoError(t, err)
			require.Same(t, fresh, cached)
			require.Equal(t, 2, calls)
		})
	}
}

func TestSchemaClientRefreshFencesOlderFlights(t *testing.T) {
	for _, oldWrite := range []bool{false, true} {
		for _, newWrite := range []bool{false, true} {
			for _, oldBypass := range []bool{false, true} {
				t.Run(fmt.Sprintf("oldWrite=%v/newWrite=%v/oldBypass=%v", oldWrite, newWrite, oldBypass), func(t *testing.T) {
					c := newSchemaClient(nil, time.Hour)
					defer c.close()
					started, release := make(chan struct{}), make(chan struct{})
					var releaseOnce sync.Once
					defer releaseOnce.Do(func() { close(release) })
					var calls atomic.Int32
					p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, ctx context.Context, _ *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
						n := calls.Add(1)
						if n == 1 {
							close(started)
							select {
							case <-release:
							case <-ctx.Done():
								return nil, ctx.Err()
							}
						}
						body := testSchema(0)
						body.Description = fmt.Sprint(n)
						return body, nil
					}).Build()
					defer p.UnPatch()
					done := make(chan error, 1)
					go func() {
						_, err := fetchTestSchema(t.Context(), c, "p_1v0", oldWrite, oldBypass)
						done <- err
					}()
					<-started
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					fresh, err := fetchTestSchema(ctx, c, "p_1v0", newWrite, true)
					require.NoError(t, err, "refresh must not join an older flight")
					require.Equal(t, "2", fresh.Description)
					releaseOnce.Do(func() { close(release) })
					require.NoError(t, <-done)
					cached, err := fetchTestSchema(t.Context(), c, "p_1v1", false, false)
					require.NoError(t, err)
					require.Same(t, fresh, cached, "late read/write/refresh cannot overwrite the refreshed body")
					require.EqualValues(t, 2, calls.Load())
				})
			}
		}
	}
}

func TestSchemaClientRefreshCollectionIsolation(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, ctx context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
		if req.Vchannel == "p_1v0" {
			// A different collection refresh completes while this one is in flight.
			_, err := fetchTestSchema(ctx, c, "p_2v0", true, true)
			if err != nil {
				return nil, err
			}
		}
		return testSchema(0), nil
	}).Build()
	defer p.UnPatch()
	fresh, err := fetchTestSchema(t.Context(), c, "p_1v0", true, true)
	require.NoError(t, err)
	cached, ok := c.shard(1).bodies.load(schemaIdentity{1, 0})
	require.True(t, ok)
	require.Same(t, fresh, cached.info)
	_, ok = c.shard(2).bodies.load(schemaIdentity{2, 0})
	require.True(t, ok)
}

func TestSchemaClientRefreshFailureAndExpiredEntry(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	old := seedTestSchema(t, c, 1, testSchema(0))
	p := mockey.Mock((*schemaClient).getCollectionSchema).Return(nil, merr.ErrServiceUnavailable).Build()
	_, err := fetchTestSchema(t.Context(), c, "p_1v0", false, true)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	p.UnPatch()
	cached, ok := c.shard(1).bodies.load(schemaIdentity{1, 0})
	require.True(t, ok)
	require.Nil(t, cached.info, "a failed refresh must not serve the old body")
	p = mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, _ context.Context, _ *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
		c.shard(1).mu.Lock()
		c.shard(1).bodies.expire(time.Now().Add(2 * time.Hour))
		c.shard(1).mu.Unlock()
		c.invalidateWriteSchema("p_1v0")
		return testSchema(0), nil
	}).Build()
	defer p.UnPatch()
	fresh, err := fetchTestSchema(t.Context(), c, "p_1v0", true, true)
	require.NoError(t, err)
	require.NotSame(t, old, fresh)
	_, ok = c.shard(1).bodies.load(schemaIdentity{1, 0})
	require.False(t, ok, "expired entries cannot be resurrected by late responses")
}

func TestSchemaClientRefreshPreservesOtherVersions(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	var calls int
	description := "before alter"
	p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, _ context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
		calls++
		version := req.GetSchemaVersion()
		if req.GetCurrentWrite() {
			version = 2
		}
		schema := testSchema(version)
		schema.Description = description
		return schema, nil
	}).Build()
	defer p.UnPatch()
	old, err := fetchTestSchema(t.Context(), c, "p_1v0", false, false)
	require.NoError(t, err)
	description = "after alter"
	write, err := fetchTestSchema(t.Context(), c, "p_1v0", true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, write.Version)
	read, err := fetchTestSchema(t.Context(), c, "p_1v0", false, false)
	require.NoError(t, err)
	require.Equal(t, 2, calls, "refreshing the write version must not invalidate historical versions")
	require.Same(t, old, read)
	require.Zero(t, read.Version)
	require.Equal(t, "before alter", read.Description)
	require.Equal(t, "before alter", old.Description)
	again, err := fetchTestSchema(t.Context(), c, "p_1v0", true, false)
	require.NoError(t, err)
	require.Same(t, write, again, "write cache hits reuse the compiled SchemaInfo without RPCs")
	require.Same(t, write.FieldMap, again.FieldMap)
	require.Equal(t, 2, calls)
}

func TestSchemaClientReadRefreshInvalidatesWriteSelector(t *testing.T) {
	for _, readVersion := range []int32{0, 1, 2} {
		t.Run(fmt.Sprintf("readVersion=%d", readVersion), func(t *testing.T) {
			c := newSchemaClient(nil, time.Hour)
			defer c.close()
			var calls atomic.Int32
			var current atomic.Int32
			current.Store(1)
			p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, _ context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
				calls.Add(1)
				version := req.GetSchemaVersion()
				if req.GetCurrentWrite() {
					version = current.Load()
				}
				return testSchema(version), nil
			}).Build()
			defer p.UnPatch()
			old, err := fetchTestSchema(t.Context(), c, "p_1v0", true, false)
			require.NoError(t, err)
			require.EqualValues(t, 1, old.Version)
			current.Store(2)
			read, err := c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: readVersion, BypassCache: true})
			require.NoError(t, err)
			require.Equal(t, readVersion, read.Version, "the read refresh must retain its exact version")
			require.NotSame(t, old, read)
			fresh, err := fetchTestSchema(t.Context(), c, "p_1v0", true, false)
			require.NoError(t, err)
			require.EqualValues(t, 2, fresh.Version, "refreshing any read version must resolve the current write version from SN")
			hit, err := fetchTestSchema(t.Context(), c, "p_1v0", true, false)
			require.NoError(t, err)
			require.Same(t, fresh, hit)
			require.EqualValues(t, 3, calls.Load())
		})
	}
}

func TestSchemaClientVersionRefreshIsolation(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var calls atomic.Int32
	p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, ctx context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
		calls.Add(1)
		if req.GetCurrentWrite() {
			return testSchema(2), nil
		}
		if req.GetSchemaVersion() == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return testSchema(req.GetSchemaVersion()), nil
	}).Build()
	defer p.UnPatch()
	write, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
	require.NoError(t, err)
	done := make(chan *schemautil.SchemaInfo, 1)
	go func() {
		info, _ := c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: 1})
		done <- info
	}()
	<-started
	_, err = c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: 0, BypassCache: true})
	require.NoError(t, err)
	again, err := c.GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
	require.NoError(t, err)
	require.Same(t, write, again, "refreshing the write selector reuses the cached v2 body")
	once.Do(func() { close(release) })
	historical := <-done
	require.NotNil(t, historical)
	cached, err := c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: 1})
	require.NoError(t, err)
	require.Same(t, historical, cached, "refreshing v0 must not fence a pending v1 fill")
	require.EqualValues(t, 4, calls.Load(), "refreshing v0 must also refresh the v2 write selector")
}

func TestSchemaClientExpiredReadCannotOverwriteReplacement(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var calls atomic.Int32
	p := mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, ctx context.Context, _ *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
		call := calls.Add(1)
		if call == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		body := testSchema(0)
		body.Description = fmt.Sprint(call)
		return body, nil
	}).Build()
	defer p.UnPatch()
	done := make(chan error, 1)
	go func() {
		_, err := fetchTestSchema(t.Context(), c, "p_1v0", false, false)
		done <- err
	}()
	<-started
	c.shard(1).expire(time.Now().Add(2 * time.Hour))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fresh, err := fetchTestSchema(ctx, c, "p_1v0", false, false)
	require.NoError(t, err, "an expired entry must not join the old flight")
	require.Equal(t, "2", fresh.Description)
	once.Do(func() { close(release) })
	require.NoError(t, <-done)
	hit, err := fetchTestSchema(t.Context(), c, "p_1v0", false, false)
	require.NoError(t, err)
	require.Same(t, fresh, hit)
	require.EqualValues(t, 2, calls.Load())
}
