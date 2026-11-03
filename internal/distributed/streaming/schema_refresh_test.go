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
	cached, ok := c.bodies.load(schemaIdentity{1, 0})
	require.True(t, ok)
	require.Same(t, fresh, cached.info)
	_, ok = c.bodies.load(schemaIdentity{2, 0})
	require.True(t, ok)
}

func TestSchemaClientRefreshFailureAndExpiredEpoch(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
	old := seedTestSchema(t, c, 1, testSchema(0))
	p := mockey.Mock((*schemaClient).getCollectionSchema).Return(nil, merr.ErrServiceUnavailable).Build()
	_, err := fetchTestSchema(t.Context(), c, "p_1v0", false, true)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	p.UnPatch()
	cached, ok := c.bodies.load(schemaIdentity{1, 0})
	require.True(t, ok)
	require.Same(t, old, cached.info)
	p = mockey.Mock((*schemaClient).getCollectionSchema).To(func(_ *schemaClient, _ context.Context, _ *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
		c.mu.Lock()
		c.epochs.expire(time.Now().Add(2 * time.Hour))
		c.mu.Unlock()
		return testSchema(0), nil
	}).Build()
	defer p.UnPatch()
	fresh, err := fetchTestSchema(t.Context(), c, "p_1v0", true, true)
	require.NoError(t, err)
	require.NotSame(t, old, fresh)
	cached, ok = c.bodies.load(schemaIdentity{1, 0})
	require.True(t, ok)
	require.Same(t, old, cached.info, "expired epochs cannot publish late responses")
}

func TestSchemaClientRefreshInvalidatesOlderVersions(t *testing.T) {
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
	require.Equal(t, 3, calls, "older versions must be refetched to obtain current non-versioned metadata")
	require.Zero(t, read.Version)
	require.Equal(t, "after alter", read.Description)
	require.Equal(t, "before alter", old.Description)
	again, err := fetchTestSchema(t.Context(), c, "p_1v0", true, false)
	require.NoError(t, err)
	require.Same(t, write, again, "write cache hits reuse the compiled SchemaInfo without RPCs")
	require.Same(t, write.FieldMap, again.FieldMap)
	require.Equal(t, 3, calls)
}

func TestSchemaClientReadRefreshInvalidatesWriteSelector(t *testing.T) {
	c := newSchemaClient(nil, time.Hour)
	defer c.close()
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
	old, err := fetchTestSchema(t.Context(), c, "p_1v0", true, false)
	require.NoError(t, err)
	current.Store(1)
	read, err := fetchTestSchema(t.Context(), c, "p_1v0", false, true)
	require.NoError(t, err)
	require.Zero(t, read.Version, "Coord's published version can lag SN's write version")
	require.NotSame(t, old, read)
	fresh, err := fetchTestSchema(t.Context(), c, "p_1v0", true, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, fresh.Version, "a refreshed read body must not resurrect an old write selector")
	hit, err := fetchTestSchema(t.Context(), c, "p_1v0", true, false)
	require.NoError(t, err)
	require.Same(t, fresh, hit)
	require.EqualValues(t, 3, calls.Load())
}
