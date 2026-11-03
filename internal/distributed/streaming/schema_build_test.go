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
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestSchemaClientBuildOutsideLock(t *testing.T) {
	for _, action := range []string{"read", "refresh", "close"} {
		t.Run(action, func(t *testing.T) {
			c := newSchemaClient(nil, time.Hour)
			defer c.close()
			other := seedTestSchema(t, c, 2, testSchema(0))
			var original func(*schemapb.CollectionSchema) (*schemautil.SchemaInfo, error)
			var builds atomic.Int32
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			patch := mockey.Mock(schemautil.NewSchemaInfo).Origin(&original).To(func(schema *schemapb.CollectionSchema) (*schemautil.SchemaInfo, error) {
				if builds.Add(1) == 1 {
					close(started)
					<-release
				}
				return original(schema)
			}).Build()
			defer patch.UnPatch()
			rpc := mockey.Mock((*schemaClient).getCollectionSchema).To(func(*schemaClient, context.Context, *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
				return testSchema(0), nil
			}).Build()
			defer rpc.UnPatch()
			defer once.Do(func() { close(release) })
			type result struct {
				info *schemautil.SchemaInfo
				err  error
			}
			done := make(chan result, 1)
			go func() {
				info, err := fetchTestSchema(t.Context(), c, "p_1v0", true, false)
				done <- result{info, err}
			}()
			<-started
			// A blocked field-index build must not hold the client lock.
			unlocked := c.shard(1).mu.TryLock()
			if unlocked {
				c.shard(1).mu.Unlock()
			}
			require.True(t, unlocked)
			hit, err := c.GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_2v0", Version: 0})
			require.NoError(t, err)
			require.Same(t, other, hit)

			var concurrent *schemautil.SchemaInfo
			if action == "close" {
				c.close()
			} else {
				concurrent, err = fetchTestSchema(t.Context(), c, "p_1v0", false, action == "refresh")
				require.NoError(t, err)
			}
			once.Do(func() { close(release) })
			got := <-done
			if action == "close" {
				require.ErrorIs(t, got.err, ErrWALAccesserClosed)
				require.Empty(t, c.shard(1).bodies.entries)
				require.Empty(t, c.shard(1).writeVersions)
				return
			}
			require.NoError(t, got.err)
			if action == "read" {
				require.Same(t, concurrent, got.info, "concurrent fills share the published object")
			} else {
				require.NotSame(t, concurrent, got.info, "an older build cannot publish into a refreshed version entry")
				_, ok := c.shard(1).writeVersions[1]
				require.False(t, ok)
			}
			hit, err = fetchTestSchema(t.Context(), c, "p_1v0", false, false)
			require.NoError(t, err)
			require.Same(t, concurrent, hit)
		})
	}
}
