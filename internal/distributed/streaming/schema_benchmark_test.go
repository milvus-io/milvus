package streaming

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util/schemautil"
)

func BenchmarkSchemaClientCacheHit(b *testing.B) {
	for _, collections := range []int{1, 4096} {
		for _, write := range []bool{false, true} {
			b.Run(fmt.Sprintf("collections=%d/write=%v", collections, write), func(b *testing.B) {
				c := newSchemaClient(nil, time.Hour)
				defer c.close()
				info, err := schemautil.NewSchemaInfo(testSchema(0))
				if err != nil {
					b.Fatal(err)
				}
				channels := make([]string, collections)
				for i := range channels {
					id := int64(i + 1)
					channels[i] = fmt.Sprintf("p_%dv0", id)
					shard := c.shard(id)
					shard.mu.Lock()
					entry := &cachedSchema{info: info}
					shard.bodies.store(schemaIdentity{id, 0}, entry)
					shard.writeVersions[id] = &cachedWriteVersion{version: 0}
					shard.mu.Unlock()
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						var err error
						if write {
							_, err = c.GetCurrentWriteSchema(ctx, GetCurrentWriteSchemaRequest{VChannel: channels[i]})
						} else {
							_, err = c.GetSchemaVersion(ctx, GetSchemaVersionRequest{VChannel: channels[i], Version: 0})
						}
						if err != nil {
							b.Error(err)
							return
						}
						i = (i + 1) % len(channels)
					}
				})
				b.StopTimer()
			})
		}
	}
}

func BenchmarkSchemaCacheExpire(b *testing.B) {
	for _, entries := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			c := newSchemaCache[int, int](time.Hour)
			for i := range entries {
				c.store(i, i)
			}
			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				c.expire(now)
			}
		})
	}
}
