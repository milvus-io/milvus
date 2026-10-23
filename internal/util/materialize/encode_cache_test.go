package materialize

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

// builtBatch is BuildBatch over n rows of userRecord.
func builtBatch(t *testing.T, n int) arrow.Record {
	t.Helper()
	vals := make([]string, n)
	pairs := make([][2]int64, n)
	pks := make([]int64, n)
	ts := make([]int64, n)
	for i := range vals {
		vals[i] = fmt.Sprintf("v%d", i*7)
		pairs[i] = [2]int64{0, int64(i)}
		pks[i] = int64(i)
		ts[i] = int64(100 + i)
	}
	u := userRecord(t, vals)
	defer u.Release()
	rec, err := BuildBatch(&segcorepb.RetrieveResults{Ids: intIDs(pks...), FieldsData: []*schemapb.FieldData{tsField(ts...)}},
		&queryutil.ArrowSelection{Records: []arrow.Record{u}, Rows: queryutil.RowRefsForTest(pairs)})
	require.NoError(t, err)
	return rec
}

// BuildBatch hands out one schema per layout, and EncodeBatch's pooled
// writers produce, call after call and concurrently, the bytes a fresh
// writer produces.
func TestEncodeBatch_PooledMatchesFresh(t *testing.T) {
	a, b := builtBatch(t, 3), builtBatch(t, 40)
	defer a.Release()
	defer b.Release()
	require.Same(t, a.Schema(), b.Schema())
	require.NotNil(t, batchLayouts.layout(a.Schema()))

	for _, codec := range []Codec{CodecNone, CodecZstd} {
		opts := []ipc.Option{ipc.WithSchema(a.Schema()), ipc.WithAllocator(memory.DefaultAllocator)}
		if codec == CodecZstd {
			opts = append(opts, ipc.WithZstd(), ipc.WithMinSpaceSavings(minSpaceSavings))
		}
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					rec := a
					if (g+i)%2 == 1 {
						rec = b
					}
					wantS, wantE, err := encodeBatch(rec, opts...)
					if !assert.NoError(t, err) {
						return
					}
					gotS, gotE, err := EncodeBatch(rec, codec)
					if !assert.NoError(t, err) {
						return
					}
					assert.Equal(t, wantS, gotS)
					assert.Equal(t, wantE, gotE)
				}
			}()
		}
		wg.Wait()
	}
}

func TestLayoutCache(t *testing.T) {
	c := newLayoutCache(1)
	f := func(name string) []arrow.Field { return []arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int64}} }
	md := arrow.NewMetadata([]string{"k"}, []string{"v"})
	s1 := c.schemaFor(f("a"), md)
	require.Same(t, s1, c.schemaFor(f("a"), md))
	require.NotNil(t, c.layout(s1))
	// Anything the Schema message encodes makes another layout.
	require.NotSame(t, s1, c.schemaFor(f("a"), arrow.NewMetadata([]string{"k"}, []string{"w"})))
	// Size 1: the first layout was evicted, and so was its schema's entry.
	require.Nil(t, c.layout(s1))
	nullable := []arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}
	require.NotEqual(t, layoutKey(f("a"), md), layoutKey(nullable, md))
	fsb := func(w int) []arrow.Field {
		return []arrow.Field{{Name: "a", Type: &arrow.FixedSizeBinaryType{ByteWidth: w}}}
	}
	require.NotEqual(t, layoutKey(fsb(4), md), layoutKey(fsb(8), md))
	// A schema that did not come from the cache has no layout.
	require.Nil(t, c.layout(arrow.NewSchema(f("a"), &md)))
}

// Under eviction churn from many goroutines, bySchema keeps only layouts the
// LRU still tracks: an eviction racing an add cannot leave a stale entry.
func TestLayoutCache_ChurnLeavesNoStaleEntry(t *testing.T) {
	c := newLayoutCache(4)
	md := arrow.NewMetadata(nil, nil)
	field := func(i int) []arrow.Field {
		return []arrow.Field{{Name: strconv.Itoa(i), Type: arrow.PrimitiveTypes.Int64}}
	}
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				s := c.schemaFor(field((g*7+i)%40), md)
				assert.Equal(t, strconv.Itoa((g*7+i)%40), s.Field(0).Name)
			}
		}()
	}
	wg.Wait()
	entries := 0
	c.bySchema.Range(func(k, v any) bool {
		entries++
		l := v.(*batchLayout)
		tracked, ok := c.byKey.Peek(layoutKey(l.schema.Fields(), md))
		require.True(t, ok, "bySchema holds a layout the LRU evicted")
		require.Same(t, l, tracked)
		return true
	})
	require.LessOrEqual(t, entries, 4)
}
