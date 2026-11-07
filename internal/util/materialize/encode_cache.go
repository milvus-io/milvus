package materialize

import (
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	lru "github.com/hashicorp/golang-lru/v2"
)

// The worker writes one batch per request, and the stock IPC writer starts
// every stream by encoding the Schema message (and Write compares the
// record's schema with the writer's). Both are fixed per-batch costs. So
// BuildBatch hands out one *arrow.Schema per batch layout, and EncodeBatch
// keeps, per such schema and codec, writers that have already written the
// Schema message: a pooled writer encodes only the record batch, and the
// Schema message is the one its first write produced. The bytes are the
// ones a fresh writer produces (TestEncodeBatch_MatchesStreamWriter).

// batchLayoutCacheSize bounds the layouts kept; a layout is a collection's
// set of output fields, so a process sees few.
const batchLayoutCacheSize = 256

// batchLayout is one cached batch schema with its writer pools.
type batchLayout struct {
	schema *arrow.Schema
	pools  [2]encoderPool // CodecNone, CodecZstd
}

type encoderPool struct {
	mu        sync.Mutex
	schemaMsg []byte
	free      sync.Pool // *pooledEncoder
}

type pooledEncoder struct {
	w  *ipc.Writer
	mw *messageWriter
}

type layoutCache struct {
	byKey    *lru.Cache[string, *batchLayout]
	bySchema sync.Map // *arrow.Schema -> *batchLayout
}

func newLayoutCache(size int) *layoutCache {
	c := &layoutCache{}
	byKey, err := lru.NewWithEvict(size, func(_ string, l *batchLayout) {
		c.bySchema.Delete(l.schema)
	})
	if err != nil {
		panic(err) // only for a non-positive size
	}
	c.byKey = byKey
	return c
}

var batchLayouts = newLayoutCache(batchLayoutCacheSize)

// schemaFor returns the cached schema for fields and md, building and
// caching it on a miss.
func (c *layoutCache) schemaFor(fields []arrow.Field, md arrow.Metadata) *arrow.Schema {
	key := layoutKey(fields, md)
	if l, ok := c.byKey.Get(key); ok {
		return l.schema
	}
	l := &batchLayout{schema: arrow.NewSchema(fields, &md)}
	// bySchema first: once l is in byKey the LRU may evict it at any time,
	// and the evict callback's Delete must find the entry it removes. Storing
	// after the add could follow that Delete and leave an entry the LRU no
	// longer tracks.
	c.bySchema.Store(l.schema, l)
	if prev, ok, _ := c.byKey.PeekOrAdd(key, l); ok {
		// Another caller added the key first: drop ours (nobody else has
		// l.schema).
		c.bySchema.CompareAndDelete(l.schema, l)
		return prev.schema
	}
	return l.schema
}

// layout returns the cached layout of schema, nil when schema did not come
// from schemaFor.
func (c *layoutCache) layout(schema *arrow.Schema) *batchLayout {
	l, ok := c.bySchema.Load(schema)
	if !ok {
		return nil
	}
	return l.(*batchLayout)
}

// layoutKey identifies everything the IPC Schema message encodes for a flat
// schema: every field's name, nullability, type (its String names the
// parameters too, e.g. a fixed-size binary's width) and metadata, and the
// schema metadata. BuildBatch schemas are native-endian.
func layoutKey(fields []arrow.Field, md arrow.Metadata) string {
	var b strings.Builder
	for _, f := range fields {
		b.WriteString(f.Name)
		b.WriteByte(0)
		b.WriteString(f.Type.String())
		b.WriteByte(0)
		b.WriteString(strconv.FormatBool(f.Nullable))
		writeMetadata(&b, f.Metadata)
		b.WriteByte(1)
	}
	b.WriteByte(2)
	writeMetadata(&b, md)
	return b.String()
}

func writeMetadata(b *strings.Builder, md arrow.Metadata) {
	for i, k := range md.Keys() {
		b.WriteString(strconv.Itoa(len(k)))
		b.WriteByte(':')
		b.WriteString(k)
		v := md.Values()[i]
		b.WriteString(strconv.Itoa(len(v)))
		b.WriteByte(':')
		b.WriteString(v)
	}
}

// encodePooled is encodeBatch with a pooled writer. The returned Schema
// message is shared by every call for the layout; callers must not modify it.
func (l *batchLayout) encodePooled(rec arrow.Record, codec Codec, opts []ipc.Option) ([]byte, []byte, error) {
	p := &l.pools[0]
	if codec == CodecZstd {
		p = &l.pools[1]
	}
	enc, _ := p.free.Get().(*pooledEncoder)
	if enc == nil {
		mw := &messageWriter{}
		enc = &pooledEncoder{w: ipc.NewWriterWithPayloadWriter(mw, opts...), mw: mw}
	}
	if err := enc.w.Write(rec); err != nil {
		// The writer's state is unknown: drop it.
		return nil, nil, wrapWriteErr(err)
	}
	msgs := enc.mw.msgs
	enc.mw.msgs = nil
	var schemaMsg, batch []byte
	switch len(msgs) {
	case 2: // a new writer: the Schema message, then the batch
		p.mu.Lock()
		if p.schemaMsg == nil {
			p.schemaMsg = msgs[0]
		}
		schemaMsg = p.schemaMsg
		p.mu.Unlock()
		batch = msgs[1]
	case 1:
		p.mu.Lock()
		schemaMsg = p.schemaMsg
		p.mu.Unlock()
		batch = msgs[0]
	default:
		return nil, nil, errMessageCount(len(msgs))
	}
	p.free.Put(enc)
	return schemaMsg, batch, nil
}
