package dql

import (
	"strconv"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/milvus-io/milvus/internal/util/materialize"
)

// materializeSchemaCacheSize bounds each map of the process-wide cache.
const materializeSchemaCacheSize = 1024

// materializeSchemaKey names the Arrow schema a Materialize request expects:
// the collection, its schema version and the ordered output field ids. A
// key only proposes a fingerprint; the worker compares it with the batch's
// real schema and sends the bytes when they differ, so a stale key costs
// bytes on the wire, never a wrong decode.
type materializeSchemaKey struct {
	collectionID  int64
	schemaVersion int32
	outputFields  string
}

func newMaterializeSchemaKey(collectionID int64, schemaVersion int32, outputFieldIDs []int64) materializeSchemaKey {
	var b strings.Builder
	for i, id := range outputFieldIDs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(id, 10))
	}
	return materializeSchemaKey{collectionID: collectionID, schemaVersion: schemaVersion, outputFields: b.String()}
}

// materializeSchemaRef is a resolved schema: its fingerprint and the parsed
// schema (which also holds the encapsulated Arrow IPC Schema message). The
// zero value means none.
type materializeSchemaRef struct {
	fingerprint uint64
	schema      *materialize.Schema
}

// materializeSchemaCache holds the Arrow schemas the proxy has received,
// parsed, so that a request can name one by fingerprint, the workers omit its
// bytes (design doc 20261019 §2), and no reply parses a schema the proxy
// already holds. Both maps are bounded LRUs and safe for concurrent use.
type materializeSchemaCache struct {
	byKey         *lru.Cache[materializeSchemaKey, uint64]
	byFingerprint *lru.Cache[uint64, *materialize.Schema]
}

func newMaterializeSchemaCache(capacity int) *materializeSchemaCache {
	byKey, err := lru.New[materializeSchemaKey, uint64](capacity)
	if err != nil {
		panic(err) // only for a non-positive capacity
	}
	byFingerprint, err := lru.New[uint64, *materialize.Schema](capacity)
	if err != nil {
		panic(err)
	}
	return &materializeSchemaCache{byKey: byKey, byFingerprint: byFingerprint}
}

var materializeSchemas = newMaterializeSchemaCache(materializeSchemaCacheSize)

// resolve returns the schema cached for key, or the zero ref on a miss. The
// caller keeps the returned schema for the whole request, so a later
// eviction cannot strand a reply that omitted the bytes.
func (c *materializeSchemaCache) resolve(key materializeSchemaKey) materializeSchemaRef {
	fp, ok := c.byKey.Get(key)
	if !ok {
		return materializeSchemaRef{}
	}
	s, ok := c.byFingerprint.Get(fp)
	if !ok {
		return materializeSchemaRef{}
	}
	return materializeSchemaRef{fingerprint: fp, schema: s}
}

// parsed returns the cached schema with fingerprint fp, nil if none.
func (c *materializeSchemaCache) parsed(fp uint64) *materialize.Schema {
	s, _ := c.byFingerprint.Get(fp)
	return s
}

// store records a schema a reply carried; the fill has checked its bytes
// hash to fp (materializeBodySchema).
func (c *materializeSchemaCache) store(key materializeSchemaKey, fp uint64, schema *materialize.Schema) {
	c.byFingerprint.Add(fp, schema)
	c.byKey.Add(key, fp)
}
