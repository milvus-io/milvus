package dql

import (
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/resource"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// materializeFill builds a Materialize request's result straight from the
// body entries of every shard's reply (design doc 20261019 §7): each reply
// is read on arrival (add), and finish gathers every column once.
//
// Output order:
//   - requery (order != nil): the requested ids' order, which is the
//     search result's. A requested pk no reply returned is left out; the
//     requery then sees its result differ from the ids it asked for, so
//     organize takes its general path and reports the missing id as today.
//   - filter query: arrival order, the first time a pk is seen. Not sorted by
//     pk: an intended change for the queries Materialize serves, none of
//     which pages, iterates or limits (materializeInScope).
//
// Duplicate pks: within one shard the newest timestamp wins, the first seen
// on a tie or a zero timestamp, as ReduceByPKTS decides on the delegator
// today; across shards a pk is a DataIntegrity error, as the proxy's
// NewSortAndCheckPKPageOperator raises today.
//
// The batches read the replies' bodies in place (design doc 20261019 §6),
// so the fill holds every body it read until release, which runs after
// finish.
//
// add is safe for concurrent use. Reading the entries, pks included, happens
// outside the lock, and so does a requery's slot lookup (its table is
// complete from the start and only read); a filter query's slot lookup and
// slot creation, which adds pks to the table, hold it.
type materializeFill struct {
	maxOutputSize int64
	// known is the schema the request named; schemaKey, when set, is where
	// a schema a reply carried is cached (materializeSchemas).
	known     materializeSchemaRef
	schemaKey *materializeSchemaKey
	// fields resolves field ids for the gather.
	fields *typeutil.SchemaHelper

	mu     sync.Mutex
	closed bool
	err    error  // see fail
	reason string // see fallBack

	layout  *materialize.Schema // every batch shares it
	fp      uint64
	batches []*materialize.Batch
	bodies  []*resource.MaterializeBody // what the batches read from
	shards  map[string]int32            // channel -> shard index

	slots []fillSlot // one per distinct pk
	table pkSlots    // pk -> slot, keyed by the collection's pk type
	// requery only: per requested id, in order, its slot (a pk requested
	// twice shares one slot); nil for a filter query.
	order []int32
	// accepted are the shards' replies read, whose side effects the task
	// applies once the request finishes on Materialize (finishMaterialize).
	accepted []acceptedShard

	allRetrieveCount int64
	scannedRemote    int64
	scannedTotal     int64
	relatedDataSize  int64
	featureBits      uint64
}

// acceptedShard is one shard's reply the fill took.
type acceptedShard struct {
	nodeID  int64
	channel string
	mvcc    uint64
	cost    *internalpb.CostAggregation
}

// fillSlot is where one pk's row comes from; batch < 0 while no reply has
// returned the pk.
type fillSlot struct {
	batch int32
	row   int32
	shard int32
	ts    int64
}

// pkSlots maps a pk to its slot: a slotTable of the collection's pk type,
// chosen once per request.
type pkSlots interface {
	// slotsOf returns the slot of each row's pk in b, in row order, -1 for a
	// pk the table lacks. It only reads the table.
	slotsOf(b *materialize.Batch) []int32
	// getOrInsert returns the slot of row's pk in b, giving it si when it
	// has none (inserted).
	getOrInsert(b *materialize.Batch, row int, si int32) (slot int32, inserted bool)
	stringPK() bool
}

type slotTable[K comparable] struct {
	m   map[K]int32
	pks func(*materialize.Batch) []K
	str bool
}

func newSlotTable[K comparable](n int, str bool, pks func(*materialize.Batch) []K) *slotTable[K] {
	return &slotTable[K]{m: make(map[K]int32, n), pks: pks, str: str}
}

func (t *slotTable[K]) stringPK() bool { return t.str }

func (t *slotTable[K]) slotsOf(b *materialize.Batch) []int32 {
	pks := t.pks(b)
	out := make([]int32, len(pks))
	for r, pk := range pks {
		si, ok := t.m[pk]
		if !ok {
			si = -1
		}
		out[r] = si
	}
	return out
}

func (t *slotTable[K]) getOrInsert(b *materialize.Batch, row int, si int32) (int32, bool) {
	pk := t.pks(b)[row]
	if old, ok := t.m[pk]; ok {
		return old, false
	}
	t.m[pk] = si
	return si, true
}

// prebuild adds the requested pks in order and returns per position its
// slot; a pk requested twice shares one.
func (t *slotTable[K]) prebuild(pks []K) []int32 {
	order := make([]int32, len(pks))
	for i, pk := range pks {
		si, ok := t.m[pk]
		if !ok {
			si = int32(len(t.m))
			t.m[pk] = si
		}
		order[i] = si
	}
	return order
}

func intPKs(b *materialize.Batch) []int64  { return b.IntPKs }
func strPKs(b *materialize.Batch) []string { return b.StrPKs }

// newMaterializeFill returns an empty fill. fields is the collection's
// schema. requested, when set, is the requery's ids: the output follows
// their order and holds only them. known is the schema the request names
// (the zero ref for none), and key, when set, where a schema a reply carries
// is cached.
func newMaterializeFill(fields *typeutil.SchemaHelper, maxOutputSize int64, requested *schemapb.IDs,
	known materializeSchemaRef, key *materializeSchemaKey,
) *materializeFill {
	f := &materializeFill{
		maxOutputSize: maxOutputSize, known: known, schemaKey: key, fields: fields,
		shards: make(map[string]int32),
	}
	switch ids := requested.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		t := newSlotTable(len(ids.IntId.GetData()), false, intPKs)
		f.order = t.prebuild(ids.IntId.GetData())
		f.table, f.slots = t, unfilledSlots(len(t.m))
	case *schemapb.IDs_StrId:
		t := newSlotTable(len(ids.StrId.GetData()), true, strPKs)
		f.order = t.prebuild(ids.StrId.GetData())
		f.table, f.slots = t, unfilledSlots(len(t.m))
	default:
		if pk, err := fields.GetPrimaryKeyField(); err == nil && typeutil.IsStringType(pk.GetDataType()) {
			f.table = newSlotTable(0, true, strPKs)
		} else {
			f.table = newSlotTable(0, false, intPKs)
		}
	}
	return f
}

func unfilledSlots(n int) []fillSlot {
	slots := make([]fillSlot, n)
	for i := range slots {
		slots[i].batch = -1
	}
	return slots
}

// add reads nodeID's reply for channel and assigns its rows; it takes the
// reply's body, which the fill holds until release. A reply read is
// recorded as accepted, for its side effects (acceptedShard), and a schema
// it carried is cached. It returns an error the caller must keep away from
// the load balancer: merr.ErrServiceUnimplemented for a schema that differs
// from another reply's (version skew, the request falls back to Query), and
// anything else fails the request.
func (f *materializeFill) add(nodeID int64, channel string, res *internalpb.MaterializeResults) error {
	body := resource.TakeMaterializeBody(res)
	h := res.GetHeader()
	a := acceptedShard{nodeID: nodeID, channel: channel, mvcc: h.GetMvccTimestamp(), cost: h.GetCostAggregation()}
	empty := body.Len() == 0
	table, open := f.count(h, a, empty)
	if !open || empty {
		body.Release()
		return nil
	}
	schema, err := materializeBodySchema(res, f.known)
	if err != nil {
		body.Release()
		return err
	}
	batches := make([]*materialize.Batch, body.Len())
	// body_raw_size is the reply's total over its entries -- a delegator
	// relays its workers' sum, and the header has no per-entry size -- so it
	// bounds each entry's inflation by the shard's total, not its own size.
	for i := range batches {
		if batches[i], err = schema.ReadEntry(body.Entry(i), i, h.GetBodyRawSize()); err != nil {
			body.Release()
			return err
		}
	}
	if schema.StringPK() != table.stringPK() {
		body.Release()
		return merr.WrapErrServiceInternalMsg("materialize: the body's pk is %s, the request's %s",
			pkTypeName(schema.StringPK()), pkTypeName(table.stringPK()))
	}
	// A requery's table is complete and only read (release drops the
	// reference, it does not clear the map): look the pks up here, outside
	// the lock.
	var looked [][]int32
	if f.order != nil {
		looked = make([][]int32, len(batches))
		for i, b := range batches {
			looked[i] = table.slotsOf(b)
		}
	}

	if err := f.assign(a, h.GetSchemaFingerprint(), schema, body, batches, looked); err != nil {
		return err
	}
	if f.schemaKey != nil && len(h.GetBodySchema()) > 0 {
		materializeSchemas.store(*f.schemaKey, h.GetSchemaFingerprint(), schema)
	}
	return nil
}

// assign takes one reply's read batches under the lock: it checks the
// schema against the other replies', assigns the rows to slots (fillRows),
// with looked, a requery's slots per batch, or for a filter query by
// looking each pk up in the table, and records the reply as accepted.
// On an error before the batches are taken it releases body.
func (f *materializeFill) assign(a acceptedShard, fp uint64, schema *materialize.Schema, body *resource.MaterializeBody,
	batches []*materialize.Batch, looked [][]int32,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		body.Release()
		return nil
	}
	if f.layout == nil {
		f.layout, f.fp = schema, fp
	} else if fp != f.fp {
		body.Release()
		return merr.Wrapf(merr.ErrServiceUnimplemented,
			"materialize: shards replied with schemas %d and %d", f.fp, fp)
	}
	// From here the batches are the fill's: a failure below fails the
	// request, and release frees the body.
	f.bodies = append(f.bodies, body)
	shard, ok := f.shards[a.channel]
	if !ok {
		shard = int32(len(f.shards))
		f.shards[a.channel] = shard
	}
	for i, b := range batches {
		bi := int32(len(f.batches))
		f.batches = append(f.batches, b)
		var slots []int32
		if looked != nil {
			slots = looked[i]
		}
		if err := f.fillRows(b, slots, bi, shard); err != nil {
			return err
		}
	}
	f.accepted = append(f.accepted, a)
	return nil
}

// count adds the reply's statistics, and records an empty reply (no body
// to read) as accepted. It returns the slot table, and false once the fill
// is released or the request falls back to Query, when there is no point
// reading the reply.
func (f *materializeFill) count(h *internalpb.MaterializeHeader, a acceptedShard, empty bool) (pkSlots, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allRetrieveCount += h.GetAllRetrieveCount()
	f.scannedRemote += h.GetScannedRemoteBytes()
	f.scannedTotal += h.GetScannedTotalBytes()
	f.relatedDataSize += h.GetCostAggregation().GetTotalRelatedDataSize()
	f.featureBits |= h.GetFeatureBits()
	open := !f.closed && f.reason == ""
	if open && empty {
		f.accepted = append(f.accepted, a)
	}
	return f.table, open
}

// checkOutputSize applies maxOutputSize where Query applies it: per shard
// (the delegator's reduce, reduce_by_pk_op.go; the proxy has no cross-shard
// total), to the rows kept after deduplication, sized as that reduce sizes
// them (Batch.RowSize). A relayed body is not deduplicated -- it holds every
// worker's rows, pk and system columns and padding -- so its raw size is no
// measure of what Query would accept; the fill is the first place the kept
// rows are known. Each deduplicated slot is charged once, as that reduce
// counts deduplicated rows: a pk a requery asks for twice counts once. f.mu
// is held.
func (f *materializeFill) checkOutputSize() error {
	if f.maxOutputSize <= 0 {
		return nil
	}
	perShard := make([]int64, len(f.shards))
	for _, s := range f.slots {
		if s.batch < 0 {
			continue
		}
		perShard[s.shard] += f.batches[s.batch].RowSize(int(s.row))
		if perShard[s.shard] > f.maxOutputSize {
			return merr.WrapErrParameterInvalidMsg("query results exceed the maxOutputSize Limit %d", f.maxOutputSize)
		}
	}
	return nil
}

func pkTypeName(str bool) string {
	if str {
		return "utf8"
	}
	return "int64"
}

// fillRows assigns one batch's rows to their slots; f.mu is held. A
// requery's rows come with their slots (looked, from slotsOf), -1 for a pk
// not asked for. A filter query's are looked up here, one get-or-insert per
// row: a pk seen for the first time gets a new slot.
func (f *materializeFill) fillRows(b *materialize.Batch, looked []int32, batch, shard int32) error {
	ts := b.Timestamps(f.layout)
	for r := 0; r < b.Rows; r++ {
		var t int64
		if ts != nil {
			t = ts[r]
		}
		var si int32
		if looked != nil {
			if si = looked[r]; si < 0 {
				// Not asked for; organize would drop it too.
				continue
			}
		} else {
			var inserted bool
			if si, inserted = f.table.getOrInsert(b, r, int32(len(f.slots))); inserted {
				f.slots = append(f.slots, fillSlot{batch: batch, row: int32(r), shard: shard, ts: t})
				continue
			}
		}
		s := &f.slots[si]
		switch {
		case s.batch < 0:
			*s = fillSlot{batch: batch, row: int32(r), shard: shard, ts: t}
		case looked == nil && s.batch == batch:
			// A worker's batch holds each pk once (its reduce dedups).
			return merr.WrapErrServiceInternalMsg("materialize: pk %v twice in one batch", f.pkAt(b, r))
		case s.shard != shard:
			return merr.WrapErrDataIntegrityMsg("duplicate PK %v found across shards", f.pkAt(b, r))
		case t != 0 && t > s.ts:
			// ReduceByPKTS: only a strictly newer, non-zero timestamp
			// replaces the row kept.
			s.batch, s.row, s.ts = batch, int32(r), t
		}
	}
	return nil
}

func (f *materializeFill) pkAt(b *materialize.Batch, r int) any {
	if f.layout.StringPK() {
		return b.StrPKs[r]
	}
	return b.IntPKs[r]
}

// Fallback reasons, the materialize_fallback_total reason label.
const (
	// fallbackUnimplemented: a node without the Materialize RPC.
	fallbackUnimplemented = "unimplemented"
	// fallbackUnsupported: a node that cannot serve this request's shape
	// (an element-level result, a field not carried as Arrow, workers
	// with different schemas).
	fallbackUnsupported = "unsupported"
	// fallbackSchemaSkew: shards replied with different schemas.
	fallbackSchemaSkew = "schema_skew"
)

// fail records a failure the load balancer must not see; the first one
// wins, unless a fallback replaces it.
func (f *materializeFill) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err == nil {
		f.err = err
	}
}

// fallBack records a merr.ErrServiceUnimplemented failure for which the
// request reruns with Query. It replaces any other failure; the first
// fallback wins.
func (f *materializeFill) fallBack(reason string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reason == "" {
		f.err, f.reason = err, reason
	}
}

// failure returns the fallback reason when the request must rerun with
// Query ("" otherwise), and the failure recorded.
func (f *materializeFill) failure() (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reason, f.err
}

// release drops every batch and slot and frees the bodies the batches were
// read from. A reply still being added afterwards releases its own.
func (f *materializeFill) release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	for _, b := range f.bodies {
		b.Release()
	}
	f.bodies, f.batches = nil, nil
	// order stays: add reads it without the lock.
	f.slots, f.table, f.accepted = nil, nil, nil
}

// finish gathers the selected rows into the result, one copy per value,
// from the received pieces into the final FieldData. It runs once every
// reply was added and before release.
func (f *materializeFill) finish() (*internalpb.RetrieveResults, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := &internalpb.RetrieveResults{
		Status:             merr.Success(),
		AllRetrieveCount:   f.allRetrieveCount,
		ScannedRemoteBytes: f.scannedRemote,
		ScannedTotalBytes:  f.scannedTotal,
		CostAggregation:    &internalpb.CostAggregation{TotalRelatedDataSize: f.relatedDataSize},
		FeatureBits:        f.featureBits,
	}

	// The output rows, from the slots.
	var rows []materialize.RowRef
	if f.order == nil {
		rows = make([]materialize.RowRef, len(f.slots))
		for i, s := range f.slots {
			rows[i] = materialize.RowRef{Batch: s.batch, Row: s.row}
		}
	} else {
		rows = make([]materialize.RowRef, 0, len(f.order))
		for _, si := range f.order {
			if s := f.slots[si]; s.batch >= 0 {
				rows = append(rows, materialize.RowRef{Batch: s.batch, Row: s.row})
			}
		}
	}
	if len(rows) == 0 {
		return out, nil
	}
	if err := f.checkOutputSize(); err != nil {
		return nil, err
	}
	ids, fields, err := f.layout.Gather(f.batches, rows, f.fields)
	if err != nil {
		return nil, err
	}
	out.Ids = ids
	out.FieldsData = fields
	return out, nil
}

// materializeBodySchema returns the schema to read the reply's body with:
// the reply's own bytes, checked against its fingerprint and parsed unless
// the proxy already holds that schema, or known when the reply omitted them
// because the request named known's fingerprint. Any other combination is a
// Milvus bug.
func materializeBodySchema(res *internalpb.MaterializeResults, known materializeSchemaRef) (*materialize.Schema, error) {
	h := res.GetHeader()
	fp := h.GetSchemaFingerprint()
	if s := h.GetBodySchema(); len(s) > 0 {
		if materialize.SchemaFingerprint(s) != fp {
			return nil, merr.WrapErrServiceInternalMsg("materialize: schema bytes do not match fingerprint %d", fp)
		}
		if known.schema != nil && known.fingerprint == fp {
			return known.schema, nil
		}
		if cached := materializeSchemas.parsed(fp); cached != nil {
			return cached, nil
		}
		return materialize.ParseSchema(s)
	}
	if known.schema != nil && known.fingerprint == fp {
		return known.schema, nil
	}
	return nil, merr.WrapErrServiceInternalMsg("materialize: reply omitted schema %d, the request named %d", fp, known.fingerprint)
}
