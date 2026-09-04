// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package routing maps a write to the vchannel that owns it.
//
// A row is placed by one number, its routing value: the hash of whatever the
// collection's shard_by expression names — its primary key, or its namespace id.
// The namespace id is only a valid key for a collection whose rows have ALWAYS
// been placed by it (namespace.sharding.enabled=true in partition_key mode);
// the routing commit is where that is checked, not here.
// The value is then taken modulo the collection's routing modulus, and the shard
// owning that residue owns the row.
//
// A collection that has never been split carries no residues and a modulus of
// zero. It is not a second code path: the table built for it is the residue
// table with one residue per shard in vchannel order, which is the legacy
// hash % shardNum placement bit for bit.
package routing

import (
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// NamespaceIDField is the system field a namespace-sharded collection routes by.
// It is the one system field shard_by can name today.
const NamespaceIDField = "$namespace_id"

// Table is the single routing entry point: it maps one write to the vchannel
// that owns it.
//
// Every collection routes the same way — the routing value of a row, modulo the
// collection's routing modulus. What differs is only which value that is, which
// the collection's shard_by expression names. Callers that already hold the
// value use RouteInsertHashes and RouteDeleteHashes; RouteInsert and RouteDelete
// are the primary-key form, and refuse a collection that routes by anything
// else rather than placing every row wrong.
type Table struct {
	// residues resolves a routing value to its owning vchannel. Never nil in a
	// table Derive returned; for a never-split collection it is the compat table,
	// which reproduces the legacy modulo exactly.
	residues *ResidueTable

	// channels is the collection's vchannel list, in meta order.
	channels []string

	// explicit records whether the residues came from the collection meta or were
	// synthesized from the channel order for a never-split collection. It changes
	// no placement; it is what lets a caller report which rule applied.
	explicit bool

	// owners is the number of vchannels that own at least one residue, computed
	// once by Derive so NumShards does not walk the M slots per call.
	owners int

	// routingField is the field named by shard_by, empty when shard_by was not
	// declared. pkField is the collection's primary key. RouteInsert hashes the
	// primary key, so it is valid exactly when the two agree.
	routingField string
	pkField      string
}

// Option configures a Table at Derive time.
type Option func(*Table) error

// WithShardBy declares the collection's shard_by expression and the name of its
// primary key field, so the table knows whether hashing the primary key places a
// row correctly.
//
// Without it a table assumes the primary key, which is what shard_by means when
// it is empty — every collection until its first split.
func WithShardBy(shardBy, primaryKeyField string) Option {
	return func(t *Table) error {
		field, err := ParseShardBy(shardBy)
		if err != nil {
			return err
		}
		t.routingField, t.pkField = field, primaryKeyField
		return nil
	}
}

// ParseShardBy parses a collection's shard_by expression and returns the field
// whose hash places a row. An empty expression returns an empty field: the
// routing was never declared, which is every collection until its first split,
// and the primary key applies.
//
// The grammar is hash(<field>), where <field> is taken whole rather than lexed
// as a user field name — one system field, $namespace_id, does not fit the rules
// a user field name follows. The spelling is canonical and compared bytewise: no
// space, no quoting, no case folding. A non-empty expression that does not match
// is rejected whole rather than parsed in part, since a partial read would route
// by a guess.
func ParseShardBy(expr string) (string, error) {
	if expr == "" {
		return "", nil
	}
	const prefix = "hash("
	if !strings.HasPrefix(expr, prefix) {
		return "", merr.WrapErrServiceInternalMsg("shard_by %q is not a hash(<field>) expression", expr)
	}
	rest := expr[len(prefix):]
	// The closing parenthesis must be the last byte, and the only one: an
	// unterminated expression, bytes after the parenthesis, or a second one
	// inside the field all mean the server emitted something this reader does
	// not understand. HasSuffix rather than Index: on the input "hash(" the
	// index of a missing parenthesis (-1) equals len(rest)-1, and slicing by
	// it would panic.
	if !strings.HasSuffix(rest, ")") || strings.Count(rest, ")") != 1 {
		return "", merr.WrapErrServiceInternalMsg("shard_by %q is not a hash(<field>) expression", expr)
	}
	field := rest[:len(rest)-1]
	if field == "" {
		return "", merr.WrapErrServiceInternalMsg("shard_by %q names no field", expr)
	}
	// The field is taken whole, but whitespace is never part of a field name
	// (user names are identifiers, the system name is $namespace_id), so a
	// padded spelling is rejected here rather than accepted and failed later
	// by RoutesByPrimaryKey, which would report it as a routing-field mismatch.
	if strings.ContainsAny(field, " \t\r\n") {
		return "", merr.WrapErrServiceInternalMsg("shard_by %q carries whitespace in its field", expr)
	}
	return field, nil
}

// Shard is one shard's routing meta as Derive consumes it: the vchannel plus the
// residues read from its CollectionShardInfo.
type Shard struct {
	Vchannel string
	Buckets  []uint64
}

// Derive builds the routing table of a collection from its routing modulus and
// per-shard residues.
//
// Shards must already be filtered to the ones that currently accept writes, which
// is what ShardsFromMeta does: a fenced split source and a released one own no
// keys, and their key space belongs to the targets. Derive then validates that
// what remains covers the key space exactly — a gap or an overlap is an error,
// never a silent mis-route.
//
// The modulus is what says whether the collection has been split, not the
// presence of residues. Zero means never split, and the table is built from the
// channel order; non-zero means the meta must say which residues each shard owns,
// and a topology that does not is rejected rather than quietly downgraded to the
// legacy modulo — which, over a vchannel list that a split has already grown,
// would re-place every row in the collection.
func Derive(modulus uint64, channels []string, shards []Shard, opts ...Option) (*Table, error) {
	if len(channels) == 0 {
		// The legacy rule divides by the channel count and the explicit rule
		// indexes into it; neither has an answer here, and a table that routes
		// nowhere is not a table.
		return nil, merr.WrapErrServiceInternal("routing table needs at least one vchannel")
	}
	t := &Table{channels: append([]string(nil), channels...)}
	for _, opt := range opts {
		if err := opt(t); err != nil {
			return nil, err
		}
	}

	explicit := false
	for _, s := range shards {
		if len(s.Buckets) > 0 {
			explicit = true
			break
		}
	}

	switch {
	case modulus == 0 && explicit:
		return nil, merr.WrapErrServiceInternal("shards carry residues but the collection reports no routing modulus")
	case modulus != 0 && !explicit:
		return nil, merr.WrapErrServiceInternalMsg(
			"collection reports routing modulus %d but no shard carries a residue", modulus)
	case modulus == 0:
		// The legacy rule is "shard i owns residue i at modulus len(channels)",
		// so a shorter shard list means the caller declared some vchannel
		// non-writable while the collection reports no modulus -- meta that
		// cannot be produced by a real split, since retiring a shard is what
		// writes residues in the first place. Deriving anyway would route the
		// excluded shard's residue straight back to it. A caller with no shard
		// info at all is the ordinary never-split case and is left alone.
		if len(shards) != 0 && len(shards) != len(channels) {
			return nil, merr.WrapErrServiceInternalMsg(
				"collection reports no routing modulus but only %d of %d vchannels own keys",
				len(shards), len(channels))
		}
		// Same length is not the same set. A shard list that names a vchannel
		// the collection does not carry is malformed on this branch exactly as
		// on the explicit one; deriving from the channel list alone would just
		// hide it.
		if len(shards) != 0 {
			if err := refuseShardsOutside(channels, shards); err != nil {
				return nil, err
			}
		}
		residues, err := deriveCompat(channels)
		if err != nil {
			return nil, err
		}
		t.residues = residues
		return t, nil
	}

	// A shard the table's OWN channel list does not carry cannot be routed to
	// by any caller of this table, and no meta refresh changes that. Left in,
	// it would surface later from owner() as ErrCollectionRoutingStale --
	// retriable -- so every retry consumer would loop until its deadline on a
	// condition that is permanently malformed meta. Refuse it here, where the
	// two cases are still distinguishable, and non-retriably.
	if err := refuseShardsOutside(t.channels, shards); err != nil {
		return nil, err
	}
	hashShards := make([]HashShard, 0, len(shards))
	for _, s := range shards {
		hashShards = append(hashShards, HashShard(s))
	}
	residues, err := DeriveHash(modulus, hashShards)
	if err != nil {
		return nil, err
	}
	t.residues, t.explicit = residues, true
	t.owners = len(hashShards)
	return t, nil
}

// IsExplicit reports whether the table routes by the per-shard residues in the
// meta rather than by the channel order, i.e. whether the collection has been
// split at least once. Nil-safe.
func (t *Table) IsExplicit() bool { return t != nil && t.explicit }

// NumShards returns the number of shards the collection routes over. Nil-safe,
// like the other predicates on this type.
func (t *Table) NumShards() int {
	if t == nil {
		return 0
	}
	if !t.explicit {
		return len(t.channels)
	}
	// The vchannel list only ever grows: a split retires its source but keeps
	// the name, so len(channels) counts shards that own no key. Answer with the
	// shards that actually own residues instead -- that is the number a caller
	// asking "how many shards does this collection route over" means. Derive
	// counted them: every shard it accepted owns at least one residue
	// (DeriveHash refuses an empty residue set) and no vchannel appears twice
	// (refuseShardsOutside refuses a duplicate).
	return t.owners
}

// RoutesByPrimaryKey reports whether hashing a row's primary key yields its
// routing value — true when shard_by was not declared, and when it names the
// primary key itself. Nil-safe.
func (t *Table) RoutesByPrimaryKey() bool {
	if t == nil {
		return true
	}
	return t.routingField == "" || t.routingField == t.pkField
}

// LegacyShards returns the residue assignment of a collection that has NEVER
// been split: modulus len(vchannels), and shard i owning residue i.
//
// That assignment is what hash % len(vchannels) already does, so it is the shape
// an existing collection is in before anything writes residues for it. It is
// exported because the alternative is every caller that needs to plan a first
// split re-deriving the convention, and a caller that enumerates its shards from
// a map rather than from the vchannel list produces a PERMUTATION of it -- which
// tiles the key space just as exactly and is therefore invisible to every check
// this package makes downstream. One implementation, in the order the vchannel
// list defines.
func LegacyShards(vchannels []string) (uint64, []Shard, error) {
	if len(vchannels) == 0 {
		return 0, nil, merr.WrapErrServiceInternal("a collection with no vchannel has no residue assignment")
	}
	seen := make(map[string]struct{}, len(vchannels))
	shards := make([]Shard, 0, len(vchannels))
	for i, vchannel := range vchannels {
		if vchannel == "" {
			return 0, nil, merr.WrapErrServiceInternalMsg("vchannel %d carries no name", i)
		}
		if _, dup := seen[vchannel]; dup {
			return 0, nil, merr.WrapErrServiceInternalMsg("vchannel %q is listed twice", vchannel)
		}
		seen[vchannel] = struct{}{}
		shards = append(shards, Shard{Vchannel: vchannel, Buckets: []uint64{uint64(i)}})
	}
	return uint64(len(vchannels)), shards, nil
}

// refuseShardsOutside rejects, non-retriably, a shard whose vchannel the
// collection does not carry. No caller of the resulting table could route to
// it, and no meta refresh changes that, so it must not become the retriable
// ErrCollectionRoutingStale that owner() would otherwise report on every key.
func refuseShardsOutside(channels []string, shards []Shard) error {
	known := make(map[string]struct{}, len(channels))
	for _, ch := range channels {
		known[ch] = struct{}{}
	}
	seen := make(map[string]struct{}, len(shards))
	for _, s := range shards {
		if _, ok := known[s.Vchannel]; !ok {
			return merr.WrapErrServiceInternalMsg(
				"routing shard %q is not in the collection's vchannel list", s.Vchannel)
		}
		// A subset of the same length is not the same set: [v0, v0] against
		// [v0, v1] would pass the length check on the compat branch and on the
		// explicit branch would let one vchannel claim two residue sets under
		// two entries. ShardsFromMeta cannot produce this, but Derive is exported
		// and the check is what makes the compat-branch length comparison mean
		// equality.
		if _, dup := seen[s.Vchannel]; dup {
			return merr.WrapErrServiceInternalMsg(
				"routing shard %q appears twice in the collection's shard list", s.Vchannel)
		}
		seen[s.Vchannel] = struct{}{}
	}
	return nil
}

// ShardsFromMeta converts the per-shard routing meta of a DescribeCollection
// response into Derive's input, keeping only the shards that currently accept
// writes.
//
// ShardNormal (serving) and ShardCreating (a split target, already created and
// writable) participate; the fenced split source (ShardSplitting) and the
// released one (ShardDropped) are excluded, because their key space now belongs
// to the targets. Excluding them is what keeps the remainder an exact cover.
//
// A state this build does not know may own keys, so it fails rather than being
// dropped: dropping it silently re-routes whatever it owned, and mapping it onto
// the zero value would make a shard that takes no writes look like one that does.
func ShardsFromMeta(vchannels []string, infos []*schemapb.CollectionShardInfo) ([]Shard, error) {
	if len(infos) == 0 {
		// Not a mismatch: this is the never-split shape. The proto documents an
		// empty shard_infos as "the peer predates shard split", and today's
		// rootcoord never populates it at all, so EVERY current
		// DescribeCollection response looks like this. Derive already reads no
		// shard info as the legacy assignment; answer the same way here rather
		// than refusing, non-retriably, to build a table for every collection
		// that has never been split.
		return nil, nil
	}
	if len(infos) != len(vchannels) {
		return nil, merr.WrapErrServiceInternalMsg("routing shard info count %d mismatches vchannel count %d",
			len(infos), len(vchannels))
	}
	shards := make([]Shard, 0, len(vchannels))
	for i, vchannel := range vchannels {
		// The two arrays are parallel by convention, and nothing downstream can
		// catch a violation: a PERMUTED infos still tiles [0, M) exactly, so
		// Derive, DeriveHash and DeriveHashPartial all accept it and every
		// residue ends up bound to a shard that does not own it -- inserts land
		// on the wrong shard, deletes match nothing, and no error is raised
		// anywhere. The name is the only thing that can detect it, so check it
		// wherever the producer set it. An empty name is tolerated: it is the
		// persisted shape of a collection created before the field existed.
		if name := infos[i].GetVchannelName(); name != "" && name != vchannel {
			return nil, merr.WrapErrServiceInternalMsg(
				"routing shard info %d names vchannel %q but the vchannel list has %q at that position",
				i, name, vchannel)
		}
		switch state := infos[i].GetState(); state {
		case schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardCreating:
			// A Creating shard is admitted because a split's targets are
			// write-routable from the routing commit onward, before they are
			// serviceable for reads. That relies on an invariant the commit
			// owns: a Creating entry is published WITH its residues, in the same
			// transaction. One published without them makes Derive refuse the
			// whole table -- loudly, and for the entire collection, not only for
			// the shard at fault.
			shards = append(shards, Shard{
				Vchannel: vchannel,
				Buckets:  infos[i].GetHashRouting().GetBuckets(),
			})
		case schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardDropped:
			// Owns no keys; the targets carved from it own them now.
		default:
			return nil, merr.WrapErrServiceInternalMsg("shard %q reports shard state %d, which this build does not know",
				vchannel, int32(state))
		}
	}
	return shards, nil
}

// HashPrimaryKeys returns each primary key's routing value — the same value the
// split's rewrite partitioner computes, so a row the write path sends to a shard
// is the row the rewrite would put there. Nil for an unsupported id type,
// matching typeutil.HashPK2Channels.
//
// The width matters: the hash is a uint32 widened to uint64, NOT reduced modulo
// anything. Reducing first (as the legacy path did, by the shard count) would
// destroy the bits the residues are cut on.
func HashPrimaryKeys(pks *schemapb.IDs) ([]uint64, error) {
	var out []uint64
	switch pks.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		data := pks.GetIntId().GetData()
		out = make([]uint64, 0, len(data))
		for _, pk := range data {
			h, err := typeutil.Hash32Int64(pk)
			if err != nil {
				// Never swallowed and never turned into "no rows": a batch that
				// silently placed nothing would be reported to the client as a
				// successful write of rows no shard ever received.
				return nil, merr.WrapErrServiceInternalErr(err, "cannot hash primary key %d", pk)
			}
			out = append(out, uint64(h))
		}
	case *schemapb.IDs_StrId:
		data := pks.GetStrId().GetData()
		out = make([]uint64, 0, len(data))
		for _, pk := range data {
			out = append(out, uint64(typeutil.HashString2Uint32(pk)))
		}
	case nil:
		// No id field at all is ZERO ROWS, not unhashable rows -- GetSizeOfIDs
		// says the same -- and the delete path does reach here with an empty
		// batch off a query stream. Placing nothing is the right answer.
	default:
		// An id field this build does not know, on the other hand, carries rows
		// it cannot hash. Falling through would hand the caller an empty routing
		// result and no error: the "successful write of rows no shard received"
		// the IntId branch above refuses to produce.
		//
		// Untestable from outside milvus-proto -- the oneof interface has an
		// unexported method, so no test can fabricate a variant this build does
		// not know. It guards the day one is added there.
		return nil, merr.WrapErrServiceInternalMsg(
			"primary keys carry an id field this build cannot hash (%T)", pks.GetIdField())
	}
	return out, nil
}

// HashNamespace returns a namespace id's routing value, so a namespace-sharded
// collection reaches RouteInsertHashes through the same hash the rewrite uses.
func HashNamespace(namespace string) uint64 {
	return uint64(typeutil.HashString2Uint32(namespace))
}

// Route returns the vchannel owning the given routing value, or an error when
// the value resolves to no shard — which means the routing meta is inconsistent,
// since a valid table tiles the key space.
func (t *Table) Route(value uint64) (string, error) {
	if t == nil {
		return "", errNoTable()
	}
	vchannel, ok := t.residues.LookupOK(value)
	if !ok {
		return "", merr.WrapErrServiceInternalMsg("routing value %d resolved to no shard", value)
	}
	return vchannel, nil
}

// errNoTable is what every routing entry point answers on a nil table. A nil
// table is how a caller carries "the routing meta was malformed and I refused to
// derive it", so it must reject the write rather than fall back to a placement
// rule nobody chose — and rather than panic in the proxy.
func errNoTable() error {
	return merr.WrapErrServiceInternal("no routing table")
}

// RouteInsert maps a batch of primary keys to the vchannels owning them,
// returning vchannel -> row offsets and the per-row shard index that
// InsertMsg.HashValues carries.
//
// Valid only where the primary key is what the collection routes by. A
// namespace-sharded collection routes by its namespace id, and hashing primary
// keys for it would send every row to a shard that does not own it, so this
// refuses instead; hash the routing value and call RouteInsertHashes.
func (t *Table) RouteInsert(pks *schemapb.IDs, channels []string) (map[string][]int, []uint32, error) {
	if t == nil {
		return nil, nil, errNoTable()
	}
	if !t.RoutesByPrimaryKey() {
		return nil, nil, errNotPrimaryKeyRouted(t)
	}
	hashes, err := HashPrimaryKeys(pks)
	if err != nil {
		return nil, nil, err
	}
	return t.RouteInsertHashes(hashes, channels)
}

// RouteInsertHashes maps a batch of routing values to the vchannels owning them,
// returning vchannel -> row offsets and the per-row shard index.
//
// The shard index is the position, within channels, of the vchannel that owns
// the row — the same meaning it has everywhere else in the write path, where
// InsertMsg.HashValues is read as an index into the channel list. It is derived
// from the placement rather than recomputed, so the two can never disagree.
//
// An unowned value is an error rather than a guess: a table derived by Derive
// tiles the key space, so a miss means the routing meta is inconsistent and
// placing the row anywhere would corrupt the collection quietly. So is a row
// whose owner is not in channels — the caller's view of the topology and the
// table's disagree, which is a real state mid-split, and writing to a vchannel
// the caller never resolved is not a recovery from it.
func (t *Table) RouteInsertHashes(hashes []uint64, channels []string) (map[string][]int, []uint32, error) {
	if t == nil {
		return nil, nil, errNoTable()
	}
	index, err := channelIndex(channels)
	if err != nil {
		return nil, nil, err
	}

	var hashValues []uint32
	if len(hashes) > 0 {
		hashValues = make([]uint32, 0, len(hashes))
	}
	// Accumulate by POSITION, not by name. The owning vchannel's position is
	// already resolved for hashValues, so indexing a slice keeps the per-row
	// cost at the one map lookup owner() needs -- building the map inline costs
	// three more on every row.
	avgCapacity := (len(hashes) / len(channels)) + 1
	byPosition := make([][]int, len(channels))
	for i, hash := range hashes {
		_, position, err := t.owner(hash, index)
		if err != nil {
			return nil, nil, err
		}
		if byPosition[position] == nil {
			byPosition[position] = make([]int, 0, avgCapacity)
		}
		byPosition[position] = append(byPosition[position], i)
		hashValues = append(hashValues, position)
	}

	offsets := make(map[string][]int, len(channels))
	for position, rows := range byPosition {
		if len(rows) > 0 {
			offsets[channels[position]] = rows
		}
	}
	return offsets, hashValues, nil
}

// RouteDelete maps a batch of primary keys to the index, within channels, of the
// vchannel owning each one — the form the delete repacker consumes.
//
// Same rule and same restriction as RouteInsert: a delete that went to the wrong
// shard would not delete anything, and one that went to a fenced split source
// would be rejected outright.
func (t *Table) RouteDelete(pks *schemapb.IDs, channels []string) ([]uint32, error) {
	if t == nil {
		return nil, errNoTable()
	}
	if !t.RoutesByPrimaryKey() {
		return nil, errNotPrimaryKeyRouted(t)
	}
	hashes, err := HashPrimaryKeys(pks)
	if err != nil {
		return nil, err
	}
	return t.RouteDeleteHashes(hashes, channels)
}

// RouteDeleteHashes maps a batch of routing values to the index, within channels,
// of the vchannel owning each one.
func (t *Table) RouteDeleteHashes(hashes []uint64, channels []string) ([]uint32, error) {
	if t == nil {
		return nil, errNoTable()
	}
	index, err := channelIndex(channels)
	if err != nil {
		return nil, err
	}
	var out []uint32
	if len(hashes) > 0 {
		out = make([]uint32, 0, len(hashes))
	}
	for _, hash := range hashes {
		_, position, err := t.owner(hash, index)
		if err != nil {
			return nil, err
		}
		out = append(out, position)
	}
	return out, nil
}

// owner resolves one routing value to its vchannel and that vchannel's position
// in the caller's channel set.
func (t *Table) owner(hash uint64, index map[string]uint32) (string, uint32, error) {
	vchannel, ok := t.residues.LookupOK(hash)
	if !ok {
		return "", 0, merr.WrapErrServiceInternalMsg("routing value %d resolved to no shard", hash)
	}
	position, ok := index[vchannel]
	if !ok {
		// Not malformed meta: the table and the request's channel set came from
		// two different DescribeCollection snapshots, which is a real state
		// while a split commits its routing. It must stay RETRIABLE -- every
		// retry consumer keys on that bit, and a non-retriable answer here turns
		// a refresh-and-retry into a hard write failure for the client.
		return "", 0, merr.WrapErrCollectionRoutingStale(vchannel,
			"shard owns the key but is not in the request's channel set")
	}
	return vchannel, position, nil
}

// channelIndex maps each channel to its position, rejecting an empty set: the
// shard index the write path carries is a position in it, and there is none.
func channelIndex(channels []string) (map[string]uint32, error) {
	if len(channels) == 0 {
		return nil, merr.WrapErrServiceInternal("routing request carries no channels")
	}
	index := make(map[string]uint32, len(channels))
	for i, channel := range channels {
		if _, ok := index[channel]; ok {
			return nil, merr.WrapErrServiceInternalMsg("channel %q is listed twice in the request's channel set", channel)
		}
		index[channel] = uint32(i)
	}
	return index, nil
}

func errNotPrimaryKeyRouted(t *Table) error {
	return merr.WrapErrServiceInternalMsg(
		"collection routes by %q, not by its primary key %q; hash that value and use RouteInsertHashes or RouteDeleteHashes",
		t.routingField, t.pkField)
}
