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

package dml

import (
	"context"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A collection that has been split places a row by the residue of its primary
// key modulo the routing modulus of the collection, and the shard that owns the
// residue owns the row (design doc 20261109-shard_split_on_queryview.md, 8.1).
// A collection that was never split keeps the legacy hash % shardNum placement:
// the table of such a collection is exactly that placement, so the two agree
// bit for bit, and the legacy code path is kept verbatim rather than re-derived.
//
// The source of a split is not a shard of the routing any more once RootCoord
// has applied the split: the vchannel list the proxy caches is the list of the
// shards that take DML. Whether the source is still served is a matter of the
// read path, which does not take its shards from here.
//
// A namespace collection places rows by its namespace, not by primary key: a
// namespace maps to a position in the channel list, which is not a residue
// table. Such a collection must not be split; if one is, its writes by
// namespace are refused rather than placed by position (see
// errNamespacePlacementOnSplitCollection).

// writeRoute is the placement one write attempt routes its rows with.
type writeRoute struct {
	// table is the residue table of a split collection, nil for a collection
	// that was never split.
	table *routing.Table
	// vchannels is the channel list the row indexes of the attempt refer to. For
	// a split collection it is the list the table was derived from, read in the
	// same cache lookup, so the two never describe different topologies.
	vchannels []string
	// sources lists the split sources of the collection that may still hold
	// an idempotency window for a keyed insert (see
	// split_fence_idempotency.go), from metacache.SplitRouting.Sources: the
	// shard infos RootCoord reports in state ShardSplitting. They never take
	// part in the routing. Until RootCoord reports them a write learns a
	// source only from being refused by it.
	sources []metacache.SplitSource
	// info is the cache entry the route was read from. A write that is
	// refused evicts the entry only while it is still this one (see
	// splitFence.refresh).
	info *collectionInfo
}

// split reports whether the route places rows by a residue table.
func (r *writeRoute) split() bool {
	return r != nil && r.table != nil
}

// lists reports whether the route lists a vchannel as a shard that takes DML.
func (r *writeRoute) lists(vchannel string) bool {
	return slices.Contains(r.vchannels, vchannel)
}

// legacyWriteRoute is the route of a collection that was never split.
func legacyWriteRoute(vchannels []string) *writeRoute {
	return &writeRoute{vchannels: vchannels}
}

// splitRoutingOf returns the split routing of a collection, or nil for one that
// was never split. A nil entry is tolerated: GetCollectionInfo can hand back
// nil with no error on an uncached describe path and in test doubles.
//
// A collection that reports a routing modulus has been split. Its shards own
// explicit residues, so the legacy modulo would place rows on shards that do
// not own them. When its routing metadata cannot be derived the write is
// refused instead of routed by position. That refusal stays ServiceInternal:
// the routing metadata comes from RootCoord, not from the request, and a
// contract violation in another component's output is a Milvus bug
// (docs/dev/error_handling_guide.md, "Input vs System"). No other code fits:
// retrying cannot change the metadata, and nothing is unsupported about the
// request.
func splitRoutingOf(info *collectionInfo) (*metacache.SplitRouting, error) {
	if info == nil || info.SplitRouting == nil {
		return nil, nil
	}
	if info.SplitRouting.Table == nil {
		cause := info.SplitRouting.Err
		if cause == nil {
			cause = merr.WrapErrServiceInternalMsg("no routing table was derived")
		}
		return nil, merr.Wrapf(cause,
			"collection %d has been split (routing modulus %d) but its routing metadata cannot be derived; refusing to place its writes by position",
			info.CollID, info.RoutingModulus)
	}
	return info.SplitRouting, nil
}

// resolveWriteRoute reads the route of one write attempt. The verdict -- split
// or never split -- and the channel list come from one cache lookup: a split
// that is applied between two lookups would otherwise run the legacy modulo
// over the list the split has changed.
//
// The lookup is the one the channel manager of the proxy does, by collection id
// alone, so a collection that was never split gets the channel list it always
// got, from the same cache entry, whatever name or alias the request used.
func resolveWriteRoute(ctx context.Context, cache Cache, collectionID int64) (*writeRoute, error) {
	info, err := cache.GetCollectionInfo(ctx, "", "", collectionID)
	if err != nil {
		return nil, err
	}
	if info == nil || len(info.VChannels) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("collection %d has no vchannel in its cached meta", collectionID)
	}
	split, err := splitRoutingOf(info)
	if err != nil {
		return nil, err
	}
	if split == nil {
		route := legacyWriteRoute(info.VChannels)
		route.info = info
		return route, nil
	}
	return &writeRoute{table: split.Table, vchannels: info.VChannels, sources: split.Sources, info: info}, nil
}

// sourcesToProbe returns, sorted, the vchannels of the split sources a keyed
// insert must ask before it places a row on the shards that replaced them,
// and for each one the pending row its probe carries.
//
// A source whose shard info names the residues it owned can only hold a key
// of those residues, so it is asked only when a pending row's key has one of
// them; a source that names none is always asked. The probe of a narrowed
// source carries a row of its own residues, so that even a source that took
// the probe (a Milvus bug, see probeFencedWindows) received a row it owned.
// pks are the primary keys of the insert, by row offset.
func (r *writeRoute) sourcesToProbe(pks *schemapb.IDs, pending *pendingRows) (map[string]int, error) {
	probes := make(map[string]int, len(r.sources))
	if len(r.sources) == 0 || pending.done() {
		return probes, nil
	}
	narrowed := false
	for _, source := range r.sources {
		if len(source.Buckets) == 0 {
			probes[source.VChannel] = pending.first()
			continue
		}
		narrowed = true
	}
	if !narrowed {
		return probes, nil
	}
	if !r.split() {
		return nil, merr.WrapErrServiceInternalMsg("a collection with split sources has no routing table")
	}
	residues, err := routing.PKResidues(pks, r.table.Modulus())
	if err != nil {
		return nil, err
	}
	for _, offset := range pending.sortedOffsets() {
		if offset >= len(residues) {
			return nil, merr.WrapErrServiceInternalMsg("pending row %d has no primary key among %d", offset, len(residues))
		}
		for _, source := range r.sources {
			if len(source.Buckets) == 0 {
				continue
			}
			if _, ok := probes[source.VChannel]; !ok && source.MayHold(residues[offset]) {
				probes[source.VChannel] = offset
			}
		}
	}
	return probes, nil
}

// pkChannelIndexes returns, for every primary key, the index into channelNames
// of the vchannel that owns it.
//
// A nil table is a collection that was never split, and keeps
// typeutil.HashPK2Channels verbatim, including its errors on an empty channel
// set and an unsupported id type. A split collection resolves each key through
// its table. The table and channelNames come from one describe of the
// collection, so a vchannel the table names and the list lacks is a Milvus bug,
// not a race.
func pkChannelIndexes(table *routing.Table, pks *schemapb.IDs, channelNames []string) ([]uint32, error) {
	if table == nil {
		return typeutil.HashPK2Channels(pks, channelNames)
	}
	if len(channelNames) == 0 {
		return nil, common.ErrRoutingTableNoValues
	}
	owners, err := table.VChannelsOfPKs(pks)
	if err != nil {
		return nil, err
	}
	index := make(map[string]uint32, len(channelNames))
	for i, channel := range channelNames {
		index[channel] = uint32(i)
	}
	indexes := make([]uint32, len(owners))
	for i, vchannel := range owners {
		channelIndex, ok := index[vchannel]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg(
				"a key is owned by vchannel %q at routing modulus %d, which the collection's channel list does not carry",
				vchannel, table.Modulus())
		}
		indexes[i] = channelIndex
	}
	return indexes, nil
}

// errNamespacePlacementOnSplitCollection is the refusal of a write that places
// its rows by namespace on a collection that has been split. The planner of a
// split must not pick such a collection, but the request itself is valid: the
// collection is in a state that does not support writes by namespace. So it is
// ErrOperationNotSupported -- a System error, not retriable, the code the
// import of a split collection is refused with too -- not ServiceInternal,
// which is not meant to leave Milvus.
func errNamespacePlacementOnSplitCollection() error {
	return merr.WrapErrOperationNotSupportedMsg(
		"the collection places rows by namespace and has been split; a namespace has no owner in a residue table")
}
