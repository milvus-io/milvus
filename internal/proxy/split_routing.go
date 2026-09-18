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

package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A collection that has been split places a row by the residue of its primary
// key modulo the collection's routing modulus, and the shard owning that
// residue owns the row (design doc §3.1). A collection that has never been
// split keeps the legacy hash % shardNum placement: the residue table of such a
// collection is exactly that placement, so the two agree bit for bit, and the
// legacy path is kept verbatim rather than re-derived.
//
// Namespace collections are never split (design doc §1.3), so their namespace
// placement is untouched here.

// writeRoute is the placement one write attempt routes its rows with.
type writeRoute struct {
	// table is the residue table of a split collection, nil for a collection
	// that has never been split.
	table *routing.ResidueTable
	// vchannels is the channel list the row indexes of the attempt refer to. For
	// a split collection it is the list the table was derived from, read in the
	// same cache lookup, so the two never describe different topologies.
	vchannels []string
	// writable lists the vchannels that own at least one residue, in vchannel
	// order. It is vchannels for a collection that has never been split.
	writable []string
	// fenced lists the shards a split has fenced and not yet retired.
	fenced []string
}

// split reports whether the route places rows by a residue table.
func (r *writeRoute) split() bool {
	return r != nil && r.table != nil
}

// modulus is the routing modulus rows are placed by: the table's for a split
// collection, the shard count for one that has never been split.
func (r *writeRoute) modulus() uint64 {
	if r.split() {
		return r.table.Modulus()
	}
	return uint64(len(r.vchannels))
}

// legacyWriteRoute is the route of a collection that has never been split.
func legacyWriteRoute(vchannels []string) *writeRoute {
	return &writeRoute{vchannels: vchannels, writable: vchannels}
}

// splitRoutingOf returns the split routing of a collection, or nil for one that
// has never been split. A nil entry is tolerated: GetCollectionInfo can hand
// back nil with no error on an uncached describe path and in test doubles.
//
// A collection that reports a routing modulus has been split. Its vchannel list
// has grown and its shards own explicit residues, so the legacy modulo would
// place rows on shards that do not own them, the fenced source among them. When
// its routing meta cannot be derived the write is refused instead of routed by
// position.
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
			"collection %d has been split (routing modulus %d) but its routing meta cannot be derived; refusing to place its writes by position",
			info.CollID, info.RoutingModulus)
	}
	return info.SplitRouting, nil
}

// resolveWriteRoute reads the route of one write attempt. legacyChannels
// supplies the channel list of a collection that has never been split, which
// the write paths have always read from the channel manager.
func resolveWriteRoute(
	ctx context.Context,
	cache Cache,
	dbName string,
	collectionName string,
	collectionID int64,
	legacyChannels func() ([]string, error),
) (*writeRoute, error) {
	info, err := cache.GetCollectionInfo(ctx, dbName, collectionName, collectionID)
	if err != nil {
		return nil, err
	}
	split, err := splitRoutingOf(info)
	if err != nil {
		return nil, err
	}
	if split == nil {
		channels, err := legacyChannels()
		if err != nil {
			return nil, err
		}
		return legacyWriteRoute(channels), nil
	}
	return newSplitWriteRoute(info.VChannels, split), nil
}

func newSplitWriteRoute(vchannels []string, split *metacache.SplitRouting) *writeRoute {
	fenced := typeutil.NewSet(split.Fenced...)
	writable := make([]string, 0, len(vchannels))
	for _, vchannel := range vchannels {
		if !fenced.Contain(vchannel) {
			writable = append(writable, vchannel)
		}
	}
	return &writeRoute{
		table:     split.Table,
		vchannels: vchannels,
		writable:  writable,
		fenced:    split.Fenced,
	}
}

// pkChannelIndexes returns, for every primary key, the index into channelNames
// of the vchannel owning it.
//
// A nil table is a collection that has never been split, and keeps
// typeutil.HashPK2Channels verbatim, including its errors on an empty channel
// set and an unsupported id type. A split collection resolves each key's
// residue through its table. The table and channelNames come from one describe
// of the collection, so a vchannel the table names and the list lacks is a
// Milvus bug, not a race.
func pkChannelIndexes(table *routing.ResidueTable, pks *schemapb.IDs, channelNames []string) ([]uint32, error) {
	if table == nil {
		return typeutil.HashPK2Channels(pks, channelNames)
	}
	if len(channelNames) == 0 {
		return nil, common.ErrRoutingTableNoValues
	}
	residues, err := routing.PKResidues(pks, table.Modulus())
	if err != nil {
		return nil, err
	}
	index := make(map[string]uint32, len(channelNames))
	for i, channel := range channelNames {
		index[channel] = uint32(i)
	}
	indexes := make([]uint32, len(residues))
	for i, residue := range residues {
		vchannel, _ := table.Lookup(residue)
		channelIndex, ok := index[vchannel]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg(
				"residue %d of modulus %d is owned by vchannel %q, which the collection's channel list does not carry",
				residue, table.Modulus(), vchannel)
		}
		indexes[i] = channelIndex
	}
	return indexes, nil
}
