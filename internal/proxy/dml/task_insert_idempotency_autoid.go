package dml

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// reassignAutoIDForStableIdempotency re-draws the auto ids of an idempotent
// insert so that row offset i always lands on the shard owning residue i % M,
// M being the routing modulus of route (the collection's modulus once it has
// been split, its shard count before), whatever ids a retry draws.
//
// Pinning offsets to residues, not to positions in a channel list, is what
// keeps a retry stable across a shard split: a split only refines residues (M
// stays, or doubles, and i % 2M refines i % M), so a shard the split did not
// touch owns exactly the offsets it owned before, and the offsets of the split
// shard go to its targets, where the source's idempotency window answers for
// them (see split_fence_idempotency.go). For a never-split collection the
// owner of residue r is the r-th shard and a key's residue is its
// HashPK2Channels index, so this is the legacy offset % shardNum bucketing bit
// for bit.
func (it *InsertTask) reassignAutoIDForStableIdempotency(primaryFieldSchema *schemapb.FieldSchema, route *writeRoute) error {
	placement := newAutoIDPlacement(route)
	if placement.owners <= 1 || len(it.insertMsg.GetRowIDs()) == 0 {
		return nil
	}
	if it.idempotencyKey == "" {
		return merr.WrapErrServiceInternalMsg("idempotency key is required to stabilize auto id shard assignment")
	}
	if it.idAllocator == nil {
		return merr.WrapErrServiceInternalMsg("id allocator is required to stabilize auto id shard assignment")
	}

	clusterID := paramtable.Get().CommonCfg.ClusterID.GetAsUint64()
	if err := reassignAutoIDByResidue(
		it.insertMsg.RowIDs,
		primaryFieldSchema.GetDataType(),
		placement,
		clusterID,
		it.idAllocator.Alloc,
	); err != nil {
		return err
	}

	primaryFieldData, err := autoGenPrimaryFieldData(primaryFieldSchema, it.insertMsg.GetRowIDs())
	if err != nil {
		return err
	}
	primaryFieldData.FieldId = primaryFieldSchema.GetFieldID()
	replacePrimaryFieldData(it, primaryFieldSchema, primaryFieldData)

	ids, err := parsePrimaryFieldData2IDs(primaryFieldData)
	if err != nil {
		return err
	}
	it.result.IDs = ids
	return nil
}

func replacePrimaryFieldData(it *InsertTask, primaryFieldSchema *schemapb.FieldSchema, primaryFieldData *schemapb.FieldData) {
	for idx, fieldData := range it.insertMsg.GetFieldsData() {
		if fieldData.GetFieldId() == primaryFieldSchema.GetFieldID() || fieldData.GetFieldName() == primaryFieldSchema.GetName() {
			it.insertMsg.FieldsData[idx] = primaryFieldData
			return
		}
	}
	it.insertMsg.FieldsData = append(it.insertMsg.FieldsData, primaryFieldData)
}

// autoIDPlacement is how auto ids are placed: residue r modulo the modulus is
// owned by shard ownerOf[r], and shard o owns share[o] residues.
type autoIDPlacement struct {
	modulus uint64
	ownerOf []int
	share   []int
	owners  int
}

// newAutoIDPlacement is the placement of route: its table's residues for a
// split collection, one residue per shard in channel order for a never-split
// one.
func newAutoIDPlacement(route *writeRoute) *autoIDPlacement {
	if !route.split() {
		return legacyAutoIDPlacement(len(route.vchannels))
	}
	modulus := route.table.Modulus()
	index := make(map[string]int)
	placement := &autoIDPlacement{modulus: modulus, ownerOf: make([]int, modulus)}
	for r := uint64(0); r < modulus; r++ {
		vchannel, _ := route.table.Lookup(r)
		owner, ok := index[vchannel]
		if !ok {
			owner = len(placement.share)
			index[vchannel] = owner
			placement.share = append(placement.share, 0)
		}
		placement.ownerOf[r] = owner
		placement.share[owner]++
	}
	placement.owners = len(placement.share)
	return placement
}

// legacyAutoIDPlacement is the placement of a never-split collection of n
// shards: modulus n, shard i owning residue i.
func legacyAutoIDPlacement(n int) *autoIDPlacement {
	placement := &autoIDPlacement{modulus: uint64(n), ownerOf: make([]int, n), share: make([]int, n), owners: n}
	for i := range placement.ownerOf {
		placement.ownerOf[i] = i
		placement.share[i] = 1
	}
	return placement
}

// reassignAutoIDByResidue replaces rowIDs so that the id of offset i routes to
// the owner of residue i % modulus (see reassignAutoIDForStableIdempotency).
// An id qualifies when its residue is any residue that owner holds, so a row
// costs about modulus/share draws -- the inverse of its owner's share of the
// key space, the shard count when shards are even -- not the modulus.
func reassignAutoIDByResidue(
	rowIDs []int64,
	primaryDataType schemapb.DataType,
	placement *autoIDPlacement,
	clusterID uint64,
	allocFunc func(uint32) (int64, int64, error),
) error {
	if len(rowIDs) == 0 || placement.owners <= 1 {
		return nil
	}
	// The ids drawn so far were never written, so they are candidates too.
	return reassignAutoIDsAtOffsets(rowIDs, allRowOffsets(len(rowIDs)), rowIDs, primaryDataType, placement, clusterID, allocFunc)
}

// reassignAutoIDsAtOffsets replaces rowIDs[offset], for every offset in
// offsets, with an id routing to the owner of residue offset % modulus.
// recycled are ids the caller gives up, placed before any new one is drawn.
func reassignAutoIDsAtOffsets(
	rowIDs []int64,
	offsets []int,
	recycled []int64,
	primaryDataType schemapb.DataType,
	placement *autoIDPlacement,
	clusterID uint64,
	allocFunc func(uint32) (int64, int64, error),
) error {
	if allocFunc == nil {
		return merr.WrapErrServiceInternalMsg("id allocator is nil")
	}

	ownerOfOffset := func(offset int) int {
		return placement.ownerOf[uint64(offset)%placement.modulus]
	}
	required := make([]int, placement.owners)
	for _, offset := range offsets {
		required[ownerOfOffset(offset)]++
	}

	buckets := make([][]int64, placement.owners)
	if err := appendAutoIDCandidatesByOwner(buckets, recycled, primaryDataType, placement); err != nil {
		return err
	}
	// Each round allocates ids and files them under the owner of their
	// residue, which normally fills every short bucket within a couple of
	// rounds. Bound the loop so a pathological hash distribution cannot spin
	// forever and keep burning the global id space; fail loudly after a
	// generous cap instead.
	//
	// COST (accepted by design): a candidate that falls to an already-satisfied
	// owner is discarded. Each top-up round sizes each short owner's draws by
	// the inverse of its share (missing * modulus / share), so that one round
	// almost always suffices: with even shards that is missing * shardNum, as
	// before any split, and only a genuinely small shard costs more, bounded by
	// one over its share. Amplification shrinks with batch size: ~1.01x for 100k
	// rows over 4 even shards, ~21x for a 100-row insert over 64. The id space
	// is int64, so the burn is negligible; the extra RTT only applies to autoID
	// inserts carrying an explicit idempotency key. The alternatives do not work:
	// deriving the shard from the row offset directly would break Delete/Upsert,
	// which route the PK by its own hash, and deterministic PRNG-generated ids
	// cannot guarantee global uniqueness.
	const maxAutoIDStabilizeRounds = 256
	for round := 0; ; round++ {
		allocCount := uint64(0)
		missing := 0
		for owner, count := range required {
			if short := count - len(buckets[owner]); short > 0 {
				missing += short
				allocCount += (uint64(short)*placement.modulus + uint64(placement.share[owner]) - 1) / uint64(placement.share[owner])
			}
		}
		if missing == 0 {
			break
		}
		if round >= maxAutoIDStabilizeRounds {
			return merr.WrapErrServiceInternalMsg("failed to stabilize idempotent autoID assignment: still short %d candidate(s) across %d shards after %d allocation rounds", missing, placement.owners, maxAutoIDStabilizeRounds)
		}
		allocCount = max(allocCount, uint64(placement.owners))
		if allocCount > math.MaxUint32 {
			allocCount = math.MaxUint32
		}
		begin, end, err := common.AllocAutoID(allocFunc, uint32(allocCount), clusterID)
		if err != nil {
			return err
		}
		if err := appendAutoIDRangeCandidates(buckets, begin, end, primaryDataType, placement); err != nil {
			return err
		}
	}

	cursor := make([]int, placement.owners)
	for _, offset := range offsets {
		owner := ownerOfOffset(offset)
		rowIDs[offset] = buckets[owner][cursor[owner]]
		cursor[owner]++
	}
	return nil
}

func appendAutoIDRangeCandidates(buckets [][]int64, begin, end int64, primaryDataType schemapb.DataType, placement *autoIDPlacement) error {
	rowIDs := make([]int64, 0, end-begin)
	for id := begin; id < end; id++ {
		rowIDs = append(rowIDs, id)
	}
	return appendAutoIDCandidatesByOwner(buckets, rowIDs, primaryDataType, placement)
}

// appendAutoIDCandidatesByOwner files every candidate id under the shard that
// owns the residue of the primary key it becomes.
func appendAutoIDCandidatesByOwner(buckets [][]int64, rowIDs []int64, primaryDataType schemapb.DataType, placement *autoIDPlacement) error {
	owners, err := autoIDCandidateOwners(rowIDs, primaryDataType, placement)
	if err != nil {
		return err
	}
	for i, owner := range owners {
		buckets[owner] = append(buckets[owner], rowIDs[i])
	}
	return nil
}

// autoIDCandidateOwners returns, for every candidate id, the shard owning the
// residue of the primary key it becomes.
func autoIDCandidateOwners(rowIDs []int64, primaryDataType schemapb.DataType, placement *autoIDPlacement) ([]int, error) {
	if len(rowIDs) == 0 {
		return nil, nil
	}
	ids, err := autoIDCandidatesToPrimaryIDs(rowIDs, primaryDataType)
	if err != nil {
		return nil, err
	}
	residues, err := routing.PKResidues(ids, placement.modulus)
	if err != nil {
		return nil, err
	}
	owners := make([]int, len(residues))
	for i, residue := range residues {
		owners[i] = placement.ownerOf[residue]
	}
	return owners, nil
}

func autoIDCandidatesToPrimaryIDs(rowIDs []int64, primaryDataType schemapb.DataType) (*schemapb.IDs, error) {
	switch primaryDataType {
	case schemapb.DataType_Int64:
		return &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: rowIDs},
			},
		}, nil
	case schemapb.DataType_VarChar:
		fieldData, err := autoGenPrimaryFieldData(&schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}, rowIDs)
		if err != nil {
			return nil, err
		}
		return parsePrimaryFieldData2IDs(fieldData)
	default:
		return nil, merr.WrapErrServiceInternalMsg("unsupported auto id primary field type: %s", primaryDataType.String())
	}
}
