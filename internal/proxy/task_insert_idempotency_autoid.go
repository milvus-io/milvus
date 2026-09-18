package proxy

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// reassignAutoIDForStableIdempotency re-draws the auto ids of an idempotent
// insert so that the id of row offset i has residue i % M, where M is the
// routing modulus of route: the collection's modulus once it has been split,
// its shard count before. Row offset i therefore lands on the shard owning
// residue i % M, whatever ids a retry draws.
//
// Bucketing by residue, not by position in a channel list, is what keeps a
// retry stable across a shard split: a split only refines residues (M stays,
// or doubles, and i % 2M refines i % M), so a shard the split did not touch
// owns exactly the offsets it owned before, and the offsets of the split shard
// go to its targets, where the source's idempotency window answers for them
// (see split_fence_idempotency.go). For a never-split collection, M is the
// shard count and the residue of a key is its HashPK2Channels index, so this is
// the legacy offset % shardNum bucketing bit for bit.
func (it *insertTask) reassignAutoIDForStableIdempotency(primaryFieldSchema *schemapb.FieldSchema, route *writeRoute) error {
	modulus := route.modulus()
	if modulus <= 1 || len(it.insertMsg.GetRowIDs()) == 0 {
		return nil
	}
	if it.idempotencyKey == "" {
		return merr.WrapErrServiceInternalMsg("idempotency key is required to stabilize auto id shard assignment")
	}
	if it.idAllocator == nil {
		return merr.WrapErrServiceInternalMsg("id allocator is required to stabilize auto id shard assignment")
	}

	it.vChannels = route.vchannels
	clusterID := Params.CommonCfg.ClusterID.GetAsUint64()
	if err := reassignAutoIDByResidue(
		it.insertMsg.RowIDs,
		primaryFieldSchema.GetDataType(),
		modulus,
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

func replacePrimaryFieldData(it *insertTask, primaryFieldSchema *schemapb.FieldSchema, primaryFieldData *schemapb.FieldData) {
	for idx, fieldData := range it.insertMsg.GetFieldsData() {
		if fieldData.GetFieldId() == primaryFieldSchema.GetFieldID() || fieldData.GetFieldName() == primaryFieldSchema.GetName() {
			it.insertMsg.FieldsData[idx] = primaryFieldData
			return
		}
	}
	it.insertMsg.FieldsData = append(it.insertMsg.FieldsData, primaryFieldData)
}

// reassignAutoIDByResidue replaces rowIDs so that the id of offset i has
// residue i % modulus (see reassignAutoIDForStableIdempotency).
func reassignAutoIDByResidue(
	rowIDs []int64,
	primaryDataType schemapb.DataType,
	modulus uint64,
	clusterID uint64,
	allocFunc func(uint32) (int64, int64, error),
) error {
	if len(rowIDs) == 0 || modulus <= 1 {
		return nil
	}
	if allocFunc == nil {
		return merr.WrapErrServiceInternalMsg("id allocator is nil")
	}

	required := make([]int, modulus)
	for offset := range rowIDs {
		required[uint64(offset)%modulus]++
	}

	buckets := make([][]int64, modulus)
	if err := appendAutoIDCandidatesByResidue(buckets, rowIDs, primaryDataType, modulus); err != nil {
		return err
	}
	// Each round allocates ids and files them by residue, which normally fills
	// every short bucket within a couple of rounds. Bound the loop so a
	// pathological hash distribution cannot spin forever and keep burning the
	// global id space; fail loudly after a generous cap instead.
	//
	// COST (accepted by design): a candidate that falls into an already-satisfied
	// bucket is discarded, and each top-up round deliberately over-allocates
	// (missing * modulus) so that one round almost always suffices. Id
	// amplification therefore grows with the modulus and shrinks with batch size:
	// ~1.01x for 100k rows at M=4, ~1.25x for 10k at M=16, but ~21x for a 100-row
	// insert at M=64, where the over-allocation dominates. The id space is int64,
	// so the burn is negligible; the extra RTT only applies to idempotency-enabled
	// autoID collections. The alternatives do not work: deriving the shard from
	// the row offset directly would break Delete/Upsert, which route the PK by its
	// own hash, and deterministic PRNG-generated ids cannot guarantee global
	// uniqueness.
	const maxAutoIDStabilizeRounds = 256
	for round := 0; ; round++ {
		missing := missingAutoIDBucketCount(required, buckets)
		if missing == 0 {
			break
		}
		if round >= maxAutoIDStabilizeRounds {
			return merr.WrapErrServiceInternalMsg("failed to stabilize idempotent autoID assignment: still short %d candidate(s) across %d residues after %d allocation rounds", missing, modulus, maxAutoIDStabilizeRounds)
		}
		allocCount := uint64(missing) * modulus
		if allocCount > math.MaxUint32 {
			allocCount = math.MaxUint32
		}
		begin, end, err := common.AllocAutoID(allocFunc, uint32(allocCount), clusterID)
		if err != nil {
			return err
		}
		if err := appendAutoIDRangeCandidates(buckets, begin, end, primaryDataType, modulus); err != nil {
			return err
		}
	}

	cursor := make([]int, modulus)
	for offset := range rowIDs {
		bucket := uint64(offset) % modulus
		rowIDs[offset] = buckets[bucket][cursor[bucket]]
		cursor[bucket]++
	}
	return nil
}

func appendAutoIDRangeCandidates(buckets [][]int64, begin, end int64, primaryDataType schemapb.DataType, modulus uint64) error {
	rowIDs := make([]int64, 0, end-begin)
	for id := begin; id < end; id++ {
		rowIDs = append(rowIDs, id)
	}
	return appendAutoIDCandidatesByResidue(buckets, rowIDs, primaryDataType, modulus)
}

// appendAutoIDCandidatesByResidue files every candidate id under the residue
// of the primary key it becomes.
func appendAutoIDCandidatesByResidue(buckets [][]int64, rowIDs []int64, primaryDataType schemapb.DataType, modulus uint64) error {
	ids, err := autoIDCandidatesToPrimaryIDs(rowIDs, primaryDataType)
	if err != nil {
		return err
	}
	residues, err := routing.PKResidues(ids, modulus)
	if err != nil {
		return err
	}
	for i, residue := range residues {
		buckets[residue] = append(buckets[residue], rowIDs[i])
	}
	return nil
}

func missingAutoIDBucketCount(required []int, buckets [][]int64) int {
	missing := 0
	for bucket, count := range required {
		if count > len(buckets[bucket]) {
			missing += count - len(buckets[bucket])
		}
	}
	return missing
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
