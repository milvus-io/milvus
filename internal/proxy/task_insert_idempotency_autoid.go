package proxy

import (
	"math"
	"sort"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
)

func (it *insertTask) reassignAutoIDForStableIdempotency(primaryFieldSchema *schemapb.FieldSchema, channelNames []vChan) error {
	if len(channelNames) <= 1 || len(it.insertMsg.GetRowIDs()) == 0 {
		return nil
	}
	if it.idempotencyKey == "" {
		return errors.New("idempotency key is required to stabilize auto id shard assignment")
	}
	if it.idAllocator == nil {
		return errors.New("id allocator is required to stabilize auto id shard assignment")
	}

	channelNames = stableAutoIDChannelOrder(channelNames)
	it.vChannels = channelNames
	clusterID := Params.CommonCfg.ClusterID.GetAsUint64()
	if err := reassignAutoIDByOffsetChannels(
		it.insertMsg.RowIDs,
		primaryFieldSchema.GetDataType(),
		channelNames,
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

func stableAutoIDChannelOrder(channelNames []vChan) []vChan {
	// AutoID retry routing is based on row offset, so it must not inherit
	// nondeterminism from the channel manager's return order.
	ordered := append([]vChan(nil), channelNames...)
	sort.Strings(ordered)
	return ordered
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

func reassignAutoIDByOffsetChannels(
	rowIDs []int64,
	primaryDataType schemapb.DataType,
	channelNames []vChan,
	clusterID uint64,
	allocFunc func(uint32) (int64, int64, error),
) error {
	numChannels := len(channelNames)
	if len(rowIDs) == 0 || numChannels <= 1 {
		return nil
	}
	if allocFunc == nil {
		return errors.New("id allocator is nil")
	}
	channelNames = stableAutoIDChannelOrder(channelNames)

	required := make([]int, numChannels)
	for offset := range rowIDs {
		required[offset%numChannels]++
	}

	buckets := make([][]int64, numChannels)
	if err := appendAutoIDCandidatesByChannels(buckets, rowIDs, primaryDataType, channelNames); err != nil {
		return err
	}
	// Each round allocates ids and routes them to buckets by PK hash, which
	// normally fills every short bucket within a couple of rounds. Bound the loop
	// so a pathological hash distribution cannot spin forever and keep burning the
	// global id space; fail loudly after a generous cap instead.
	const maxAutoIDStabilizeRounds = 256
	for round := 0; ; round++ {
		missing := missingAutoIDBucketCount(required, buckets)
		if missing == 0 {
			break
		}
		if round >= maxAutoIDStabilizeRounds {
			return errors.Errorf("failed to stabilize idempotent autoID assignment: still short %d candidate(s) across %d channels after %d allocation rounds", missing, numChannels, maxAutoIDStabilizeRounds)
		}
		allocCount := uint64(missing) * uint64(numChannels)
		if allocCount < uint64(numChannels) {
			allocCount = uint64(numChannels)
		}
		if allocCount > math.MaxUint32 {
			allocCount = math.MaxUint32
		}
		begin, end, err := common.AllocAutoID(allocFunc, uint32(allocCount), clusterID)
		if err != nil {
			return err
		}
		if err := appendAutoIDRangeCandidates(buckets, begin, end, primaryDataType, channelNames); err != nil {
			return err
		}
	}

	cursor := make([]int, numChannels)
	for offset := range rowIDs {
		bucket := offset % numChannels
		rowIDs[offset] = buckets[bucket][cursor[bucket]]
		cursor[bucket]++
	}
	return nil
}

func appendAutoIDRangeCandidates(buckets [][]int64, begin, end int64, primaryDataType schemapb.DataType, channelNames []vChan) error {
	rowIDs := make([]int64, 0, end-begin)
	for id := begin; id < end; id++ {
		rowIDs = append(rowIDs, id)
	}
	return appendAutoIDCandidatesByChannels(buckets, rowIDs, primaryDataType, channelNames)
}

func appendAutoIDCandidatesByChannels(buckets [][]int64, rowIDs []int64, primaryDataType schemapb.DataType, channelNames []vChan) error {
	ids, err := autoIDCandidatesToPrimaryIDs(rowIDs, primaryDataType)
	if err != nil {
		return err
	}
	channel2RowOffsets, err := assignChannelsByPK(ids, channelNames, &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{},
	})
	if err != nil {
		return err
	}
	channelIndex := make(map[string]int, len(channelNames))
	for idx, channelName := range channelNames {
		channelIndex[channelName] = idx
	}
	for _, channelName := range channelNames {
		bucket := channelIndex[channelName]
		for _, offset := range channel2RowOffsets[channelName] {
			buckets[bucket] = append(buckets[bucket], rowIDs[offset])
		}
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
		return nil, errors.Errorf("unsupported auto id primary field type: %s", primaryDataType.String())
	}
}
