package stats

import (
	"sort"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// segmentBrief is the brief info of a segment.
type segmentBrief struct {
	collectionID   int64
	partitionID    int64
	segmentID      int64
	createTimeTick uint64
	rowNum         uint64
	binarySize     uint64
}

// selectSegmentsWithL0BlockPolicy selects the segments with flush time tick.
func (m *StatsManager) selectSegmentsWithL0BlockPolicy() map[int64]policy.SealPolicy {
	m.mu.Lock()
	pchannels := lo.Keys(m.sealOperators)
	m.mu.Unlock()

	results := make(map[int64]policy.SealPolicy, 0)
	for _, pchannel := range pchannels {
		sealPolicies := m.selectSegmentsWithL0BlockPolicyForPChannel(pchannel)
		for segmentID, sealPolicy := range sealPolicies {
			results[segmentID] = sealPolicy
		}
	}
	return results
}

// selectSegmentsWithL0BlockPolicyForPChannel selects the segments with flush time tick for a pchannel.
func (m *StatsManager) selectSegmentsWithL0BlockPolicyForPChannel(pchannel string) map[int64]policy.SealPolicy {
	m.mu.Lock()
	operator, ok := m.sealOperators[pchannel]
	m.mu.Unlock()
	if !ok {
		return nil
	}

	// get the last flush time tick of the pchannel.
	lastFlushL0TimeTicks := operator.GetLastL0FlushTimeTick()

	// create the slice of segmentBrief for L1 segments.
	stats := m.createL1SegmentBriefSlice()

	// create the l0BlockCollector and select the segment that should be sealed.
	l0BlockCollector := &l0BlockSegmentSelector{
		lastFlushL0TimeTicks: lastFlushL0TimeTicks,
		blockRowNum:          make(map[utils.PartitionUniqueKey]int64),
		blockSize:            make(map[utils.PartitionUniqueKey]int64),
		stats:                stats,
	}
	return l0BlockCollector.SelectSegments(m.cfg.l0MaxBlockRowNum, m.cfg.l0MaxBlockBinaryBytes)
}

// createL1SegmentBriefSlice creates a slice of segmentBrief for L1 segments.
func (m *StatsManager) createL1SegmentBriefSlice() []segmentBrief {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats := make([]segmentBrief, 0, len(m.segmentStats))
	for id, stat := range m.segmentStats {
		if stat.Level == datapb.SegmentLevel_L1 {
			stats = append(stats, segmentBrief{
				collectionID:   m.segmentIndex[id].CollectionID,
				partitionID:    m.segmentIndex[id].PartitionID,
				segmentID:      id,
				binarySize:     stat.Modified.BinarySize,
				rowNum:         stat.Modified.Rows,
				createTimeTick: stat.CreateTimeTick,
			})
		}
	}
	return stats
}

// l0BlockSegmentSelector is the selector of the l0 block segment.
type l0BlockSegmentSelector struct {
	lastFlushL0TimeTicks map[utils.PartitionUniqueKey]uint64
	blockRowNum          map[utils.PartitionUniqueKey]int64
	blockSize            map[utils.PartitionUniqueKey]int64
	stats                []segmentBrief
}

// SelectSegments selects the segments that should be sealed.
func (c *l0BlockSegmentSelector) SelectSegments(l0MaxBlockRowNum int64, l0MaxBlockBinaryBytes int64) map[int64]policy.SealPolicy {
	// collect the block row num and size of each partition.
	c.collect(l0MaxBlockRowNum, l0MaxBlockBinaryBytes)

	// if no partition reach the limit, return nil.
	if len(c.blockRowNum) == 0 && len(c.blockSize) == 0 {
		return nil
	}

	// sort the segment with create time tick.
	sort.Slice(c.stats, func(i, j int) bool {
		return c.stats[i].createTimeTick < c.stats[j].createTimeTick
	})

	result := make(map[int64]policy.SealPolicy, 0)
	// select the segment with block row num or size.
	for _, stat := range c.stats {
		uniqueKey := []utils.PartitionUniqueKey{
			{CollectionID: stat.collectionID, PartitionID: common.AllPartitionsID},
			{CollectionID: stat.collectionID, PartitionID: stat.partitionID},
		}
		for _, key := range uniqueKey {
			sealPolicy := c.shouldBeSealed(key)
			if !sealPolicy.IsNil() {
				result[stat.segmentID] = sealPolicy
				break
			}
		}
		if _, ok := result[stat.segmentID]; ok {
			for _, key := range uniqueKey {
				c.decrementPartitionBlockRowNumAndSize(key, int64(stat.rowNum), int64(stat.binarySize))
			}
		}
	}
	return result
}

// collect collects the block row num and size of the partition.
func (c *l0BlockSegmentSelector) collect(l0MaxBlockRowNum int64, l0MaxBlockBinaryBytes int64) {
	for _, stat := range c.stats {
		uniqueKey := utils.PartitionUniqueKey{CollectionID: stat.collectionID, PartitionID: stat.partitionID}
		if lastFlushL0TimeTick, ok := c.lastFlushL0TimeTicks[uniqueKey]; ok && lastFlushL0TimeTick >= stat.createTimeTick {
			c.blockRowNum[uniqueKey] += int64(stat.rowNum)
			c.blockSize[uniqueKey] += int64(stat.binarySize)
		}
		// the segment also contributes to the all partitions.
		uniqueKey = utils.PartitionUniqueKey{CollectionID: stat.collectionID, PartitionID: common.AllPartitionsID}
		if lastFlushL0TimeTick, ok := c.lastFlushL0TimeTicks[uniqueKey]; ok && lastFlushL0TimeTick >= stat.createTimeTick {
			c.blockRowNum[uniqueKey] += int64(stat.rowNum)
			c.blockSize[uniqueKey] += int64(stat.binarySize)
		}
	}

	for uniqueKey := range c.blockRowNum {
		c.blockRowNum[uniqueKey] -= l0MaxBlockRowNum
		c.blockSize[uniqueKey] -= l0MaxBlockBinaryBytes
		if c.blockRowNum[uniqueKey] <= 0 {
			delete(c.blockRowNum, uniqueKey)
		}
		if c.blockSize[uniqueKey] <= 0 {
			delete(c.blockSize, uniqueKey)
		}
	}
}

// shouldBeSealed returns the seal policy of the segment if it should be sealed.
func (c *l0BlockSegmentSelector) shouldBeSealed(uniqueKey utils.PartitionUniqueKey) policy.SealPolicy {
	if rowNum, ok := c.blockRowNum[uniqueKey]; ok && rowNum > 0 {
		return policy.PolicyL0BlockRows(uniqueKey.PartitionID, uint64(c.blockRowNum[uniqueKey]))
	}
	if blockSize, ok := c.blockSize[uniqueKey]; ok && blockSize > 0 {
		return policy.PolicyL0BlockBytes(uniqueKey.PartitionID, uint64(blockSize))
	}
	return policy.NilPolicy()
}

// decrementPartitionBlockRowNumAndSize decrements the block row num and size of the partition.
func (c *l0BlockSegmentSelector) decrementPartitionBlockRowNumAndSize(uniqueKey utils.PartitionUniqueKey, rowNum int64, binarySize int64) {
	c.blockRowNum[uniqueKey] -= rowNum
	if c.blockRowNum[uniqueKey] <= 0 {
		delete(c.blockRowNum, uniqueKey)
	}
	c.blockSize[uniqueKey] -= binarySize
	if c.blockSize[uniqueKey] <= 0 {
		delete(c.blockSize, uniqueKey)
	}
}
