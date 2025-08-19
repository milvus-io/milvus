package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func TestL0BlockSegmentSelector_SelectSegments(t *testing.T) {
	tests := []struct {
		name                   string
		lastFlushL0TimeTicks   map[utils.PartitionUniqueKey]uint64
		stats                  []segmentBrief
		l0MaxBlockRowNum       int64
		l0MaxBlockBinaryBytes  int64
		expectedSealedSegments map[int64]policy.SealPolicy
	}{
		{
			name:                   "empty stats",
			lastFlushL0TimeTicks:   map[utils.PartitionUniqueKey]uint64{},
			stats:                  []segmentBrief{},
			l0MaxBlockRowNum:       1000,
			l0MaxBlockBinaryBytes:  1024,
			expectedSealedSegments: nil,
		},
		{
			name: "no partitions reach limit",
			lastFlushL0TimeTicks: map[utils.PartitionUniqueKey]uint64{
				{CollectionID: 1, PartitionID: 1}: 100,
			},
			stats: []segmentBrief{
				{collectionID: 1, partitionID: 1, segmentID: 1, createTimeTick: 50, rowNum: 100, binarySize: 200},
			},
			l0MaxBlockRowNum:       1000,
			l0MaxBlockBinaryBytes:  1024,
			expectedSealedSegments: nil,
		},
		{
			name: "segment exceeds row limit",
			lastFlushL0TimeTicks: map[utils.PartitionUniqueKey]uint64{
				{CollectionID: 1, PartitionID: 1}: 100,
			},
			stats: []segmentBrief{
				{collectionID: 1, partitionID: 1, segmentID: 1, createTimeTick: 50, rowNum: 1500, binarySize: 200},
			},
			l0MaxBlockRowNum:      1000,
			l0MaxBlockBinaryBytes: 2048,
			expectedSealedSegments: map[int64]policy.SealPolicy{
				1: policy.PolicyL0BlockRows(1, 500),
			},
		},
		{
			name: "segment exceeds size limit",
			lastFlushL0TimeTicks: map[utils.PartitionUniqueKey]uint64{
				{CollectionID: 1, PartitionID: 1}: 100,
			},
			stats: []segmentBrief{
				{collectionID: 1, partitionID: 1, segmentID: 1, createTimeTick: 50, rowNum: 100, binarySize: 2000},
			},
			l0MaxBlockRowNum:      10000,
			l0MaxBlockBinaryBytes: 1024,
			expectedSealedSegments: map[int64]policy.SealPolicy{
				1: policy.PolicyL0BlockBytes(1, 976),
			},
		},
		{
			name: "multiple segments with different create times",
			lastFlushL0TimeTicks: map[utils.PartitionUniqueKey]uint64{
				{CollectionID: 1, PartitionID: 1}: 100,
			},
			stats: []segmentBrief{
				{collectionID: 1, partitionID: 1, segmentID: 1, createTimeTick: 80, rowNum: 800, binarySize: 200},
				{collectionID: 1, partitionID: 1, segmentID: 2, createTimeTick: 50, rowNum: 800, binarySize: 200},
			},
			l0MaxBlockRowNum:      1000,
			l0MaxBlockBinaryBytes: 2048,
			expectedSealedSegments: map[int64]policy.SealPolicy{
				2: policy.PolicyL0BlockRows(1, 600),
			},
		},
		{
			name: "all partitions ID handling",
			lastFlushL0TimeTicks: map[utils.PartitionUniqueKey]uint64{
				{CollectionID: 1, PartitionID: common.AllPartitionsID}: 100,
			},
			stats: []segmentBrief{
				{collectionID: 1, partitionID: 1, segmentID: 1, createTimeTick: 50, rowNum: 1500, binarySize: 200},
			},
			l0MaxBlockRowNum:      1000,
			l0MaxBlockBinaryBytes: 2048,
			expectedSealedSegments: map[int64]policy.SealPolicy{
				1: policy.PolicyL0BlockRows(common.AllPartitionsID, 500),
			},
		},
		{
			name: "segment created after flush time tick should be ignored",
			lastFlushL0TimeTicks: map[utils.PartitionUniqueKey]uint64{
				{CollectionID: 1, PartitionID: 1}: 100,
			},
			stats: []segmentBrief{
				{collectionID: 1, partitionID: 1, segmentID: 1, createTimeTick: 150, rowNum: 1500, binarySize: 200},
			},
			l0MaxBlockRowNum:       1000,
			l0MaxBlockBinaryBytes:  2048,
			expectedSealedSegments: nil,
		},
		{
			name: "multiple segments from different partitions, but not reach limit",
			lastFlushL0TimeTicks: map[utils.PartitionUniqueKey]uint64{
				{CollectionID: 1, PartitionID: 1}: 100,
				{CollectionID: 1, PartitionID: 2}: 200,
			},
			stats: []segmentBrief{
				{collectionID: 1, partitionID: 1, segmentID: 1, createTimeTick: 50, rowNum: 800, binarySize: 200},
				{collectionID: 1, partitionID: 2, segmentID: 2, createTimeTick: 150, rowNum: 800, binarySize: 200},
			},
			l0MaxBlockRowNum:       1000,
			l0MaxBlockBinaryBytes:  2048,
			expectedSealedSegments: map[int64]policy.SealPolicy{},
		},
		{
			name: "multiple segments from different partitions",
			lastFlushL0TimeTicks: map[utils.PartitionUniqueKey]uint64{
				{CollectionID: 1, PartitionID: 1}:  100,
				{CollectionID: 1, PartitionID: 2}:  200,
				{CollectionID: 1, PartitionID: -1}: 200,
			},
			stats: []segmentBrief{
				{collectionID: 1, partitionID: 1, segmentID: 1, createTimeTick: 50, rowNum: 500, binarySize: 200},
				{collectionID: 1, partitionID: 2, segmentID: 2, createTimeTick: 150, rowNum: 1500, binarySize: 200},
			},
			l0MaxBlockRowNum:      1000,
			l0MaxBlockBinaryBytes: 2048,
			expectedSealedSegments: map[int64]policy.SealPolicy{
				1: policy.PolicyL0BlockRows(-1, 1000),
				2: policy.PolicyL0BlockRows(-1, 500),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := &l0BlockSegmentSelector{
				lastFlushL0TimeTicks: tt.lastFlushL0TimeTicks,
				blockRowNum:          make(map[utils.PartitionUniqueKey]int64),
				blockSize:            make(map[utils.PartitionUniqueKey]int64),
				stats:                tt.stats,
			}

			result := selector.SelectSegments(tt.l0MaxBlockRowNum, tt.l0MaxBlockBinaryBytes)

			if tt.expectedSealedSegments == nil {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, len(tt.expectedSealedSegments), len(result))
				for segmentID, expectedPolicy := range tt.expectedSealedSegments {
					actualPolicy, exists := result[segmentID]
					assert.True(t, exists, "segment %d should be sealed", segmentID)
					assert.Equal(t, expectedPolicy, actualPolicy)
				}
			}
		})
	}
}

func TestL0BlockSegmentSelector_collect(t *testing.T) {
	tests := []struct {
		name                  string
		lastFlushL0TimeTicks  map[utils.PartitionUniqueKey]uint64
		stats                 []segmentBrief
		l0MaxBlockRowNum      int64
		l0MaxBlockBinaryBytes int64
		expectedBlockRowNum   map[utils.PartitionUniqueKey]int64
		expectedBlockSize     map[utils.PartitionUniqueKey]int64
	}{
		{
			name: "collect single partition",
			lastFlushL0TimeTicks: map[utils.PartitionUniqueKey]uint64{
				{CollectionID: 1, PartitionID: 1}: 100,
			},
			stats: []segmentBrief{
				{collectionID: 1, partitionID: 1, segmentID: 1, createTimeTick: 50, rowNum: 1500, binarySize: 2000},
			},
			l0MaxBlockRowNum:      1000,
			l0MaxBlockBinaryBytes: 1500,
			expectedBlockRowNum: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 500,
			},
			expectedBlockSize: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 500,
			},
		},
		{
			name: "segment created after flush time tick",
			lastFlushL0TimeTicks: map[utils.PartitionUniqueKey]uint64{
				{CollectionID: 1, PartitionID: 1}: 100,
			},
			stats: []segmentBrief{
				{collectionID: 1, partitionID: 1, segmentID: 1, createTimeTick: 150, rowNum: 1500, binarySize: 2000},
			},
			l0MaxBlockRowNum:      1000,
			l0MaxBlockBinaryBytes: 1500,
			expectedBlockRowNum:   map[utils.PartitionUniqueKey]int64{},
			expectedBlockSize:     map[utils.PartitionUniqueKey]int64{},
		},
		{
			name: "collect with all partitions",
			lastFlushL0TimeTicks: map[utils.PartitionUniqueKey]uint64{
				{CollectionID: 1, PartitionID: common.AllPartitionsID}: 100,
			},
			stats: []segmentBrief{
				{collectionID: 1, partitionID: 1, segmentID: 1, createTimeTick: 50, rowNum: 1500, binarySize: 2000},
			},
			l0MaxBlockRowNum:      1000,
			l0MaxBlockBinaryBytes: 1500,
			expectedBlockRowNum: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: common.AllPartitionsID}: 500,
			},
			expectedBlockSize: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: common.AllPartitionsID}: 500,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := &l0BlockSegmentSelector{
				lastFlushL0TimeTicks: tt.lastFlushL0TimeTicks,
				blockRowNum:          make(map[utils.PartitionUniqueKey]int64),
				blockSize:            make(map[utils.PartitionUniqueKey]int64),
				stats:                tt.stats,
			}

			selector.collect(tt.l0MaxBlockRowNum, tt.l0MaxBlockBinaryBytes)

			assert.Equal(t, tt.expectedBlockRowNum, selector.blockRowNum)
			assert.Equal(t, tt.expectedBlockSize, selector.blockSize)
		})
	}
}

func TestL0BlockSegmentSelector_shouldBeSealed(t *testing.T) {
	tests := []struct {
		name           string
		blockRowNum    map[utils.PartitionUniqueKey]int64
		blockSize      map[utils.PartitionUniqueKey]int64
		uniqueKey      utils.PartitionUniqueKey
		expectedPolicy policy.SealPolicy
	}{
		{
			name: "should be sealed by row num",
			blockRowNum: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 500,
			},
			blockSize:      map[utils.PartitionUniqueKey]int64{},
			uniqueKey:      utils.PartitionUniqueKey{CollectionID: 1, PartitionID: 1},
			expectedPolicy: policy.PolicyL0BlockRows(1, 500),
		},
		{
			name:        "should be sealed by size",
			blockRowNum: map[utils.PartitionUniqueKey]int64{},
			blockSize: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 1000,
			},
			uniqueKey:      utils.PartitionUniqueKey{CollectionID: 1, PartitionID: 1},
			expectedPolicy: policy.PolicyL0BlockBytes(1, 1000),
		},
		{
			name:           "should not be sealed",
			blockRowNum:    map[utils.PartitionUniqueKey]int64{},
			blockSize:      map[utils.PartitionUniqueKey]int64{},
			uniqueKey:      utils.PartitionUniqueKey{CollectionID: 1, PartitionID: 1},
			expectedPolicy: policy.NilPolicy(),
		},
		{
			name: "priority for row num over size",
			blockRowNum: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 500,
			},
			blockSize: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 1000,
			},
			uniqueKey:      utils.PartitionUniqueKey{CollectionID: 1, PartitionID: 1},
			expectedPolicy: policy.PolicyL0BlockRows(1, 500),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := &l0BlockSegmentSelector{
				blockRowNum: tt.blockRowNum,
				blockSize:   tt.blockSize,
			}

			result := selector.shouldBeSealed(tt.uniqueKey)
			assert.Equal(t, tt.expectedPolicy, result)
		})
	}
}

func TestL0BlockSegmentSelector_decrementPartitionBlockRowNumAndSize(t *testing.T) {
	tests := []struct {
		name                string
		initialBlockRowNum  map[utils.PartitionUniqueKey]int64
		initialBlockSize    map[utils.PartitionUniqueKey]int64
		uniqueKey           utils.PartitionUniqueKey
		rowNum              int64
		binarySize          int64
		expectedBlockRowNum map[utils.PartitionUniqueKey]int64
		expectedBlockSize   map[utils.PartitionUniqueKey]int64
	}{
		{
			name: "decrement normal case",
			initialBlockRowNum: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 1000,
			},
			initialBlockSize: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 2000,
			},
			uniqueKey:  utils.PartitionUniqueKey{CollectionID: 1, PartitionID: 1},
			rowNum:     500,
			binarySize: 800,
			expectedBlockRowNum: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 500,
			},
			expectedBlockSize: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 1200,
			},
		},
		{
			name: "decrement to zero removes entry",
			initialBlockRowNum: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 500,
			},
			initialBlockSize: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 800,
			},
			uniqueKey:           utils.PartitionUniqueKey{CollectionID: 1, PartitionID: 1},
			rowNum:              500,
			binarySize:          800,
			expectedBlockRowNum: map[utils.PartitionUniqueKey]int64{},
			expectedBlockSize:   map[utils.PartitionUniqueKey]int64{},
		},
		{
			name: "decrement to negative removes entry",
			initialBlockRowNum: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 300,
			},
			initialBlockSize: map[utils.PartitionUniqueKey]int64{
				{CollectionID: 1, PartitionID: 1}: 600,
			},
			uniqueKey:           utils.PartitionUniqueKey{CollectionID: 1, PartitionID: 1},
			rowNum:              500,
			binarySize:          800,
			expectedBlockRowNum: map[utils.PartitionUniqueKey]int64{},
			expectedBlockSize:   map[utils.PartitionUniqueKey]int64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := &l0BlockSegmentSelector{
				blockRowNum: tt.initialBlockRowNum,
				blockSize:   tt.initialBlockSize,
			}

			selector.decrementPartitionBlockRowNumAndSize(tt.uniqueKey, tt.rowNum, tt.binarySize)

			assert.Equal(t, tt.expectedBlockRowNum, selector.blockRowNum)
			assert.Equal(t, tt.expectedBlockSize, selector.blockSize)
		})
	}
}
