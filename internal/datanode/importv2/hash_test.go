package importv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
)

func TestNewHashedData(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
		},
	}

	got, err := newHashedData(schema, 2, 2, 0)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(got))
	for i := 0; i < 2; i++ {
		assert.Equal(t, 2, len(got[i]))
		for j := 0; j < 2; j++ {
			assert.NotNil(t, got[i][j])
		}
	}
}

func TestHashData(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:        101,
				Name:           "partition_key",
				DataType:       schemapb.DataType_Int64,
				IsPartitionKey: true,
			},
		},
	}

	mockTask := NewMockTask(t)
	mockTask.On("GetSchema").Return(schema).Maybe()
	mockTask.On("GetVchannels").Return([]string{"channel1", "channel2"}).Maybe()
	mockTask.On("GetPartitionIDs").Return([]int64{1, 2}).Maybe()
	mockTask.On("Execute").Return([]*conc.Future[any]{}).Maybe()
	mockTask.On("GetJobID").Return(int64(1)).Maybe()
	mockTask.On("GetTaskID").Return(int64(1)).Maybe()
	mockTask.On("GetCollectionID").Return(int64(1)).Maybe()

	rows, err := storage.NewInsertData(schema)
	assert.NoError(t, err)

	// Add 1000 rows of test data
	for i := 0; i < 1000; i++ {
		rows.Append(map[int64]interface{}{
			100: int64(i),       // primary key
			101: int64(i%2 + 1), // partition key, alternates between 1 and 2
		})
	}

	got, err := HashData(mockTask, rows)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(got))

	// Verify data distribution
	totalRows := 0
	for i := 0; i < 2; i++ {
		assert.Equal(t, 2, len(got[i]))
		for j := 0; j < 2; j++ {
			assert.NotNil(t, got[i][j])
			totalRows += got[i][j].GetRowNum()
		}
	}
	assert.Equal(t, 1000, totalRows)
}

func hashSchemaWithVector() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:        101,
				Name:           "partition_key",
				DataType:       schemapb.DataType_Int64,
				IsPartitionKey: true,
			},
			{
				FieldID:    102,
				Name:       "vec",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "32"}},
			},
		},
	}
}

// TestHashDataBySchemaSparseBatchDoesNotOverAllocate pins the floor rowsHint:
// a batch with fewer rows than buckets must not reserve one full row per
// bucket (numBuckets x rowSize per batch regardless of batch size, an OOM
// hazard for high-dim vectors with many partitions). Sparse buckets get zero
// initial capacity and grow on demand; dense batches still preallocate the
// average.
func TestHashDataBySchemaSparseBatchDoesNotOverAllocate(t *testing.T) {
	schema := hashSchemaWithVector()
	vchannels := []string{"c1", "c2", "c3", "c4"}
	partitions := []int64{1, 2, 3, 4}

	newRow := func(i int) map[int64]interface{} {
		return map[int64]interface{}{
			100: int64(i),
			101: int64(i%4 + 1),
			102: make([]float32, 32),
		}
	}

	// Sparse: 1 row across 16 buckets. Only the single bucket receiving the row
	// may hold capacity; the other 15 must stay at cap 0 (with the old ceiling
	// division every bucket reserved a full row up front).
	rows, err := storage.NewInsertData(schema)
	assert.NoError(t, err)
	assert.NoError(t, rows.Append(newRow(0)))
	hashed, err := HashDataBySchema(schema, vchannels, partitions, rows)
	assert.NoError(t, err)
	emptyBuckets := 0
	for i := range hashed {
		for j := range hashed[i] {
			vec := hashed[i][j].Data[102].(*storage.FloatVectorFieldData)
			if cap(vec.Data) == 0 {
				emptyBuckets++
			}
		}
	}
	assert.Equal(t, 15, emptyBuckets)

	// Dense: 64 rows across 16 buckets -> every bucket preallocates the floor
	// average of 4 rows (uneven hashing may grow individual buckets further).
	rows, err = storage.NewInsertData(schema)
	assert.NoError(t, err)
	for i := 0; i < 64; i++ {
		assert.NoError(t, rows.Append(newRow(i)))
	}
	hashed, err = HashDataBySchema(schema, vchannels, partitions, rows)
	assert.NoError(t, err)
	for i := range hashed {
		for j := range hashed[i] {
			vec := hashed[i][j].Data[102].(*storage.FloatVectorFieldData)
			assert.GreaterOrEqual(t, cap(vec.Data), 4*32)
		}
	}
}

func TestHashDeleteData(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
		},
	}

	mockTask := NewMockTask(t)
	mockTask.On("GetSchema").Return(schema).Maybe()
	mockTask.On("GetVchannels").Return([]string{"channel1", "channel2"}).Maybe()
	mockTask.On("Execute").Return([]*conc.Future[any]{}).Maybe()
	mockTask.On("GetJobID").Return(int64(1)).Maybe()
	mockTask.On("GetTaskID").Return(int64(1)).Maybe()
	mockTask.On("GetCollectionID").Return(int64(1)).Maybe()

	delData := storage.NewDeleteData(nil, nil)
	delData.Append(storage.NewInt64PrimaryKey(1), 1)

	got, err := HashDeleteData(mockTask, delData)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(got))
	for i := 0; i < 2; i++ {
		assert.NotNil(t, got[i])
	}
}

func TestGetRowsStats(t *testing.T) {
	t.Run("test non-autoID", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "pk",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
				{
					FieldID:        101,
					Name:           "partition_key",
					DataType:       schemapb.DataType_Int64,
					IsPartitionKey: true,
				},
			},
		}

		mockTask := NewMockTask(t)
		mockTask.On("GetSchema").Return(schema).Maybe()
		mockTask.On("GetVchannels").Return([]string{"channel1", "channel2"}).Maybe()
		mockTask.On("GetPartitionIDs").Return([]int64{1, 2}).Maybe()
		mockTask.On("Execute").Return([]*conc.Future[any]{}).Maybe()
		mockTask.On("GetJobID").Return(int64(1)).Maybe()
		mockTask.On("GetTaskID").Return(int64(1)).Maybe()
		mockTask.On("GetCollectionID").Return(int64(1)).Maybe()

		rows, err := storage.NewInsertData(schema)
		assert.NoError(t, err)

		// Add 1000 rows of test data
		for i := 0; i < 1000; i++ {
			rows.Append(map[int64]interface{}{
				100: int64(i),       // primary key
				101: int64(i%2 + 1), // partition key, alternates between 1 and 2
			})
		}

		got, err := GetRowsStats(mockTask, rows)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(got))

		// Verify statistics
		totalRows := int64(0)
		for _, stats := range got {
			assert.NotNil(t, stats)
			assert.NotNil(t, stats.PartitionRows)
			assert.NotNil(t, stats.PartitionDataSize)

			for _, count := range stats.PartitionRows {
				totalRows += count
			}
		}
		assert.Equal(t, int64(1000), totalRows)
	})

	t.Run("test autoID", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "pk",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
					AutoID:       true,
				},
				{
					FieldID:        101,
					Name:           "partition_key",
					DataType:       schemapb.DataType_Int64,
					IsPartitionKey: true,
				},
			},
		}

		mockTask := NewMockTask(t)
		mockTask.On("GetSchema").Return(schema).Maybe()
		mockTask.On("GetVchannels").Return([]string{"channel1", "channel2"}).Maybe()
		mockTask.On("GetPartitionIDs").Return([]int64{1, 2}).Maybe()
		mockTask.On("Execute").Return([]*conc.Future[any]{}).Maybe()
		mockTask.On("GetJobID").Return(int64(1)).Maybe()
		mockTask.On("GetTaskID").Return(int64(1)).Maybe()
		mockTask.On("GetCollectionID").Return(int64(1)).Maybe()

		rows, err := storage.NewInsertData(schema)
		assert.NoError(t, err)

		// Add 1000 rows of test data
		for i := 0; i < 1000; i++ {
			rows.Append(map[int64]interface{}{
				101: int64(i%2 + 1), // partition key, alternates between 1 and 2
			})
		}

		got, err := GetRowsStats(mockTask, rows)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(got))

		// Verify statistics and data distribution
		totalRows := int64(0)
		channelRows := make([]int64, 2)

		channelIndex := 0
		for _, stats := range got {
			assert.NotNil(t, stats)
			assert.NotNil(t, stats.PartitionRows)
			assert.NotNil(t, stats.PartitionDataSize)

			channelTotal := int64(0)
			for _, count := range stats.PartitionRows {
				channelTotal += count
			}
			channelRows[channelIndex] = channelTotal
			totalRows += channelTotal
			channelIndex++
		}

		// Verify total rows
		assert.Equal(t, int64(1000), totalRows)

		// Verify data is evenly distributed across channels
		// Allow for small differences due to rounding
		expectedPerChannel := totalRows / 2
		for _, count := range channelRows {
			assert.InDelta(t, expectedPerChannel, count, 1)
		}
	})
}

func TestGetDeleteStats(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
		},
	}

	mockTask := NewMockTask(t)
	mockTask.On("GetSchema").Return(schema).Maybe()
	mockTask.On("GetVchannels").Return([]string{"channel1", "channel2"}).Maybe()
	mockTask.On("GetPartitionIDs").Return([]int64{1}).Maybe()
	mockTask.On("Execute").Return([]*conc.Future[any]{}).Maybe()
	mockTask.On("GetJobID").Return(int64(1)).Maybe()
	mockTask.On("GetTaskID").Return(int64(1)).Maybe()
	mockTask.On("GetCollectionID").Return(int64(1)).Maybe()

	delData := storage.NewDeleteData(nil, nil)
	delData.Append(storage.NewInt64PrimaryKey(1), 1)

	got, err := GetDeleteStats(mockTask, delData)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(got))
	for _, stats := range got {
		assert.NotNil(t, stats)
		assert.NotNil(t, stats.PartitionRows)
		assert.NotNil(t, stats.PartitionDataSize)
	}
}

func TestMergeHashedStats(t *testing.T) {
	src := map[string]*datapb.PartitionImportStats{
		"channel1": {
			PartitionRows: map[int64]int64{
				1: 10,
				2: 20,
			},
			PartitionDataSize: map[int64]int64{
				1: 100,
				2: 200,
			},
		},
	}

	dst := map[string]*datapb.PartitionImportStats{
		"channel1": {
			PartitionRows: map[int64]int64{
				1: 5,
				2: 15,
			},
			PartitionDataSize: map[int64]int64{
				1: 50,
				2: 150,
			},
		},
	}

	MergeHashedStats(src, dst)

	assert.Equal(t, int64(15), dst["channel1"].PartitionRows[1])
	assert.Equal(t, int64(35), dst["channel1"].PartitionRows[2])
	assert.Equal(t, int64(150), dst["channel1"].PartitionDataSize[1])
	assert.Equal(t, int64(350), dst["channel1"].PartitionDataSize[2])
}
