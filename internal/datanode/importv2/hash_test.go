package importv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSnapshotImportPartitionKeyRoutingBothPhases(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		{FieldID: 101, Name: "part", DataType: schemapb.DataType_Int64, IsPartitionKey: true},
	}}
	options := []*commonpb.KeyValuePair{{Key: "backup", Value: "true"}, {Key: "source_type", Value: "snapshot"}}
	partitions, channels := []int64{20, 10}, []string{"v1", "v2"}
	pre := NewPreImportTask(&datapb.PreImportRequest{
		Schema: proto.Clone(schema).(*schemapb.CollectionSchema), Options: options, PartitionIDs: partitions, Vchannels: channels,
	}, nil, nil).(*PreImportTask)
	defer pre.cancel()
	imp := NewImportTask(&datapb.ImportRequest{
		Schema: proto.Clone(schema).(*schemapb.CollectionSchema), Options: options, PartitionIDs: partitions, Vchannels: channels,
	}, nil, nil, nil).(*ImportTask)
	defer imp.cancel()
	require.False(t, pre.GetSchema().GetFields()[0].GetAutoID())
	require.False(t, imp.GetSchema().GetFields()[0].GetAutoID())
	rows, err := storage.NewInsertData(typeutil.AppendSystemFields(pre.GetSchema()))
	require.NoError(t, err)
	for i := int64(0); i < 100; i++ {
		require.NoError(t, rows.Append(map[int64]interface{}{0: i, 1: int64(100), 100: i, 101: i / 3}))
	}
	stats, err := GetRowsStats(pre, rows)
	require.NoError(t, err)
	hashed, err := HashData(imp, rows)
	require.NoError(t, err)
	total := 0
	for c, channel := range channels {
		for p, partition := range partitions {
			count := hashed[c][p].GetRowNum()
			require.Positive(t, count)
			require.EqualValues(t, count, stats[channel].PartitionRows[partition])
			total += count
			for i := 0; i < count; i++ {
				row := hashed[c][p].GetRow(i)
				key := row[int64(101)].(int64)
				hash, err := typeutil.Hash32Int64(key)
				require.NoError(t, err)
				require.EqualValues(t, hash%uint32(len(partitions)), p)
				require.Equal(t, row[int64(0)], row[int64(100)]) // Source PK is preserved.
			}
		}
	}
	require.Equal(t, 100, total)
}

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

	got, err := newHashedData(schema, 2, 2)
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
