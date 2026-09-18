package importv2

import (
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
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

func TestSnapshotImportPartitionKeyDefaultBothPhases(t *testing.T) {
	for _, fieldType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		for _, source := range []string{"absent", "empty", "present", "no_rows"} {
			for _, partitions := range [][]int64{{20}, {20, 10}} {
				t.Run(fmt.Sprintf("%s/%s/%d_partitions", fieldType, source, len(partitions)), func(t *testing.T) {
					const pkID, partID, optionalID = int64(100), int64(101), int64(102)
					rowCount := 8
					if source == "no_rows" {
						rowCount = 0
					}
					part := &schemapb.FieldSchema{FieldID: partID, Name: "part", DataType: fieldType, IsPartitionKey: true}
					var defaultKey any = int64(42)
					keyAt := func(i int) any { return int64(i + 100) }
					part.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}}
					if fieldType == schemapb.DataType_VarChar {
						defaultKey = "fallback"
						keyAt = func(i int) any { return fmt.Sprintf("tenant-%d", i) }
						part.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "fallback"}}
					}
					schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
						{FieldID: pkID, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
						part,
						{
							FieldID: optionalID, Name: "optional", DataType: schemapb.DataType_Int64,
							DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 7}},
						},
					}}
					options := []*commonpb.KeyValuePair{{Key: "backup", Value: "true"}, {Key: "source_type", Value: "snapshot"}}
					channels := []string{"v1", "v2"}
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

					newInput := func() *storage.InsertData {
						readSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{pre.GetSchema().GetFields()[0]}}
						if source == "present" {
							readSchema.Fields = append(readSchema.Fields, part)
						}
						rows, err := storage.NewInsertData(typeutil.AppendSystemFields(readSchema))
						require.NoError(t, err)
						for i := 0; i < rowCount; i++ {
							row := map[int64]interface{}{0: int64(i), 1: int64(100), pkID: int64(i)}
							if source == "present" {
								row[partID] = keyAt(i)
							}
							require.NoError(t, rows.Append(row))
						}
						if source == "empty" {
							rows.Data[partID], err = storage.NewFieldData(fieldType, part, rowCount)
							require.NoError(t, err)
						}
						return rows
					}

					// Import re-reads its input. Do not let PreImport's mutations
					// accidentally provide the defaults for the second phase.
					preRows, importRows := newInput(), newInput()
					stats, err := GetRowsStats(pre, preRows)
					require.NoError(t, err)
					require.NotContains(t, preRows.Data, optionalID, "statistics must not materialize unrelated defaults")
					if len(partitions) == 1 && source == "absent" {
						require.NotContains(t, preRows.Data, partID, "single-partition routing must retain its existing behavior")
					}
					require.NoError(t, AppendNullableDefaultFieldsData(imp.GetSchema(), importRows, rowCount))
					hashed, err := HashData(imp, importRows)
					require.NoError(t, err)
					total := 0
					for c, channel := range channels {
						for p, partition := range partitions {
							data := hashed[c][p]
							count := data.GetRowNum()
							require.EqualValues(t, count, stats[channel].PartitionRows[partition])
							total += count
							for i := 0; i < count; i++ {
								row := data.GetRow(i)
								pk := row[pkID].(int64)
								wantKey := defaultKey
								if source == "present" {
									wantKey = keyAt(int(pk))
								}
								require.Equal(t, wantKey, row[partID])
								require.Equal(t, pk, row[int64(0)], "source PK must be preserved")
								if len(partitions) > 1 {
									require.Equal(t, wantKey, preRows.Data[partID].GetRow(int(pk)))
								}
							}
						}
					}
					require.Equal(t, rowCount, total)
				})
			}
		}
	}
}

func TestGetRowsStatsPartitionKeyDefaultError(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{
			FieldID: 101, Name: "part", DataType: schemapb.DataType_Int64, IsPartitionKey: true,
			DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}},
		},
	}}
	pre := NewPreImportTask(&datapb.PreImportRequest{
		Schema: schema, PartitionIDs: []int64{20, 10}, Vchannels: []string{"v1"},
	}, nil, nil).(*PreImportTask)
	defer pre.cancel()
	rows := &storage.InsertData{Data: map[int64]storage.FieldData{
		100: &storage.Int64FieldData{Data: []int64{1, 2}},
	}}
	wantErr := errors.New("cannot create partition key field data")
	fieldData := mockey.Mock(storage.NewFieldData).Return(nil, wantErr).Build()
	defer fieldData.UnPatch()
	stats, err := GetRowsStats(pre, rows)
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, stats)
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
