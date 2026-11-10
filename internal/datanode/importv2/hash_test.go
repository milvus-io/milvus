package importv2

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestGetRowsStatsMissingPrimaryKey(t *testing.T) {
	task := NewPreImportTask(&datapb.PreImportRequest{
		Schema: &schemapb.CollectionSchema{}, PartitionIDs: []int64{1}, Vchannels: []string{"v1"},
	}, nil, nil, taskcommon.Resource{})
	defer task.Cancel()

	stats, err := GetRowsStats(task, &storage.InsertData{})
	require.Error(t, err)
	require.Nil(t, stats)
}

func TestSnapshotImportPartitionKeyRoutingBothPhases(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		{FieldID: 101, Name: "part", DataType: schemapb.DataType_Int64, IsPartitionKey: true},
	}}
	options := []*commonpb.KeyValuePair{{Key: "backup", Value: "true"}, {Key: "source_type", Value: "snapshot"}}
	partitions, channels := []int64{20, 10}, []string{"v1", "v2"}
	pre := NewPreImportTask(&datapb.PreImportRequest{
		Schema: proto.Clone(schema).(*schemapb.CollectionSchema), Options: options, PartitionIDs: partitions, Vchannels: channels,
	}, nil, nil, taskcommon.Resource{}).(*PreImportTask)
	defer pre.cancel()
	imp := NewImportTask(&datapb.ImportRequest{
		Schema: proto.Clone(schema).(*schemapb.CollectionSchema), Options: options, PartitionIDs: partitions, Vchannels: channels,
	}, nil, nil, nil, taskcommon.Resource{}).(*ImportTask)
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
					}, nil, nil, taskcommon.Resource{}).(*PreImportTask)
					defer pre.cancel()
					imp := NewImportTask(&datapb.ImportRequest{
						Schema: proto.Clone(schema).(*schemapb.CollectionSchema), Options: options, PartitionIDs: partitions, Vchannels: channels,
					}, nil, nil, nil, taskcommon.Resource{}).(*ImportTask)
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
	}, nil, nil, taskcommon.Resource{}).(*PreImportTask)
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

func TestGatherHashRowsEmptySelections(t *testing.T) {
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

	got, err := gatherHashRows(schema, &storage.InsertData{}, [][][]int{{nil, nil}, {nil, nil}})
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

// TestHashDataBySchemaSparseBatchDoesNotOverAllocate verifies that bucket
// capacity follows routed rows rather than reserving a row in every bucket.
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

	// Dense: each bucket reserves exactly its routed row count, including
	// zero capacity for empty buckets when the distribution is uneven.
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
			assert.Equal(t, vec.RowNum()*32, cap(vec.Data))
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

func TestHashDataBySchemaRoutingAndOrder(t *testing.T) {
	keys := []struct {
		dataType schemapb.DataType
		values   []any
	}{
		{schemapb.DataType_Int64, []any{int64(0), int64(-1), int64(42), int64(math.MinInt64), int64(math.MaxInt64)}},
		{schemapb.DataType_VarChar, []any{"", "tenant-a", "租户", "tenant-b", "tenant-a"}},
	}
	for _, pk := range keys {
		// None represents a collection without a partition key.
		for _, partType := range []schemapb.DataType{schemapb.DataType_None, schemapb.DataType_Int64, schemapb.DataType_VarChar} {
			for _, channelCount := range []int{1, 3} {
				for _, partitionCount := range []int{1, 4} {
					if partType == schemapb.DataType_None && partitionCount != 1 {
						continue
					}
					t.Run(fmt.Sprintf("pk=%s/part=%s/channels=%d/partitions=%d", pk.dataType, partType, channelCount, partitionCount), func(t *testing.T) {
						schema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
							{FieldID: 100, Name: "pk", DataType: pk.dataType, IsPrimaryKey: true},
						}})
						if partType != schemapb.DataType_None {
							schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 101, Name: "part", DataType: partType, IsPartitionKey: true})
						}
						channels := make([]string, channelCount)
						for i := range channels {
							channels[i] = fmt.Sprintf("channel-%d", channelCount-i)
						}
						partitions := make([]int64, partitionCount)
						for i := range partitions {
							partitions[i] = int64(100 - i*7)
						}
						var input []map[int64]any
						for i := 0; i < 31; i++ {
							row := map[int64]any{common.RowIDField: int64(1000 + i), common.TimeStampField: int64(9000 - i), 100: pk.values[i%len(pk.values)]}
							if partType != schemapb.DataType_None {
								for _, part := range keys {
									if part.dataType == partType {
										row[101] = part.values[(i/2)%len(part.values)]
									}
								}
							}
							input = append(input, row)
						}
						assertHashRows(t, schema, channels, partitions, input)
					})
				}
			}
		}
	}
}

// Exercise logical rows across nulls, including compact nullable vectors. The
// expected values come from the fixture, not GetRow on the source or HashData.
func TestHashDataBySchemaFieldValuesAndNulls(t *testing.T) {
	arrayValue := func(values ...int64) *schemapb.ScalarField {
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}}
	}
	vectorArrayValue := func(values ...float32) *schemapb.VectorField {
		return &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: values}}}
	}
	cases := []struct {
		dataType schemapb.DataType
		dim      string
		element  schemapb.DataType
		values   []any
	}{
		{dataType: schemapb.DataType_Bool, values: []any{false, true, false}},
		{dataType: schemapb.DataType_Int8, values: []any{int8(0), int8(-7), int8(100)}},
		{dataType: schemapb.DataType_Int16, values: []any{int16(0), int16(-300), int16(400)}},
		{dataType: schemapb.DataType_Int32, values: []any{int32(0), int32(-70000), int32(90000)}},
		{dataType: schemapb.DataType_Int64, values: []any{int64(0), int64(-1), int64(math.MaxInt64)}},
		{dataType: schemapb.DataType_Timestamptz, values: []any{int64(0), int64(-1234), int64(5678)}},
		{dataType: schemapb.DataType_Float, values: []any{float32(0), float32(-1.25), float32(3.5)}},
		{dataType: schemapb.DataType_Double, values: []any{float64(0), float64(-1.25), float64(3.5)}},
		{dataType: schemapb.DataType_String, values: []any{"", "hello", "中文"}},
		{dataType: schemapb.DataType_VarChar, values: []any{"", "hello", "中文"}},
		{dataType: schemapb.DataType_Text, values: []any{"", "hello", "中文"}},
		{dataType: schemapb.DataType_JSON, values: []any{[]byte("{}"), []byte(`{"n":1}`), []byte(`{"s":"中文"}`)}},
		{dataType: schemapb.DataType_Geometry, values: []any{[]byte{1, 2}, []byte{3, 4}, []byte{5, 6}}},
		{dataType: schemapb.DataType_Array, element: schemapb.DataType_Int64, values: []any{arrayValue(), arrayValue(1), arrayValue(-2, 3)}},
		{dataType: schemapb.DataType_FloatVector, dim: "2", values: []any{[]float32{0, 0}, []float32{1.5, -2}, []float32{3, 4}}},
		{dataType: schemapb.DataType_BinaryVector, dim: "16", values: []any{[]byte{0, 0}, []byte{1, 2}, []byte{3, 4}}},
		{dataType: schemapb.DataType_Float16Vector, dim: "2", values: []any{[]byte{0, 0, 0, 0}, []byte{0, 60, 0, 64}, []byte{0, 66, 0, 68}}},
		{dataType: schemapb.DataType_BFloat16Vector, dim: "2", values: []any{[]byte{0, 0, 0, 0}, []byte{128, 63, 0, 64}, []byte{64, 64, 128, 64}}},
		{dataType: schemapb.DataType_Int8Vector, dim: "2", values: []any{[]int8{0, 0}, []int8{-1, 2}, []int8{3, -4}}},
		{dataType: schemapb.DataType_SparseFloatVector, values: []any{
			typeutil.CreateSparseFloatRow([]uint32{1}, []float32{1}),
			typeutil.CreateSparseFloatRow([]uint32{2, 9}, []float32{2, 3}),
			typeutil.CreateSparseFloatRow([]uint32{20}, []float32{4}),
		}},
		{dataType: schemapb.DataType_ArrayOfVector, dim: "2", element: schemapb.DataType_FloatVector, values: []any{
			vectorArrayValue(), vectorArrayValue(1, 2), vectorArrayValue(3, 4, 5, 6),
		}},
	}
	for _, tc := range cases {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/nullable=%t", tc.dataType, nullable), func(t *testing.T) {
				field := &schemapb.FieldSchema{FieldID: 102, Name: "payload", DataType: tc.dataType, ElementType: tc.element, Nullable: nullable}
				if tc.dim != "" {
					field.TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: tc.dim}}
				}
				if tc.dataType == schemapb.DataType_JSON {
					field.Name, field.IsDynamic = common.MetaFieldName, true
				}
				schema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{EnableDynamicField: field.IsDynamic, Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "part", DataType: schemapb.DataType_Int64, IsPartitionKey: true},
					field,
				}})
				var input []map[int64]any
				for i := 0; i < 15; i++ {
					value := tc.values[(i/2)%len(tc.values)]
					if nullable && i%3 == 0 {
						value = nil
					}
					// Vary keys independently of the validity pattern to exercise
					// logical-to-physical offsets after gathering into buckets.
					input = append(input, map[int64]any{common.RowIDField: int64(i), common.TimeStampField: int64(100 + i), 100: int64(i % 2), 101: int64(i % 5), 102: value})
				}
				assertHashRows(t, schema, []string{"v2", "v1", "v0"}, []int64{20, 10}, input)
			})
		}
	}
}

// Use the public hashing primitives as the oracle, independently of the
// production routing closures and row-copy implementation.
func assertHashRows(t *testing.T, schema *schemapb.CollectionSchema, channels []string, partitions []int64, input []map[int64]any) {
	t.Helper()
	source, err := storage.NewInsertDataWithFunctionOutputField(schema)
	require.NoError(t, err)
	want := make([][][]map[int64]any, len(channels))
	for c := range want {
		want[c] = make([][]map[int64]any, len(partitions))
	}
	for _, row := range input {
		require.NoError(t, source.Append(row))
		c, p := expectedHashBucket(t, row[100], len(channels)), 0
		if len(partitions) > 1 {
			p = expectedHashBucket(t, row[101], len(partitions))
		}
		want[c][p] = append(want[c][p], row)
	}
	got, err := HashDataBySchema(schema, channels, partitions, source)
	require.NoError(t, err)
	require.Len(t, got, len(channels))
	for c := range want {
		require.Len(t, got[c], len(partitions))
		for p, rows := range want[c] {
			bucket := got[c][p]
			require.NotNil(t, bucket)
			require.Equal(t, len(rows), bucket.GetRowNum(), "bucket [%d][%d]", c, p)
			require.Len(t, bucket.Data, len(source.Data))
			for id, column := range bucket.Data {
				require.IsType(t, source.Data[id], column)
				require.Equal(t, source.Data[id].GetDataType(), column.GetDataType())
				require.Equal(t, source.Data[id].GetNullable(), column.GetNullable())
				require.Equal(t, len(rows), column.RowNum(), "field %d in bucket [%d][%d]", id, c, p)
				var validity []bool
				for i, row := range rows {
					require.Equal(t, row[id], column.GetRow(i), "field %d, row %d in bucket [%d][%d]", id, i, c, p)
					validity = append(validity, row[id] != nil)
				}
				if column.GetNullable() {
					if len(rows) == 0 {
						require.Empty(t, column.GetValidData())
					} else {
						require.Equal(t, validity, column.GetValidData())
					}
				}
				if sparse, ok := column.(*storage.SparseFloatVectorFieldData); ok {
					var dim int64
					for _, row := range rows {
						if value, ok := row[id].([]byte); ok {
							dim = max(dim, typeutil.SparseFloatRowDim(value))
						}
					}
					require.Equal(t, dim, sparse.Dim, "field %d dimension in bucket [%d][%d]", id, c, p)
				}
			}
		}
	}
	for i, row := range input {
		require.Equal(t, row, source.GetRow(i), "source row %d changed during hashing", i)
	}
}

func TestHashDataBySchemaAllNullColumn(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{
			FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, Nullable: true,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
		},
	}}
	assertHashRows(t, schema, []string{"v1", "v0"}, []int64{10}, []map[int64]any{
		{100: int64(1), 102: nil}, {100: int64(2), 102: nil}, {100: int64(3), 102: nil},
	})
}

func expectedHashBucket(t *testing.T, key any, count int) int {
	t.Helper()
	var hash uint32
	switch key := key.(type) {
	case int64:
		var err error
		hash, err = typeutil.Hash32Int64(key)
		require.NoError(t, err)
	case string:
		hash = typeutil.HashString2Uint32(key)
	default:
		t.Fatalf("unsupported fixture key %T", key)
	}
	return int(uint64(hash) % uint64(count))
}

func TestHashDataBySchemaEmptyAndInvalidInput(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
	t.Run("empty_batch", func(t *testing.T) {
		assertHashRows(t, schema, []string{"v1", "v0"}, []int64{20}, nil)
	})
	rows, err := storage.NewInsertData(schema)
	require.NoError(t, err)
	for _, tc := range []struct {
		name       string
		schema     *schemapb.CollectionSchema
		rows       *storage.InsertData
		channels   []string
		partitions []int64
	}{
		{"nil_schema", nil, rows, []string{"v0"}, []int64{10}},
		{"nil_data", schema, nil, []string{"v0"}, []int64{10}},
		{"no_channels", schema, rows, nil, []int64{10}},
		{"no_partitions", schema, rows, []string{"v0"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := HashDataBySchema(tc.schema, tc.channels, tc.partitions, tc.rows)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Nil(t, got)
		})
	}
	t.Run("no_primary_key", func(t *testing.T) {
		_, wantErr := typeutil.GetPrimaryFieldSchema(&schemapb.CollectionSchema{})
		require.Error(t, wantErr)
		got, err := HashDataBySchema(&schemapb.CollectionSchema{}, []string{"v0"}, []int64{10}, rows)
		require.Error(t, err)
		require.Equal(t, merr.Code(wantErr), merr.Code(err))
		require.Nil(t, got)
	})
}

func TestHashDataBySchemaStructSubfieldsAndFunctionOutput(t *testing.T) {
	schema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 102, Name: "function_output", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 110, Name: "items", Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 111, Name: "items.price", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, Nullable: true},
				{
					FieldID: 112, Name: "items.vector", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector, Nullable: true,
					TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
				},
			},
		}},
	})
	var input []map[int64]any
	for i := 0; i < 12; i++ {
		row := map[int64]any{
			common.RowIDField: int64(i), common.TimeStampField: int64(100 + i), 100: int64(i % 3),
			102: typeutil.CreateSparseFloatRow([]uint32{uint32(i + 1)}, []float32{float32(i + 1)}),
			111: nil, 112: nil,
		}
		if i%3 != 0 {
			row[111] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{int64(i), int64(i + 1)}}}}
			row[112] = &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{float32(i), 1, float32(i + 1), 2}}}}
		}
		input = append(input, row)
	}
	assertHashRows(t, schema, []string{"v2", "v1", "v0"}, []int64{10}, input)
}

func TestHashDataBySchemaOwnsDenseVectorBuffers(t *testing.T) {
	schema := hashSchemaWithVector()
	source, err := storage.NewInsertData(schema)
	require.NoError(t, err)
	vector := make([]float32, 32)
	vector[0], vector[31] = 1, 2
	require.NoError(t, source.Append(map[int64]any{100: int64(7), 101: int64(8), 102: vector}))
	got, err := HashDataBySchema(schema, []string{"v0"}, []int64{10}, source)
	require.NoError(t, err)
	bucket := got[0][0]
	bucket.Data[100].(*storage.Int64FieldData).Data[0] = 70
	bucket.Data[102].(*storage.FloatVectorFieldData).Data[0] = 99
	require.Equal(t, int64(7), source.Data[100].GetRow(0))
	require.Equal(t, float32(1), source.Data[102].(*storage.FloatVectorFieldData).Data[0])
	source.Data[102].(*storage.FloatVectorFieldData).Data[31] = 88
	require.Equal(t, float32(2), bucket.Data[102].(*storage.FloatVectorFieldData).Data[31])
}

func TestHashDataBySchemaGatherErrors(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 102, Name: "value", DataType: schemapb.DataType_Int64},
	}}
	t.Run("unknown_source_field", func(t *testing.T) {
		rows := &storage.InsertData{Data: map[int64]storage.FieldData{
			100: &storage.Int64FieldData{Data: []int64{1}},
			999: &storage.Int64FieldData{Data: []int64{2}},
		}}
		got, err := HashDataBySchema(schema, []string{"v0"}, []int64{10}, rows)
		require.Nil(t, got)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	})
	t.Run("incompatible_source_column", func(t *testing.T) {
		rows := &storage.InsertData{Data: map[int64]storage.FieldData{
			100: &storage.Int64FieldData{Data: []int64{1}},
			102: &storage.StringFieldData{Data: []string{"wrong type"}},
		}}
		got, err := HashDataBySchema(schema, []string{"v0"}, []int64{10}, rows)
		require.Nil(t, got)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}

func BenchmarkHashDataBySchema(b *testing.B) {
	for _, tc := range []struct {
		name     string
		dim      int
		scalars  int
		nullable bool
		skewed   bool
	}{
		{name: "wide_fields", dim: 128, scalars: 32},
		{name: "high_dim_skewed", dim: 768, scalars: 8, skewed: true},
		{name: "nullable_vector", dim: 768, scalars: 8, nullable: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			const rowCount = 4096
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "part", DataType: schemapb.DataType_Int64, IsPartitionKey: true},
				{
					FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, Nullable: tc.nullable,
					TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(tc.dim)}},
				},
			}}
			for i := 0; i < tc.scalars; i++ {
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
					FieldID: int64(103 + i), Name: fmt.Sprintf("scalar%d", i), DataType: schemapb.DataType_Int64,
				})
			}
			schema = typeutil.AppendSystemFields(schema)
			rows, err := storage.NewInsertData(schema)
			require.NoError(b, err)
			vector := make([]float32, tc.dim)
			for i := range vector {
				vector[i] = float32(i+1) / float32(tc.dim)
			}
			for i := 0; i < rowCount; i++ {
				partitionKey := int64(i*97 + 17)
				if tc.skewed {
					partitionKey = 7
				}
				row := map[int64]any{
					common.RowIDField: int64(i), common.TimeStampField: int64(1000 + i),
					100: int64(i), 101: partitionKey, 102: vector,
				}
				if tc.nullable && (i*73+19)%100 < 50 {
					row[102] = nil
				}
				for j := 0; j < tc.scalars; j++ {
					row[int64(103+j)] = int64(i + j)
				}
				require.NoError(b, rows.Append(row))
			}
			channels := []string{"v0", "v1", "v2", "v3"}
			partitions := make([]int64, 16)
			for i := range partitions {
				partitions[i] = int64(i + 1)
			}
			b.ReportAllocs()
			b.SetBytes(int64(rows.GetMemorySize()))
			var result HashedData
			measureMemory := os.Getenv("MILVUS_HASH_BENCH_MEMORY") == "1"
			var before runtime.MemStats
			if measureMemory {
				runtime.GC()
				runtime.ReadMemStats(&before)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err = HashDataBySchema(schema, channels, partitions, rows)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if measureMemory {
				var after, live runtime.MemStats
				runtime.ReadMemStats(&after)
				runtime.GC()
				runtime.ReadMemStats(&live)
				b.ReportMetric(float64(before.HeapAlloc), "heap_base_B")
				b.ReportMetric(float64(after.HeapAlloc), "heap_after_B")
				b.ReportMetric(float64(live.HeapAlloc), "heap_live_B")
				b.ReportMetric(float64(before.HeapInuse), "heap_inuse_base_B")
				b.ReportMetric(float64(after.HeapInuse), "heap_inuse_after_B")
				b.ReportMetric(float64(live.HeapInuse), "heap_inuse_live_B")
				if path := os.Getenv("MILVUS_HASH_BENCH_HEAP_PROFILE"); path != "" {
					profileDir, err := os.OpenRoot(filepath.Dir(path))
					require.NoError(b, err)
					b.Cleanup(func() { require.NoError(b, profileDir.Close()) })
					profile, err := profileDir.Create(filepath.Base(path))
					require.NoError(b, err)
					err = pprof.WriteHeapProfile(profile)
					closeErr := profile.Close()
					require.NoError(b, err)
					require.NoError(b, closeErr)
				}
				runtime.KeepAlive(rows)
				runtime.KeepAlive(result)
			}
			b.ReportMetric(float64(rowCount)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
			var totalRows int
			for _, buckets := range result {
				for _, bucket := range buckets {
					totalRows += bucket.GetRowNum()
				}
			}
			require.Equal(b, rowCount, totalRows)
		})
	}
}
