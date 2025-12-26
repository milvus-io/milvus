package packed

import (
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestPackedFFIReader(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	dir := t.TempDir()
	t.Log("Case temp dir: ", dir)
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)

	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	const (
		numRows = 1000
		dim     = 128
	)

	// Create schema: int64 primary key + 128-dim float vector
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "pk",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
		{
			Name:     "vector",
			Type:     &arrow.FixedSizeBinaryType{ByteWidth: dim * 4}, // float32 = 4 bytes
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"101"}),
		},
	}, nil)

	basePath := "files/packed_reader_test/1"
	version := int64(0)

	// Build record batch
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	pkBuilder := b.Field(0).(*array.Int64Builder)
	vectorBuilder := b.Field(1).(*array.FixedSizeBinaryBuilder)

	// Store expected values for verification
	expectedPks := make([]int64, numRows)
	expectedVectors := make([][]byte, numRows)

	for i := 0; i < numRows; i++ {
		// Append primary key
		expectedPks[i] = int64(i)
		pkBuilder.Append(expectedPks[i])

		// Generate random float vector and convert to bytes
		vectorBytes := make([]byte, dim*4)
		for j := 0; j < dim; j++ {
			floatVal := rand.Float32()
			bits := math.Float32bits(floatVal)
			common.Endian.PutUint32(vectorBytes[j*4:], bits)
		}
		expectedVectors[i] = vectorBytes
		vectorBuilder.Append(vectorBytes)
	}

	rec := b.NewRecord()
	defer rec.Release()

	require.Equal(t, int64(numRows), rec.NumRows())

	// Define column groups
	columnGroups := []storagecommon.ColumnGroup{
		{Columns: []int{0, 1}, GroupID: storagecommon.DefaultShortColumnGroupID},
	}

	// Create FFI packed writer and write data
	pw, err := NewFFIPackedWriter(basePath, version, schema, columnGroups, nil, nil)
	require.NoError(t, err)

	err = pw.WriteRecordBatch(rec)
	require.NoError(t, err)

	manifest, err := pw.Close()
	require.NoError(t, err)
	require.NotEmpty(t, manifest)

	t.Logf("Successfully wrote %d rows with %d-dim float vectors, manifest: %s", numRows, dim, manifest)

	// Create storage config for reader
	storageConfig := &indexpb.StorageConfig{
		RootPath:    dir,
		StorageType: "local",
	}

	// Create FFI packed reader
	neededColumns := []string{"pk", "vector"}
	reader, err := NewFFIPackedReader(manifest, schema, neededColumns, 8192, storageConfig, nil)
	require.NoError(t, err)
	require.NotNil(t, reader)

	// Verify schema
	assert.Equal(t, schema, reader.Schema())

	// Read all records and verify data
	totalRowsRead := int64(0)
	for {
		record, err := reader.ReadNext()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NotNil(t, record)

		// Verify column count
		assert.Equal(t, int64(2), record.NumCols())

		// Verify pk column
		pkCol := record.Column(0).(*array.Int64)
		for i := 0; i < pkCol.Len(); i++ {
			expectedIdx := int(totalRowsRead) + i
			assert.Equal(t, expectedPks[expectedIdx], pkCol.Value(i), fmt.Sprintf("pk mismatch at row %d", expectedIdx))
		}

		// Verify vector column
		vectorCol := record.Column(1).(*array.FixedSizeBinary)
		for i := 0; i < vectorCol.Len(); i++ {
			expectedIdx := int(totalRowsRead) + i
			assert.Equal(t, expectedVectors[expectedIdx], vectorCol.Value(i), fmt.Sprintf("vector mismatch at row %d", expectedIdx))
		}

		totalRowsRead += record.NumRows()
	}

	// Verify total rows read
	assert.Equal(t, int64(numRows), totalRowsRead)

	t.Logf("Successfully read %d rows", totalRowsRead)

	// Close reader
	err = reader.Close()
	require.NoError(t, err)
}

func TestPackedFFIReaderPartialColumns(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	dir := t.TempDir()
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)

	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	const (
		numRows = 500
		dim     = 64
	)

	// Create schema with 3 columns
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "pk",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
		{
			Name:     "score",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"101"}),
		},
		{
			Name:     "vector",
			Type:     &arrow.FixedSizeBinaryType{ByteWidth: dim * 4},
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"102"}),
		},
	}, nil)

	basePath := "files/packed_reader_partial_test/1"
	version := int64(0)

	// Build record batch
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	pkBuilder := b.Field(0).(*array.Int64Builder)
	scoreBuilder := b.Field(1).(*array.Float64Builder)
	vectorBuilder := b.Field(2).(*array.FixedSizeBinaryBuilder)

	expectedPks := make([]int64, numRows)
	expectedScores := make([]float64, numRows)

	for i := 0; i < numRows; i++ {
		expectedPks[i] = int64(i)
		pkBuilder.Append(expectedPks[i])

		expectedScores[i] = rand.Float64() * 100
		scoreBuilder.Append(expectedScores[i])

		vectorBytes := make([]byte, dim*4)
		for j := 0; j < dim; j++ {
			floatVal := rand.Float32()
			bits := math.Float32bits(floatVal)
			common.Endian.PutUint32(vectorBytes[j*4:], bits)
		}
		vectorBuilder.Append(vectorBytes)
	}

	rec := b.NewRecord()
	defer rec.Release()

	// Define column groups
	columnGroups := []storagecommon.ColumnGroup{
		{Columns: []int{0, 1, 2}, GroupID: storagecommon.DefaultShortColumnGroupID},
	}

	// Write data
	pw, err := NewFFIPackedWriter(basePath, version, schema, columnGroups, nil, nil)
	require.NoError(t, err)

	err = pw.WriteRecordBatch(rec)
	require.NoError(t, err)

	manifest, err := pw.Close()
	require.NoError(t, err)

	// Create storage config
	storageConfig := &indexpb.StorageConfig{
		RootPath:    dir,
		StorageType: "local",
	}

	// Read only pk and score columns (skip vector)
	neededColumns := []string{"pk", "score"}
	partialSchema := arrow.NewSchema([]arrow.Field{
		schema.Field(0),
		schema.Field(1),
	}, nil)

	reader, err := NewFFIPackedReader(manifest, partialSchema, neededColumns, 8192, storageConfig, nil)
	require.NoError(t, err)
	require.NotNil(t, reader)

	totalRowsRead := int64(0)
	for {
		record, err := reader.ReadNext()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		// Verify only 2 columns are returned
		assert.Equal(t, int64(2), record.NumCols())

		// Verify pk column
		pkCol := record.Column(0).(*array.Int64)
		for i := 0; i < pkCol.Len(); i++ {
			expectedIdx := int(totalRowsRead) + i
			assert.Equal(t, expectedPks[expectedIdx], pkCol.Value(i))
		}

		// Verify score column
		scoreCol := record.Column(1).(*array.Float64)
		for i := 0; i < scoreCol.Len(); i++ {
			expectedIdx := int(totalRowsRead) + i
			assert.Equal(t, expectedScores[expectedIdx], scoreCol.Value(i))
		}

		totalRowsRead += record.NumRows()
	}

	assert.Equal(t, int64(numRows), totalRowsRead)
	t.Logf("Successfully read %d rows with partial columns", totalRowsRead)

	err = reader.Close()
	require.NoError(t, err)
}

func TestPackedFFIReaderMultipleBatches(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	dir := t.TempDir()
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)

	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	const (
		numRows   = 500
		dim       = 64
		numWrites = 3
	)

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "pk",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
		{
			Name:     "vector",
			Type:     &arrow.FixedSizeBinaryType{ByteWidth: dim * 4},
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"101"}),
		},
	}, nil)

	basePath := "files/packed_reader_multi_batch_test/1"
	version := int64(0)

	columnGroups := []storagecommon.ColumnGroup{
		{Columns: []int{0, 1}, GroupID: storagecommon.DefaultShortColumnGroupID},
	}

	var manifest string
	totalWrittenRows := 0

	// Write multiple batches
	for batch := 0; batch < numWrites; batch++ {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)

		pkBuilder := b.Field(0).(*array.Int64Builder)
		vectorBuilder := b.Field(1).(*array.FixedSizeBinaryBuilder)

		for i := 0; i < numRows; i++ {
			pkBuilder.Append(int64(totalWrittenRows + i))

			vectorBytes := make([]byte, dim*4)
			for j := 0; j < dim; j++ {
				floatVal := rand.Float32()
				bits := math.Float32bits(floatVal)
				common.Endian.PutUint32(vectorBytes[j*4:], bits)
			}
			vectorBuilder.Append(vectorBytes)
		}

		rec := b.NewRecord()

		pw, err := NewFFIPackedWriter(basePath, version, schema, columnGroups, nil, nil)
		require.NoError(t, err)

		err = pw.WriteRecordBatch(rec)
		require.NoError(t, err)

		manifest, err = pw.Close()
		require.NoError(t, err)

		_, version, err = UnmarshalManfestPath(manifest)
		require.NoError(t, err)

		totalWrittenRows += numRows

		b.Release()
		rec.Release()
	}

	// Read all data
	storageConfig := &indexpb.StorageConfig{
		RootPath:    dir,
		StorageType: "local",
	}

	neededColumns := []string{"pk", "vector"}
	reader, err := NewFFIPackedReader(manifest, schema, neededColumns, 8192, storageConfig, nil)
	require.NoError(t, err)

	totalRowsRead := int64(0)
	for {
		record, err := reader.ReadNext()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRowsRead += record.NumRows()
	}

	assert.Equal(t, int64(totalWrittenRows), totalRowsRead)
	t.Logf("Successfully read %d rows from %d batches", totalRowsRead, numWrites)

	err = reader.Close()
	require.NoError(t, err)
}
