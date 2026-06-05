package packed

import (
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestFFIPackedWriterDestroyIsIdempotent(t *testing.T) {
	var nilWriter *FFIPackedWriter
	require.NotPanics(t, func() {
		nilWriter.Destroy()
	})

	writer := &FFIPackedWriter{}
	require.NotPanics(t, func() {
		writer.Destroy()
		writer.Destroy()
	})
	_, err := writer.Close()
	require.Error(t, err)
}

func TestFFIPackedWriter_AsNewColumnGroupsAddsFields(t *testing.T) {
	writer := &FFIPackedWriter{}
	returned := writer.AsNewColumnGroups()

	require.Same(t, writer, returned)
	assert.True(t, writer.addNewColumnGroups)
}

func TestGetManifestFieldIDs_InvalidManifestPath(t *testing.T) {
	fields, err := GetManifestFieldIDs("not-a-manifest-path", nil)

	require.Error(t, err)
	assert.Nil(t, fields)
}

func TestGetManifestFieldIDs_InvalidColumnName(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, t.TempDir())
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "bad_column",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
	}, nil)
	columnGroups := []storagecommon.ColumnGroup{{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID}}
	basePath := "files/packed_writer_invalid_column/1"
	cfg := CreateStorageConfig()
	writer, err := NewFFIPackedWriter(basePath, schema, columnGroups, cfg, nil)
	require.NoError(t, err)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(1)
	record := builder.NewRecord()
	defer record.Release()
	require.NoError(t, writer.WriteRecordBatch(record))
	out, err := writer.Close()
	require.NoError(t, err)
	defer out.Destroy()
	manifest, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{NewFiles: out})
	require.NoError(t, err)

	fields, err := GetManifestFieldIDs(manifest, cfg)
	require.ErrorContains(t, err, "invalid manifest column name")
	assert.Nil(t, fields)
}

func TestGetManifestFieldIDs_FromPackedWriterManifest(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, t.TempDir())
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "100",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
		{
			Name:     "101",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"101"}),
		},
	}, nil)
	columnGroups := []storagecommon.ColumnGroup{{Columns: []int{0, 1}, GroupID: storagecommon.DefaultShortColumnGroupID}}
	basePath := "files/packed_writer_field_ids/1"
	cfg := CreateStorageConfig()
	writer, err := NewFFIPackedWriter(basePath, schema, columnGroups, cfg, nil)
	require.NoError(t, err)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(1)
	builder.Field(1).(*array.Int64Builder).Append(1000)
	record := builder.NewRecord()
	defer record.Release()
	require.NoError(t, writer.WriteRecordBatch(record))
	out, err := writer.Close()
	require.NoError(t, err)
	defer out.Destroy()
	manifest, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{NewFiles: out})
	require.NoError(t, err)

	fields, err := GetManifestFieldIDs(manifest, cfg)
	require.NoError(t, err)
	assert.Contains(t, fields, int64(100))
	assert.Contains(t, fields, int64(101))

	_, err = writer.Close()
	require.ErrorContains(t, err, "FFIPackedWriter already closed")
}

func TestResolveManifestSingleWriterFormat_Earliest(t *testing.T) {
	format, err := ResolveManifestSingleWriterFormat(MarshalManifestPath("files/empty/segment", ManifestEarliest), nil, nil, "")

	require.NoError(t, err)
	assert.Empty(t, format)
}

func TestResolveManifestSingleWriterFormat_FromPackedWriterManifest(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, t.TempDir())
	pt.Save(pt.DataNodeCfg.StorageFormat.Key, "parquet")
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
		pt.Reset(pt.DataNodeCfg.StorageFormat.Key)
	})

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "100",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
	}, nil)
	columnGroups := []storagecommon.ColumnGroup{{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID, Fields: []int64{100}}}
	basePath := "files/packed_writer_format/1"
	cfg := CreateStorageConfig()
	writer, err := NewFFIPackedWriter(basePath, schema, columnGroups, cfg, nil)
	require.NoError(t, err)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(1)
	record := builder.NewRecord()
	defer record.Release()
	require.NoError(t, writer.WriteRecordBatch(record))
	out, err := writer.Close()
	require.NoError(t, err)
	defer out.Destroy()
	manifest, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{NewFiles: out})
	require.NoError(t, err)

	format, err := ResolveManifestSingleWriterFormat(manifest, cfg, []string{"100"}, "")
	require.NoError(t, err)
	assert.Equal(t, "parquet", format)

	format, err = ResolveManifestSingleWriterFormat(manifest, cfg, []string{"101"}, "")
	require.NoError(t, err)
	assert.Empty(t, format)
}

func TestResolveManifestSingleWriterFormat_FiltersMixedAddColumnGroups(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, t.TempDir())
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	basePath := "files/packed_writer_mixed_format/1"
	cfg := CreateStorageConfig()
	writeColumn := func(name string, fieldID int64, format string, asNewColumnGroup bool) WriterOutput {
		schema := arrow.NewSchema([]arrow.Field{
			{
				Name:     name,
				Type:     arrow.PrimitiveTypes.Int64,
				Nullable: false,
				Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{name}),
			},
		}, nil)
		columnGroups := []storagecommon.ColumnGroup{
			{Columns: []int{0}, GroupID: fieldID, Fields: []int64{fieldID}},
		}
		writer, err := NewFFIPackedWriter(
			basePath,
			schema,
			columnGroups,
			cfg,
			nil,
			map[string]string{PropertyWriterFormat: format},
		)
		require.NoError(t, err)
		if asNewColumnGroup {
			writer.AsNewColumnGroups()
		}

		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer builder.Release()
		builder.Field(0).(*array.Int64Builder).Append(1)
		record := builder.NewRecord()
		defer record.Release()
		require.NoError(t, writer.WriteRecordBatch(record))
		out, err := writer.Close()
		require.NoError(t, err)
		return out
	}

	parquetOut := writeColumn("100", 100, "parquet", false)
	manifest, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{NewFiles: parquetOut})
	parquetOut.Destroy()
	require.NoError(t, err)

	committedBasePath, version, err := UnmarshalManifestPath(manifest)
	require.NoError(t, err)
	vortexOut := writeColumn("101", 101, "vortex", true)
	manifest, err = CommitManifestUpdates(committedBasePath, version, cfg, &ManifestUpdates{NewFiles: vortexOut})
	vortexOut.Destroy()
	require.NoError(t, err)

	format, err := ResolveManifestSingleWriterFormat(manifest, cfg, []string{"100"}, "")
	require.NoError(t, err)
	assert.Equal(t, "parquet", format)

	format, err = ResolveManifestSingleWriterFormat(manifest, cfg, []string{"101"}, "")
	require.NoError(t, err)
	assert.Equal(t, "vortex", format)

	_, err = ResolveManifestSingleWriterFormat(manifest, cfg, nil, "")
	require.ErrorContains(t, err, "mixed writer formats")
}

func TestPackedFFIWriter(t *testing.T) {
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
		numRows = 5000
		dim     = 768
		batch   = 10
	)

	// Create schema: int64 primary key + 768-dim float vector
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

	basePath := "files/packed_writer_test/1"
	version := int64(0)

	for i := 0; i < batch; i++ {
		// Build record batch with 5000 rows
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()

		pkBuilder := b.Field(0).(*array.Int64Builder)
		vectorBuilder := b.Field(1).(*array.FixedSizeBinaryBuilder)

		for i := 0; i < numRows; i++ {
			// Append primary key
			pkBuilder.Append(int64(i))

			// Generate random float vector and convert to bytes
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

		require.Equal(t, int64(numRows), rec.NumRows())

		// // Setup storage config for local filesystem
		// storageConfig := &indexpb.StorageConfig{
		// 	RootPath:    dir,
		// 	StorageType: "local",
		// }

		// Define column groups: pk and vector in the same group
		columnGroups := []storagecommon.ColumnGroup{
			{Columns: []int{0, 1}, GroupID: storagecommon.DefaultShortColumnGroupID},
		}

		// Create FFI packed writer
		cfg := CreateStorageConfig()
		pw, err := NewFFIPackedWriter(basePath, schema, columnGroups, cfg, nil)
		require.NoError(t, err)

		// Write record batch
		err = pw.WriteRecordBatch(rec)
		require.NoError(t, err)

		// Close writer to obtain column groups, commit via manifest update.
		out, err := pw.Close()
		require.NoError(t, err)

		manifest, err := CommitManifestUpdates(basePath, version, cfg,
			&ManifestUpdates{NewFiles: out})
		out.Destroy()
		require.NoError(t, err)
		require.NotEmpty(t, manifest)

		p, pv, err := UnmarshalManifestPath(manifest)
		require.NoError(t, err)
		assert.Equal(t, p, basePath)
		assert.Equal(t, pv, version+1)
		version = pv

		t.Logf("Successfully wrote %d rows with %d-dim float vectors, manifest: %s", numRows, dim, manifest)
	}
}

func TestFFIPackedWriter_CloseThenCommitUpdates(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	dir := t.TempDir()
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "pk",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
	}, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	pkb := b.Field(0).(*array.Int64Builder)
	for i := 0; i < 4; i++ {
		pkb.Append(int64(i))
	}
	rec := b.NewRecord()
	defer rec.Release()

	columnGroups := []storagecommon.ColumnGroup{
		{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID},
	}

	basePath := "files/close_commit_test/1"
	cfg := CreateStorageConfig()
	w, err := NewFFIPackedWriter(basePath, schema, columnGroups, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, w.WriteRecordBatch(rec))

	out, err := w.Close()
	require.NoError(t, err)
	require.NotNil(t, out)
	defer out.Destroy()

	mfPath, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg,
		&ManifestUpdates{NewFiles: out})
	require.NoError(t, err)

	_, v, err := UnmarshalManifestPath(mfPath)
	require.NoError(t, err)
	require.Equal(t, int64(1), v, "exactly one version bump expected")
}
