package packed_test

import (
	"io"
	"path"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func nativePackedSchema(t *testing.T) (*arrow.Schema, *schemapb.FieldSchema) {
	t.Helper()
	leaf := &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int32}}
	inner := &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}}
	field := &schemapb.FieldSchema{FieldID: 101, Name: "101", DataType: schemapb.DataType_Array,
		ElementType: schemapb.DataType_Array, Nullable: true, ElementNullable: true,
		TypeSchema: &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}}}
	typ, err := storage.ArrowTypeForField(field)
	require.NoError(t, err)
	id := func(name string, typ arrow.DataType, nullable bool) arrow.Field {
		return arrow.Field{Name: name, Type: typ, Nullable: nullable,
			Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{name})}
	}
	return arrow.NewSchema([]arrow.Field{id("100", arrow.PrimitiveTypes.Int64, false), id("101", typ, true)}, nil), field
}

func TestNativeArrayPackedParquetAndVortexRoundTrip(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, t.TempDir())
	t.Cleanup(func() { pt.Reset(pt.CommonCfg.StorageType.Key); pt.Reset(pt.LocalStorageCfg.Path.Key) })
	cfg := packed.CreateStorageConfig()
	schema, field := nativePackedSchema(t)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	row := &schemapb.ScalarField{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
		ElementType: schemapb.DataType_Int32,
		Data: []*schemapb.ScalarField{
			{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{7, 0}}}},
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{}}},
		},
	}}}
	rows := []*schemapb.ScalarField{row, nil, {Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int32}}}}
	for i, value := range rows {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		require.NoError(t, storage.SerializeNativeArrayRow(b.Field(1), value, field))
	}
	rec := b.NewRecord()
	defer rec.Release()
	outer := rec.Column(1).(*array.List)
	start, end := outer.ValueOffsets(1)
	require.Equal(t, start, end)
	innerArray := outer.ListValues().(*array.List)
	start, end = innerArray.ValueOffsets(1)
	require.Equal(t, start, end)

	for _, format := range []string{"parquet", "vortex"} {
		t.Run(format, func(t *testing.T) {
			basePath := path.Join(cfg.GetRootPath(), "native_array", format)
			writer, err := packed.NewFFISegmentWriter(schema, &packed.SegmentWriterConfig{
				SegmentPath: basePath, WriterFormat: format,
				ColumnGroups: []storagecommon.ColumnGroup{{GroupID: 0, Columns: []int{0, 1}}},
			}, cfg, nil)
			require.NoError(t, err)
			require.NoError(t, writer.Write(rec))
			output, err := writer.Close()
			require.NoError(t, err)
			defer output.Destroy()
			manifest, err := packed.CommitManifestUpdates(basePath, packed.ManifestEarliest, cfg, &packed.ManifestUpdates{NewFiles: output})
			require.NoError(t, err)
			reader, err := packed.NewFFIPackedReader(manifest, schema, []string{"100", "101"}, 64<<20, cfg, nil, packed.ExternalReaderContext{})
			require.NoError(t, err)
			defer reader.Close()
			gotRec, err := reader.ReadNext()
			require.NoError(t, err)
			defer gotRec.Release()
			require.Equal(t, int64(3), gotRec.NumRows())
			for i, want := range rows {
				got, err := storage.DeserializeNativeArrayRow(gotRec.Column(1), i, field)
				require.NoError(t, err)
				require.True(t, proto.Equal(want, got), "row %d: got %v, want %v", i, got, want)
			}
			_, err = reader.ReadNext()
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

func TestNativeArrayPackedParquetRejectsNonemptyNullList(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, t.TempDir())
	t.Cleanup(func() { pt.Reset(pt.CommonCfg.StorageType.Key); pt.Reset(pt.LocalStorageCfg.Path.Key) })
	cfg := packed.CreateStorageConfig()
	schema, _ := nativePackedSchema(t)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	outer := b.Field(1).(*array.ListBuilder)
	outer.Append(true)
	inner := outer.ValueBuilder().(*array.ListBuilder)
	inner.Append(true)
	inner.ValueBuilder().(*array.Int32Builder).Append(7)
	inner.AppendNull()
	inner.ValueBuilder().(*array.Int32Builder).Append(42)
	inner.Append(true)
	inner.ValueBuilder().(*array.Int32Builder).Append(9)
	rec := b.NewRecord()
	defer rec.Release()
	innerArray := rec.Column(1).(*array.List).ListValues().(*array.List)
	start, end := innerArray.ValueOffsets(1)
	require.Greater(t, end, start)
	writer, err := packed.NewFFISegmentWriter(schema, &packed.SegmentWriterConfig{
		SegmentPath: path.Join(cfg.GetRootPath(), "bad_native_array"), WriterFormat: "parquet",
		ColumnGroups: []storagecommon.ColumnGroup{{GroupID: 0, Columns: []int{0, 1}}},
	}, cfg, nil)
	require.NoError(t, err)
	err = writer.Write(rec)
	if err == nil {
		output, closeErr := writer.Close()
		if output != nil {
			output.Destroy()
		}
		err = closeErr
	} else {
		_, _ = writer.Close()
	}
	require.Error(t, err)
}
