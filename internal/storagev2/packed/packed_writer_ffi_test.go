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
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

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
		pw, err := NewFFIPackedWriter(basePath, version, schema, columnGroups, nil, nil)
		require.NoError(t, err)

		// Write record batch
		err = pw.WriteRecordBatch(rec)
		require.NoError(t, err)

		// Close writer and get manifest
		manifest, err := pw.Close()
		require.NoError(t, err)
		require.NotEmpty(t, manifest)

		p, pv, err := UnmarshalManfestPath(manifest)
		require.NoError(t, err)
		assert.Equal(t, p, basePath)
		assert.Equal(t, pv, version+1)
		version = pv

		t.Logf("Successfully wrote %d rows with %d-dim float vectors, manifest: %s", numRows, dim, manifest)
	}
}
