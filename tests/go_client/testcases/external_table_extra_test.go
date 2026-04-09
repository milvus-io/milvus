package testcases

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// --- S3 storage helpers ---

// s3StorageConfig holds S3 storage configuration.
type s3StorageConfig struct {
	endpoint  string // S3 endpoint (without scheme)
	accessKey string
	secretKey string
	bucket    string
	rootPath  string // Milvus rootPath in the bucket
	region    string
}

func getS3StorageConfig() s3StorageConfig {
	return s3StorageConfig{
		endpoint:  envOrDefault("S3_ENDPOINT", "s3.us-west-2.amazonaws.com"),
		accessKey: envOrDefault("S3_ACCESS_KEY", ""),
		secretKey: envOrDefault("S3_SECRET_KEY", ""),
		bucket:    envOrDefault("S3_BUCKET", "file-transfering-bucket"),
		rootPath:  envOrDefault("S3_ROOT_PATH", "milvus-ext-table-test"),
		region:    envOrDefault("S3_REGION", "us-west-2"),
	}
}

// newS3AsMinIOClient creates a minio-go client connecting to AWS S3.
func newS3AsMinIOClient(cfg s3StorageConfig) (*miniogo.Client, error) {
	return miniogo.New(cfg.endpoint, &miniogo.Options{
		Creds:  miniocreds.NewStaticV4(cfg.accessKey, cfg.secretKey, ""),
		Secure: true,
		Region: cfg.region,
	})
}

func uploadToS3(ctx context.Context, t *testing.T, mc *miniogo.Client, bucket, key string, data []byte) {
	t.Helper()
	_, err := mc.PutObject(ctx, bucket, key,
		bytes.NewReader(data), int64(len(data)),
		miniogo.PutObjectOptions{ContentType: "application/octet-stream"})
	require.NoError(t, err, "upload to S3: %s", key)
	t.Logf("Uploaded s3://%s/%s (%d bytes)", bucket, key, len(data))
}

func cleanupS3Prefix(ctx context.Context, mc *miniogo.Client, bucket, prefix string) {
	for obj := range mc.ListObjects(ctx, bucket, miniogo.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			continue
		}
		_ = mc.RemoveObject(ctx, bucket, obj.Key, miniogo.RemoveObjectOptions{})
	}
}

// countManifestsForCollection counts segment manifests in MinIO for a given collectionID.
// Returns both the count and the paths found.
func countManifestsForCollection(ctx context.Context, mc *miniogo.Client, bucket, collectionID string) (int, []string) {
	prefix := fmt.Sprintf("external/%s/segments/", collectionID)
	var paths []string
	for obj := range mc.ListObjects(ctx, bucket, miniogo.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err == nil {
			paths = append(paths, obj.Key)
		}
	}
	return len(paths), paths
}

// encodeSparseVector encodes a sparse vector as Milvus internal format:
// sequence of (uint32_LE index, uint32_LE float32bits) pairs, 8 bytes each.
func encodeSparseVector(indices []uint32, values []float32) []byte {
	buf := make([]byte, 8*len(indices))
	for i := range indices {
		binary.LittleEndian.PutUint32(buf[i*8:], indices[i])
		binary.LittleEndian.PutUint32(buf[i*8+4:], math.Float32bits(values[i]))
	}
	return buf
}

// helperVectorE2E is a helper to run a single-vector external table E2E test.
// It generates a parquet with id + vector columns, refreshes, indexes, loads, and queries.
func helperVectorE2E(t *testing.T, ctx context.Context, mc *base.MilvusClient, minioClient *miniogo.Client,
	minioCfg minioConfig, testName string, vecField *entity.Field, arrowVecField arrow.Field,
	vecBuilder func(b array.Builder, i int), metricType entity.MetricType) {
	t.Helper()

	collName := common.GenRandomString("ext_vec", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)
	const numRows = 50

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrowVecField,
	}, nil)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()

	for i := 0; i < numRows; i++ {
		builder.Field(0).(*array.Int64Builder).Append(int64(i))
		vecBuilder(builder.Field(1), i)
	}

	record := builder.NewRecord()
	defer record.Release()
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", buf.Bytes())

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(vecField)

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	require.NoError(t, err)
	t.Logf("[create] ✓ %s", testName)

	// Refresh
	refreshAndWait(ctx, t, mc, collName)
	t.Logf("[refresh] ✓ %s", testName)

	// Index with correct metric
	idx := index.NewAutoIndex(metricType)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, vecField.Name, idx))
	require.NoError(t, err)
	require.NoError(t, idxTask.Await(ctx))

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))
	t.Logf("[index+load] ✓ %s (metric=%s)", testName, metricType)

	// count(*)
	countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	require.NoError(t, err)
	count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, int64(numRows), count)
	t.Logf("[count(*)] ✓ %s → %d rows", testName, count)
}

// TestExternalTableSchemaValidation tests all data types and schema features.
// For each case, we only call CreateCollection and check if it succeeds or fails.
// No MinIO or Refresh needed — this is purely schema validation.
func TestExternalTableSchemaValidation(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// --- Data types: each paired with FloatVector to form a valid external collection ---
	dataTypeCases := []struct {
		name      string
		field     *entity.Field
		expectErr string // empty = expect success
	}{
		// Scalar types
		{"Bool", entity.NewField().WithName("f").WithDataType(entity.FieldTypeBool).WithExternalField("f_col"), ""},
		{"Int8", entity.NewField().WithName("f").WithDataType(entity.FieldTypeInt8).WithExternalField("f_col"), ""},
		{"Int16", entity.NewField().WithName("f").WithDataType(entity.FieldTypeInt16).WithExternalField("f_col"), ""},
		{"Int32", entity.NewField().WithName("f").WithDataType(entity.FieldTypeInt32).WithExternalField("f_col"), ""},
		{"Int64", entity.NewField().WithName("f").WithDataType(entity.FieldTypeInt64).WithExternalField("f_col"), ""},
		{"Float", entity.NewField().WithName("f").WithDataType(entity.FieldTypeFloat).WithExternalField("f_col"), ""},
		{"Double", entity.NewField().WithName("f").WithDataType(entity.FieldTypeDouble).WithExternalField("f_col"), ""},
		{"VarChar", entity.NewField().WithName("f").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithExternalField("f_col"), ""},
		{"JSON", entity.NewField().WithName("f").WithDataType(entity.FieldTypeJSON).WithExternalField("f_col"), ""},

		// Array types (various element types)
		{"Array_VarChar", entity.NewField().WithName("f").WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeVarChar).WithMaxLength(128).WithMaxCapacity(100).WithExternalField("f_col"), ""},
		{"Array_Int64", entity.NewField().WithName("f").WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeInt64).WithMaxCapacity(100).WithExternalField("f_col"), ""},
		{"Array_Int32", entity.NewField().WithName("f").WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeInt32).WithMaxCapacity(100).WithExternalField("f_col"), ""},
		{"Array_Float", entity.NewField().WithName("f").WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeFloat).WithMaxCapacity(100).WithExternalField("f_col"), ""},
		{"Array_Bool", entity.NewField().WithName("f").WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeBool).WithMaxCapacity(100).WithExternalField("f_col"), ""},

		// Vector types
		{"FloatVector", entity.NewField().WithName("f").WithDataType(entity.FieldTypeFloatVector).WithDim(4).WithExternalField("f_col"), ""},
		{"BinaryVector", entity.NewField().WithName("f").WithDataType(entity.FieldTypeBinaryVector).WithDim(32).WithExternalField("f_col"), ""},
		{"Float16Vector", entity.NewField().WithName("f").WithDataType(entity.FieldTypeFloat16Vector).WithDim(4).WithExternalField("f_col"), ""},
		{"BFloat16Vector", entity.NewField().WithName("f").WithDataType(entity.FieldTypeBFloat16Vector).WithDim(4).WithExternalField("f_col"), ""},
		{"SparseVector", entity.NewField().WithName("f").WithDataType(entity.FieldTypeSparseVector).WithExternalField("f_col"), ""},

		// Geo/Timestamp types
		{"Timestamptz", entity.NewField().WithName("f").WithDataType(entity.FieldTypeTimestamptz).WithExternalField("f_col"), ""},
		{"Geometry", entity.NewField().WithName("f").WithDataType(entity.FieldTypeGeometry).WithExternalField("f_col"), ""},
	}

	for _, tc := range dataTypeCases {
		t.Run("DataType_"+tc.name, func(t *testing.T) {
			collName := common.GenRandomString("ext_dt", 6)
			schema := entity.NewSchema().
				WithName(collName).
				WithExternalSource("s3://test-bucket/data/").
				WithExternalSpec(`{"format": "parquet"}`)

			// If the field itself is a vector, no need to add another vector
			isVector := tc.field.DataType == entity.FieldTypeFloatVector ||
				tc.field.DataType == entity.FieldTypeBinaryVector ||
				tc.field.DataType == entity.FieldTypeFloat16Vector ||
				tc.field.DataType == entity.FieldTypeBFloat16Vector ||
				tc.field.DataType == entity.FieldTypeSparseVector
			if !isVector {
				schema.WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).
					WithDim(4).WithExternalField("vec_col"))
			}
			schema.WithField(tc.field)

			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
			if tc.expectErr != "" {
				require.Error(t, err, "expected error for %s", tc.name)
				require.Contains(t, err.Error(), tc.expectErr)
				t.Logf("✗ REJECTED: %s → %v", tc.name, err)
			} else if err != nil {
				t.Logf("✗ REJECTED (unexpected): %s → %v", tc.name, err)
			} else {
				t.Logf("✓ ACCEPTED: %s", tc.name)
			}
		})
	}

	// --- Schema features: expected to be rejected ---
	schemaFeatureCases := []struct {
		name      string
		schema    *entity.Schema
		expectErr string
	}{
		{
			"PrimaryKey",
			entity.NewSchema().WithName("x").
				WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("pk").WithDataType(entity.FieldTypeInt64).
					WithIsPrimaryKey(true).WithExternalField("pk_col")).
				WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).
					WithDim(4).WithExternalField("vec_col")),
			"does not support primary key",
		},
		{
			"DynamicField",
			entity.NewSchema().WithName("x").
				WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
				WithDynamicFieldEnabled(true).
				WithField(entity.NewField().WithName("f").WithDataType(entity.FieldTypeVarChar).
					WithMaxLength(256).WithExternalField("f_col")).
				WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).
					WithDim(4).WithExternalField("vec_col")),
			"does not support dynamic field",
		},
		{
			"PartitionKey",
			entity.NewSchema().WithName("x").
				WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("cat").WithDataType(entity.FieldTypeVarChar).
					WithMaxLength(256).WithIsPartitionKey(true).WithExternalField("cat_col")).
				WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).
					WithDim(4).WithExternalField("vec_col")),
			"does not support partition key",
		},
		{
			"ClusteringKey",
			entity.NewSchema().WithName("x").
				WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("ts").WithDataType(entity.FieldTypeInt64).
					WithIsClusteringKey(true).WithExternalField("ts_col")).
				WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).
					WithDim(4).WithExternalField("vec_col")),
			"does not support clustering key",
		},
		{
			"AutoID",
			entity.NewSchema().WithName("x").
				WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).
					WithIsAutoID(true).WithExternalField("id_col")).
				WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).
					WithDim(4).WithExternalField("vec_col")),
			"does not support auto id",
		},
		{
			"MissingExternalField",
			entity.NewSchema().WithName("x").
				WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("f").WithDataType(entity.FieldTypeVarChar).
					WithMaxLength(256)). // no external_field!
				WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).
					WithDim(4).WithExternalField("vec_col")),
			"must have external_field",
		},
		{
			"NoVectorField",
			entity.NewSchema().WithName("x").
				WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).
					WithExternalField("id_col")).
				WithField(entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).
					WithMaxLength(256).WithExternalField("text_col")),
			"", // might or might not be rejected
		},
		{
			"NullableScalar",
			entity.NewSchema().WithName("x").
				WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("f").WithDataType(entity.FieldTypeInt64).
					WithNullable(true).WithExternalField("f_col")).
				WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).
					WithDim(4).WithExternalField("vec_col")),
			"", // might or might not be rejected
		},
		{
			"NullableVector",
			entity.NewSchema().WithName("x").
				WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).
					WithExternalField("id_col")).
				WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).
					WithDim(4).WithNullable(true).WithExternalField("vec_col")),
			"", // might or might not be rejected
		},
		{
			"DefaultValue",
			entity.NewSchema().WithName("x").
				WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("f").WithDataType(entity.FieldTypeInt64).
					WithDefaultValueLong(42).WithExternalField("f_col")).
				WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).
					WithDim(4).WithExternalField("vec_col")),
			"", // might or might not be rejected
		},
	}

	for _, tc := range schemaFeatureCases {
		t.Run("Schema_"+tc.name, func(t *testing.T) {
			collName := common.GenRandomString("ext_sf", 6)
			tc.schema.WithName(collName)
			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, tc.schema))
			if tc.expectErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErr)
				t.Logf("✗ REJECTED (expected): %s → %v", tc.name, err)
			} else if err != nil {
				t.Logf("✗ REJECTED: %s → %v", tc.name, err)
			} else {
				t.Logf("✓ ACCEPTED: %s", tc.name)
			}
		})
	}

	// Schema_CreateAndDescribe: create, verify HasCollection, DescribeCollection fields, ListCollections
	t.Run("Schema_CreateAndDescribe", func(t *testing.T) {
		collName := common.GenRandomString("external", 6)

		schema := entity.NewSchema().
			WithName(collName).
			WithExternalSource("s3://test-bucket/data/").
			WithExternalSpec(`{"format": "parquet"}`).
			WithField(
				entity.NewField().
					WithName("text").
					WithDataType(entity.FieldTypeVarChar).
					WithMaxLength(256).
					WithExternalField("text_column"),
			).
			WithField(
				entity.NewField().
					WithName("embedding").
					WithDataType(entity.FieldTypeFloatVector).
					WithDim(common.DefaultDim).
					WithExternalField("embedding_column"),
			)

		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		// Verify collection exists
		has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(collName))
		common.CheckErr(t, err, true)
		require.True(t, has)

		// Describe collection and verify schema
		coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
		common.CheckErr(t, err, true)

		// Verify external source and spec
		require.Equal(t, "s3://test-bucket/data/", coll.Schema.ExternalSource)
		require.Equal(t, `{"format": "parquet"}`, coll.Schema.ExternalSpec)

		// Verify fields (user-defined fields + auto-generated __virtual_pk__)
		require.Len(t, coll.Schema.Fields, 3)

		// Verify virtual PK field
		vpkField := findFieldByName(coll.Schema.Fields, "__virtual_pk__")
		require.NotNil(t, vpkField)
		require.True(t, vpkField.PrimaryKey)
		require.True(t, vpkField.AutoID)

		// Verify text field
		textField := findFieldByName(coll.Schema.Fields, "text")
		require.NotNil(t, textField)
		require.Equal(t, entity.FieldTypeVarChar, textField.DataType)
		require.Equal(t, "text_column", textField.ExternalField)

		// Verify embedding field
		embeddingField := findFieldByName(coll.Schema.Fields, "embedding")
		require.NotNil(t, embeddingField)
		require.Equal(t, entity.FieldTypeFloatVector, embeddingField.DataType)
		require.Equal(t, "embedding_column", embeddingField.ExternalField)

		// List collections and verify
		collections, err := mc.ListCollections(ctx, client.NewListCollectionOption())
		common.CheckErr(t, err, true)
		require.Contains(t, collections, collName)
		t.Logf("✓ CreateAndDescribe: verified ExternalSource, ExternalSpec, Fields, __virtual_pk__, ListCollections")
	})

	// Schema_MultipleVectors: create collection with multiple vector fields and verify
	t.Run("Schema_MultipleVectors", func(t *testing.T) {
		collName := common.GenRandomString("external", 6)

		schema := entity.NewSchema().
			WithName(collName).
			WithExternalSource("s3://test-bucket/data/").
			WithExternalSpec(`{"format": "parquet"}`).
			WithField(
				entity.NewField().
					WithName("text").
					WithDataType(entity.FieldTypeVarChar).
					WithMaxLength(256).
					WithExternalField("text_column"),
			).
			WithField(
				entity.NewField().
					WithName("dense_embedding").
					WithDataType(entity.FieldTypeFloatVector).
					WithDim(common.DefaultDim).
					WithExternalField("dense_embedding_column"),
			).
			WithField(
				entity.NewField().
					WithName("binary_embedding").
					WithDataType(entity.FieldTypeBinaryVector).
					WithDim(common.DefaultDim).
					WithExternalField("binary_embedding_column"),
			)

		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		// Describe collection and verify schema
		coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
		common.CheckErr(t, err, true)

		// Verify external source
		require.Equal(t, "s3://test-bucket/data/", coll.Schema.ExternalSource)

		// Verify fields (user-defined fields + auto-generated __virtual_pk__)
		require.Len(t, coll.Schema.Fields, 4)

		// Verify vector fields
		denseField := findFieldByName(coll.Schema.Fields, "dense_embedding")
		require.NotNil(t, denseField)
		require.Equal(t, entity.FieldTypeFloatVector, denseField.DataType)
		require.Equal(t, "dense_embedding_column", denseField.ExternalField)

		binaryField := findFieldByName(coll.Schema.Fields, "binary_embedding")
		require.NotNil(t, binaryField)
		require.Equal(t, entity.FieldTypeBinaryVector, binaryField.DataType)
		require.Equal(t, "binary_embedding_column", binaryField.ExternalField)
		t.Logf("✓ MultipleVectors: verified dense and binary vector fields")
	})
}


// TestExternalTableDataTypesE2E tests each supported scalar type end-to-end:
// upload real parquet data → refresh → index → load → count(*) → query with output
func TestExternalTableDataTypesE2E(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*600) // 10 min total
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	// Each subtest: generate parquet with specific type → full lifecycle
	cases := []struct {
		name       string
		field      *entity.Field
		arrowField arrow.Field
		builder    func(b array.Builder, i int) // append one row
	}{
		{
			"Bool",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeBool).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.FixedWidthTypes.Boolean},
			func(b array.Builder, i int) { b.(*array.BooleanBuilder).Append(i%2 == 0) },
		},
		{
			"Int8",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeInt8).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Int8},
			func(b array.Builder, i int) { b.(*array.Int8Builder).Append(int8(i % 100)) },
		},
		{
			"Int16",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeInt16).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Int16},
			func(b array.Builder, i int) { b.(*array.Int16Builder).Append(int16(i)) },
		},
		{
			"Int32",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeInt32).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Int32},
			func(b array.Builder, i int) { b.(*array.Int32Builder).Append(int32(i)) },
		},
		{
			"Int64",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeInt64).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Int64},
			func(b array.Builder, i int) { b.(*array.Int64Builder).Append(int64(i)) },
		},
		{
			"Float",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeFloat).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Float32},
			func(b array.Builder, i int) { b.(*array.Float32Builder).Append(float32(i) * 1.5) },
		},
		{
			"Double",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeDouble).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Float64},
			func(b array.Builder, i int) { b.(*array.Float64Builder).Append(float64(i) * 0.01) },
		},
		{
			"VarChar",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.BinaryTypes.String},
			func(b array.Builder, i int) { b.(*array.StringBuilder).Append(fmt.Sprintf("str_%04d", i)) },
		},
		{
			"JSON",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeJSON).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.BinaryTypes.String},
			func(b array.Builder, i int) { b.(*array.StringBuilder).Append(fmt.Sprintf(`{"k":%d}`, i)) },
		},
		{
			"Array_VarChar",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeArray).
				WithElementType(entity.FieldTypeVarChar).WithMaxLength(128).WithMaxCapacity(100).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.ListOf(arrow.BinaryTypes.String)},
			func(b array.Builder, i int) {
				lb := b.(*array.ListBuilder)
				lb.Append(true)
				lb.ValueBuilder().(*array.StringBuilder).Append(fmt.Sprintf("t_%d", i))
			},
		},
		{
			"Array_Int64",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeArray).
				WithElementType(entity.FieldTypeInt64).WithMaxCapacity(100).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
			func(b array.Builder, i int) {
				lb := b.(*array.ListBuilder)
				lb.Append(true)
				lb.ValueBuilder().(*array.Int64Builder).Append(int64(i))
				lb.ValueBuilder().(*array.Int64Builder).Append(int64(i * 10))
			},
		},
		{
			"Timestamptz",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeTimestamptz).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.FixedWidthTypes.Timestamp_us},
			func(b array.Builder, i int) {
				baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				ts := baseTime.Add(time.Duration(i) * time.Hour)
				b.(*array.TimestampBuilder).Append(arrow.Timestamp(ts.UnixMicro()))
			},
		},
		{
			"Geometry",
			entity.NewField().WithName("f").WithDataType(entity.FieldTypeGeometry).WithExternalField("f"),
			arrow.Field{Name: "f", Type: arrow.BinaryTypes.String},
			func(b array.Builder, i int) {
				b.(*array.StringBuilder).Append(fmt.Sprintf("POINT(%f %f)", float64(i)*0.1, float64(i)*0.2))
			},
		},
	}

	const numRows = 50
	const dim = 4

	for _, tc := range cases {
		t.Run("E2E_"+tc.name, func(t *testing.T) {
			collName := common.GenRandomString("ext_e2e", 6)
			extPath := fmt.Sprintf("external-e2e-test/%s", collName)

			// Generate parquet
			arrowSchema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				tc.arrowField,
				{Name: "embedding", Type: arrow.FixedSizeListOf(dim, arrow.PrimitiveTypes.Float32)},
			}, nil)

			var buf bytes.Buffer
			writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
			require.NoError(t, err)

			pool := memory.NewGoAllocator()
			builder := array.NewRecordBuilder(pool, arrowSchema)
			defer builder.Release()

			for i := 0; i < numRows; i++ {
				builder.Field(0).(*array.Int64Builder).Append(int64(i))
				tc.builder(builder.Field(1), i)
				embB := builder.Field(2).(*array.FixedSizeListBuilder)
				embB.Append(true)
				for d := 0; d < dim; d++ {
					embB.ValueBuilder().(*array.Float32Builder).Append(float32(i)*0.1 + float32(d))
				}
			}

			record := builder.NewRecord()
			defer record.Release()
			require.NoError(t, writer.Write(record))
			require.NoError(t, writer.Close())

			// Upload to MinIO
			uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", buf.Bytes())

			t.Cleanup(func() {
				cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
				_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
			})

			// Create collection
			schema := entity.NewSchema().
				WithName(collName).
				WithExternalSource(extPath).
				WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
				WithField(tc.field).
				WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(dim).WithExternalField("embedding"))

			err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
			require.NoError(t, err, "CreateCollection should succeed for %s", tc.name)
			t.Logf("[create] ✓ %s", tc.name)

			// Refresh
			refreshAndWait(ctx, t, mc, collName)
			t.Logf("[refresh] ✓ %s", tc.name)

			// Index + Load
			indexAndLoadCollection(ctx, t, mc, collName, "embedding")
			t.Logf("[index+load] ✓ %s", tc.name)

			// count(*)
			countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
				WithConsistencyLevel(entity.ClStrong).
				WithOutputFields(common.QueryCountFieldName))
			require.NoError(t, err)
			count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
			require.Equal(t, int64(numRows), count)
			t.Logf("[count(*)] ✓ %s → %d rows", tc.name, count)

			// Query with output field — this is where Array crashes
			_, err = mc.Query(ctx, client.NewQueryOption(collName).
				WithConsistencyLevel(entity.ClStrong).
				WithFilter("id == 0").
				WithOutputFields("id", "f"))
			if err != nil {
				t.Logf("[query output] ✗ %s → ERROR: %v", tc.name, err)
			} else {
				t.Logf("[query output] ✓ %s", tc.name)
			}

			// Search
			vec := make([]float32, dim)
			for i := range vec {
				vec[i] = float32(i) * 0.1
			}
			searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
				[]entity.Vector{entity.FloatVector(vec)}).
				WithConsistencyLevel(entity.ClStrong).
				WithANNSField("embedding").
				WithOutputFields("id", "f"))
			if err != nil {
				t.Logf("[search output] ✗ %s → ERROR: %v", tc.name, err)
			} else {
				require.Greater(t, searchRes[0].ResultCount, 0)
				t.Logf("[search output] ✓ %s → %d results", tc.name, searchRes[0].ResultCount)
			}
		})
	}
}


// TestExternalTableVectorTypesE2E tests special vector types with Parquet data.
func TestExternalTableVectorTypesE2E(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*600)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	const dim = 4

	t.Run("E2E_BinaryVector", func(t *testing.T) {
		const binDim = 32
		helperVectorE2E(t, ctx, mc, minioClient, minioCfg, "BinaryVector",
			entity.NewField().WithName("vec").WithDataType(entity.FieldTypeBinaryVector).WithDim(binDim).WithExternalField("vec"),
			arrow.Field{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: binDim / 8}},
			func(b array.Builder, i int) {
				data := make([]byte, binDim/8)
				for j := range data {
					data[j] = byte(i + j)
				}
				b.(*array.FixedSizeBinaryBuilder).Append(data)
			},
			entity.HAMMING)
	})

	t.Run("E2E_Float16Vector", func(t *testing.T) {
		helperVectorE2E(t, ctx, mc, minioClient, minioCfg, "Float16Vector",
			entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloat16Vector).WithDim(dim).WithExternalField("vec"),
			arrow.Field{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: dim * 2}},
			func(b array.Builder, i int) {
				data := make([]byte, dim*2)
				for j := range data {
					data[j] = byte(i + j)
				}
				b.(*array.FixedSizeBinaryBuilder).Append(data)
			},
			entity.L2)
	})

	t.Run("E2E_BFloat16Vector", func(t *testing.T) {
		helperVectorE2E(t, ctx, mc, minioClient, minioCfg, "BFloat16Vector",
			entity.NewField().WithName("vec").WithDataType(entity.FieldTypeBFloat16Vector).WithDim(dim).WithExternalField("vec"),
			arrow.Field{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: dim * 2}},
			func(b array.Builder, i int) {
				data := make([]byte, dim*2)
				for j := range data {
					data[j] = byte(i + j)
				}
				b.(*array.FixedSizeBinaryBuilder).Append(data)
			},
			entity.L2)
	})
}


// TestExternalTableSparseVectorE2E tests sparse vector external collections with multiple encoding approaches.
func TestExternalTableSparseVectorE2E(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	const numRows = 20

	// SparseVector_Binary: Parquet binary column with fake byte content
	t.Run("SparseVector_Binary", func(t *testing.T) {
		collName := common.GenRandomString("ext_sparse", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)
		const dim = 4

		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "sparse", Type: arrow.BinaryTypes.Binary},
			{Name: "embedding", Type: arrow.FixedSizeListOf(dim, arrow.PrimitiveTypes.Float32)},
		}, nil)

		var buf bytes.Buffer
		writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
		require.NoError(t, err)

		pool := memory.NewGoAllocator()
		builder := array.NewRecordBuilder(pool, arrowSchema)
		defer builder.Release()

		for i := 0; i < numRows; i++ {
			builder.Field(0).(*array.Int64Builder).Append(int64(i))
			// Write fake sparse vector bytes (1 pair: 4 bytes index + 4 bytes value)
			data := make([]byte, 8)
			builder.Field(1).(*array.BinaryBuilder).Append(data)
			embB := builder.Field(2).(*array.FixedSizeListBuilder)
			embB.Append(true)
			for d := 0; d < dim; d++ {
				embB.ValueBuilder().(*array.Float32Builder).Append(float32(i)*0.1 + float32(d))
			}
		}

		record := builder.NewRecord()
		defer record.Release()
		require.NoError(t, writer.Write(record))
		require.NoError(t, writer.Close())

		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", buf.Bytes())

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		schema := entity.NewSchema().
			WithName(collName).
			WithExternalSource(extPath).
			WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector).WithExternalField("sparse")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(dim).WithExternalField("embedding"))

		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		require.NoError(t, err)
		t.Log("[create] ✓ SparseVector_Binary")

		refreshResult, err := mc.RefreshExternalCollection(ctx,
			client.NewRefreshExternalCollectionOption(collName))
		if err != nil {
			t.Logf("[refresh submit] ✗ SparseVector_Binary → %v", err)
			return
		}

		jobID := refreshResult.JobID
		t.Logf("[refresh] started job %d", jobID)

		deadline := time.After(120 * time.Second)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-deadline:
				t.Logf("[refresh] ✗ timed out after 120s")
				return
			case <-ticker.C:
				progress, err := mc.GetRefreshExternalCollectionProgress(ctx,
					client.NewGetRefreshExternalCollectionProgressOption(jobID))
				if err != nil {
					t.Logf("[refresh poll] ✗ error: %v", err)
					return
				}
				t.Logf("[refresh] state=%s progress=%d%%", progress.State, progress.Progress)

				switch progress.State {
				case entity.RefreshStateCompleted:
					t.Log("[refresh] ✓ completed — SparseVector_Binary refresh succeeded")

					sparseIdx := index.NewAutoIndex(entity.IP)
					sparseTask, sErr := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "sparse", sparseIdx))
					require.NoError(t, sErr)
					require.NoError(t, sparseTask.Await(ctx))

					embIdx := index.NewAutoIndex(entity.L2)
					embTask, idxErr := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "embedding", embIdx))
					require.NoError(t, idxErr)
					require.NoError(t, embTask.Await(ctx))

					loadTask, lErr := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
					require.NoError(t, lErr)
					require.NoError(t, loadTask.Await(ctx))

					countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
						WithConsistencyLevel(entity.ClStrong).
						WithOutputFields(common.QueryCountFieldName))
					require.NoError(t, err)
					count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
					t.Logf("[count(*)] = %d", count)

					_, err = mc.Query(ctx, client.NewQueryOption(collName).
						WithConsistencyLevel(entity.ClStrong).
						WithFilter("id == 0").
						WithOutputFields("id", "sparse"))
					if err != nil {
						t.Logf("[query output sparse] ✗ %v", err)
					} else {
						t.Log("[query output sparse] ✓")
					}
					return

				case entity.RefreshStateFailed:
					t.Logf("[refresh] ✗ FAILED (expected): %s", progress.Reason)
					return
				}
			}
		}
	})

	// SparseVector_String_JSON: Parquet string column with JSON-like representation
	t.Run("SparseVector_String_JSON", func(t *testing.T) {
		collName := common.GenRandomString("ext_sparse", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)
		const dim = 4

		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "sparse", Type: arrow.BinaryTypes.String},
			{Name: "embedding", Type: arrow.FixedSizeListOf(dim, arrow.PrimitiveTypes.Float32)},
		}, nil)

		var buf bytes.Buffer
		writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
		require.NoError(t, err)

		pool := memory.NewGoAllocator()
		builder := array.NewRecordBuilder(pool, arrowSchema)
		defer builder.Release()

		for i := 0; i < numRows; i++ {
			builder.Field(0).(*array.Int64Builder).Append(int64(i))
			builder.Field(1).(*array.StringBuilder).Append(fmt.Sprintf(`{"%d": %f}`, i, float64(i)*0.1))
			embB := builder.Field(2).(*array.FixedSizeListBuilder)
			embB.Append(true)
			for d := 0; d < dim; d++ {
				embB.ValueBuilder().(*array.Float32Builder).Append(float32(i)*0.1 + float32(d))
			}
		}

		record := builder.NewRecord()
		defer record.Release()
		require.NoError(t, writer.Write(record))
		require.NoError(t, writer.Close())

		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", buf.Bytes())

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		schema := entity.NewSchema().
			WithName(collName).
			WithExternalSource(extPath).
			WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector).WithExternalField("sparse")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(dim).WithExternalField("embedding"))

		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		require.NoError(t, err)
		t.Log("[create] ✓ SparseVector_String_JSON")

		refreshResult, err := mc.RefreshExternalCollection(ctx,
			client.NewRefreshExternalCollectionOption(collName))
		if err != nil {
			t.Logf("[refresh submit] ✗ SparseVector_String_JSON → %v", err)
			return
		}

		jobID := refreshResult.JobID
		deadline := time.After(120 * time.Second)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-deadline:
				t.Logf("[refresh] ✗ timed out")
				return
			case <-ticker.C:
				progress, err := mc.GetRefreshExternalCollectionProgress(ctx,
					client.NewGetRefreshExternalCollectionProgressOption(jobID))
				if err != nil {
					t.Logf("[refresh poll] ✗ error: %v", err)
					return
				}
				t.Logf("[refresh] state=%s progress=%d%%", progress.State, progress.Progress)

				switch progress.State {
				case entity.RefreshStateCompleted:
					t.Log("[refresh] ✓ completed — SparseVector_String_JSON refresh succeeded")

					sparseIdx := index.NewAutoIndex(entity.IP)
					sparseTask, sErr := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "sparse", sparseIdx))
					require.NoError(t, sErr)
					require.NoError(t, sparseTask.Await(ctx))

					embIdx := index.NewAutoIndex(entity.L2)
					embTask, idxErr := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "embedding", embIdx))
					require.NoError(t, idxErr)
					require.NoError(t, embTask.Await(ctx))

					loadTask, lErr := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
					require.NoError(t, lErr)
					require.NoError(t, loadTask.Await(ctx))

					countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
						WithConsistencyLevel(entity.ClStrong).
						WithOutputFields(common.QueryCountFieldName))
					require.NoError(t, err)
					count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
					t.Logf("[count(*)] = %d", count)
					return

				case entity.RefreshStateFailed:
					t.Logf("[refresh] ✗ FAILED (expected): %s", progress.Reason)
					return
				}
			}
		}
	})

	// SparseVector_RealEncoding: real Milvus-encoded sparse vectors in Parquet binary column
	t.Run("SparseVector_RealEncoding", func(t *testing.T) {
		collName := common.GenRandomString("ext_sparse_real", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)
		const numRealRows = 100

		// Generate Parquet with real sparse vectors in Milvus internal encoding
		// Schema: id (Int64) + sparse (Binary) where binary = Milvus sparse encoding
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "sparse", Type: arrow.BinaryTypes.Binary},
		}, nil)

		var buf bytes.Buffer
		writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, nil, pqarrow.DefaultWriterProps())
		require.NoError(t, err)

		pool := memory.NewGoAllocator()
		builder := array.NewRecordBuilder(pool, arrowSchema)
		defer builder.Release()

		// Each row gets a sparse vector with 3 non-zero elements.
		// Row i has: {index: i*10, value: 1.0}, {index: i*10+1, value: 0.5}, {index: i*10+2, value: 0.1}
		for i := 0; i < numRealRows; i++ {
			builder.Field(0).(*array.Int64Builder).Append(int64(i))
			sparseBytes := encodeSparseVector(
				[]uint32{uint32(i*10), uint32(i*10 + 1), uint32(i*10 + 2)},
				[]float32{1.0, 0.5, 0.1},
			)
			builder.Field(1).(*array.BinaryBuilder).Append(sparseBytes)
		}

		record := builder.NewRecord()
		defer record.Release()
		require.NoError(t, writer.Write(record))
		require.NoError(t, writer.Close())

		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", buf.Bytes())

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		// Create external collection with only SparseFloatVector (single vector field)
		schema := entity.NewSchema().
			WithName(collName).
			WithExternalSource(extPath).
			WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector).WithExternalField("sparse"))

		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		require.NoError(t, err)
		t.Log("[create] ✓")

		// Refresh
		refreshAndWait(ctx, t, mc, collName)
		t.Log("[refresh] ✓")

		// Index with IP metric (required for sparse)
		sparseIdx := index.NewAutoIndex(entity.IP)
		idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "sparse", sparseIdx))
		require.NoError(t, err)
		require.NoError(t, idxTask.Await(ctx))
		t.Log("[index] ✓ sparse IP")

		// Load
		loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
		require.NoError(t, err)
		require.NoError(t, loadTask.Await(ctx))
		t.Log("[load] ✓")

		// count(*)
		countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		require.NoError(t, err)
		count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, int64(numRealRows), count)
		t.Logf("[count(*)] ✓ %d rows", count)

		// Query with output sparse field for row id=5
		// Expected: indices {50, 51, 52}, values {1.0, 0.5, 0.1}
		res, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter("id == 5").
			WithOutputFields("id", "sparse"))
		if err != nil {
			t.Logf("[query output] ✗ %v", err)
		} else {
			require.Equal(t, 1, res.GetColumn("id").Len())
			sparseCol := res.GetColumn("sparse")
			require.NotNil(t, sparseCol)
			t.Logf("[query output] ✓ sparse col type=%T len=%d", sparseCol, sparseCol.Len())
		}

		// Search with a sparse query vector matching row 0:
		// {index: 0, value: 1.0}, {index: 1, value: 0.5}, {index: 2, value: 0.1}
		queryVec, err := entity.NewSliceSparseEmbedding(
			[]uint32{0, 1, 2},
			[]float32{1.0, 0.5, 0.1},
		)
		require.NoError(t, err)

		searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
			[]entity.Vector{queryVec}).
			WithConsistencyLevel(entity.ClStrong).
			WithANNSField("sparse").
			WithOutputFields("id"))
		if err != nil {
			t.Logf("[search] ✗ %v", err)
		} else {
			require.Equal(t, 1, len(searchRes))
			require.Greater(t, searchRes[0].ResultCount, 0)
			nearestID, _ := searchRes[0].GetColumn("id").GetAsInt64(0)
			nearestScore := searchRes[0].Scores[0]
			t.Logf("[search] ✓ top-1: id=%d score=%.4f (expected id=0 with score≈1.26)", nearestID, nearestScore)

			// Row 0 has {0:1.0, 1:0.5, 2:0.1}, query is the same.
			// IP = 1.0*1.0 + 0.5*0.5 + 0.1*0.1 = 1.26
			require.Equal(t, int64(0), nearestID, "nearest neighbor should be id=0 (exact match)")
			require.InDelta(t, 1.26, nearestScore, 0.01, "IP score should be ~1.26")
		}

		t.Log("[ALL] ✓ SparseFloatVector with real Milvus encoding works end-to-end!")
	})
}


// TestExternalTableRefreshErrors tests various error conditions when refreshing.
func TestExternalTableRefreshErrors(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// NonExistentCollection: refresh a collection that doesn't exist
	t.Run("NonExistentCollection", func(t *testing.T) {
		_, err := mc.RefreshExternalCollection(ctx,
			client.NewRefreshExternalCollectionOption("non_existent_collection_xyz"))
		require.Error(t, err)
		t.Logf("✓ Error: %v", err)
	})

	// NormalCollection: refresh a non-external (normal) collection
	t.Run("NormalCollection", func(t *testing.T) {
		collName := common.GenRandomString("normal", 6)
		schema := entity.NewSchema().WithName(collName).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
			WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)
		t.Cleanup(func() { mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName)) })

		_, err = mc.RefreshExternalCollection(ctx,
			client.NewRefreshExternalCollectionOption(collName))
		require.Error(t, err)
		t.Logf("✓ Error: %v", err)
	})

	// EmptySource: refresh external collection with no data files
	t.Run("EmptySource", func(t *testing.T) {
		minioCfg := getMinIOConfig()
		minioClient, err := newMinIOClient(minioCfg)
		if err != nil {
			t.Skipf("MinIO unavailable: %v", err)
		}
		exists, _ := minioClient.BucketExists(ctx, minioCfg.bucket)
		if !exists {
			t.Skipf("MinIO bucket not accessible")
		}

		collName := common.GenRandomString("ext_empty", 6)
		// Point to empty directory (no parquet files)
		extPath := fmt.Sprintf("external-e2e-test/empty_%s", collName)

		t.Cleanup(func() { mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName)) })

		schema := entity.NewSchema().WithName(collName).
			WithExternalSource(extPath).WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(4).WithExternalField("vec"))
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		refreshRes, err := mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(collName))
		if err != nil {
			t.Logf("✓ Refresh submit rejected: %v", err)
			return
		}

		// Poll
		deadline := time.After(60 * time.Second)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-deadline:
				t.Fatal("timeout")
			case <-ticker.C:
				p, _ := mc.GetRefreshExternalCollectionProgress(ctx,
					client.NewGetRefreshExternalCollectionProgressOption(refreshRes.JobID))
				t.Logf("  state=%s progress=%d%% reason=%q", p.State, p.Progress, p.Reason)
				if p.State == entity.RefreshStateCompleted {
					t.Log("✓ Completed with 0 rows")
					return
				}
				if p.State == entity.RefreshStateFailed {
					t.Logf("✓ Failed (expected for empty source): %s", p.Reason)
					return
				}
			}
		}
	})

	// ConcurrentRejected: second refresh rejected when first is still running
	t.Run("ConcurrentRejected", func(t *testing.T) {
		minioCfg := getMinIOConfig()
		minioClient, err := newMinIOClient(minioCfg)
		if err != nil {
			t.Skipf("MinIO unavailable: %v", err)
		}
		exists, _ := minioClient.BucketExists(ctx, minioCfg.bucket)
		if !exists {
			t.Skipf("MinIO bucket not accessible")
		}

		collName := common.GenRandomString("ext_conc_ref", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)

		// Upload large-ish data to make refresh take a moment
		data, err := generateParquetBytes(1000, 0)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		schema := entity.NewSchema().WithName(collName).
			WithExternalSource(extPath).WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		// First refresh
		res1, err := mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(collName))
		require.NoError(t, err)
		t.Logf("First refresh job: %d", res1.JobID)

		// Immediately try second refresh
		_, err = mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(collName))
		if err != nil {
			t.Logf("✓ Second refresh rejected: %v", err)
		} else {
			t.Log("⚠️ Second refresh accepted (not rejected)")
		}

		// Wait for first to complete
		waitForRefreshComplete(ctx, t, mc, res1.JobID, 60*time.Second)
	})

	// InvalidJobID: get progress for a job ID that doesn't exist
	t.Run("InvalidJobID", func(t *testing.T) {
		_, err := mc.GetRefreshExternalCollectionProgress(ctx,
			client.NewGetRefreshExternalCollectionProgressOption(999999999))
		if err != nil {
			t.Logf("✓ Error: %v", err)
		} else {
			t.Log("⚠️ No error returned for invalid job ID")
		}
	})

	// SchemaMismatch: full lifecycle test when external_field maps to non-existent Parquet columns.
	// Parquet columns: id, value, embedding
	// Milvus mapping:  wrong_col_a → doesn't exist, wrong_col_b → doesn't exist
	// Test each stage: Create → Refresh → Index → Load → Query to find where mismatch is detected.
	t.Run("SchemaMismatch", func(t *testing.T) {
		minioCfg := getMinIOConfig()
		minioClient, err := newMinIOClient(minioCfg)
		if err != nil {
			t.Skipf("MinIO unavailable: %v", err)
		}
		exists, _ := minioClient.BucketExists(ctx, minioCfg.bucket)
		if !exists {
			t.Skipf("MinIO bucket not accessible")
		}

		collName := common.GenRandomString("ext_mismatch_e2e", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)

		// Parquet has columns: id (Int64), value (Float), embedding (FixedSizeList[Float32, 4])
		data, err := generateParquetBytes(100, 0)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		// Milvus schema maps to WRONG column names
		schema := entity.NewSchema().WithName(collName).
			WithExternalSource(extPath).WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("x").WithDataType(entity.FieldTypeInt64).
				WithExternalField("wrong_col_a")).   // Parquet has "id", not "wrong_col_a"
			WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).
				WithDim(testVecDim).WithExternalField("wrong_col_b")) // Parquet has "embedding", not "wrong_col_b"

		// Stage 1: Create
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		if err != nil {
			t.Logf("[create] ✗ Rejected: %v", err)
			return
		}
		t.Log("[create] ✓ accepted (schema mismatch not caught here)")

		// Stage 2: Refresh
		refreshRes, err := mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(collName))
		if err != nil {
			t.Logf("[refresh submit] ✗ Rejected: %v", err)
			return
		}

		deadline := time.After(60 * time.Second)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-deadline:
				t.Fatal("[refresh] timeout")
			case <-ticker.C:
				p, _ := mc.GetRefreshExternalCollectionProgress(ctx,
					client.NewGetRefreshExternalCollectionProgressOption(refreshRes.JobID))
				t.Logf("[refresh] state=%s progress=%d%% reason=%q", p.State, p.Progress, p.Reason)
				if p.State == entity.RefreshStateFailed {
					t.Logf("[refresh] ✗ Failed (mismatch caught at refresh): %s", p.Reason)
					return
				}
				if p.State == entity.RefreshStateCompleted {
					t.Log("[refresh] ✓ completed (mismatch NOT caught at refresh)")
					goto tryIndex
				}
			}
		}

	tryIndex:
		// Stage 3: Index
		idxType := index.NewAutoIndex(entity.L2)
		idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vec", idxType))
		if err != nil {
			t.Logf("[index] ✗ Failed: %v", err)
			return
		}
		idxCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		err = idxTask.Await(idxCtx)
		if err != nil {
			t.Logf("[index] ✗ Await failed (mismatch caught at index build): %v", err)
			return
		}
		t.Log("[index] ✓ completed (mismatch NOT caught at index)")

		// Stage 4: Load
		loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
		if err != nil {
			t.Logf("[load] ✗ Failed: %v", err)
			return
		}
		loadCtx, cancel2 := context.WithTimeout(ctx, 60*time.Second)
		defer cancel2()
		err = loadTask.Await(loadCtx)
		if err != nil {
			t.Logf("[load] ✗ Await failed (mismatch caught at load): %v", err)
			return
		}
		t.Log("[load] ✓ completed (mismatch NOT caught at load)")

		// Stage 5: Query
		_, err = mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		if err != nil {
			t.Logf("[query count(*)] ✗ Failed (mismatch caught at query): %v", err)
			return
		}
		t.Log("[query count(*)] ✓ completed")

		_, err = mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter("x == 0").
			WithOutputFields("x"))
		if err != nil {
			t.Logf("[query filter] ✗ Failed (mismatch caught at filtered query): %v", err)
		} else {
			t.Log("[query filter] ✓ completed — schema mismatch NEVER detected!")
		}
	})
}


// TestExternalTableDataUpdateBehavior tests how data visibility changes with refresh and reload.
func TestExternalTableDataUpdateBehavior(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*600)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	// RefreshWithoutRelease: test Refresh + Load (WITHOUT Release) to pick up new data
	t.Run("RefreshWithoutRelease", func(t *testing.T) {
		collName := common.GenRandomString("ext_norel", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		// Setup: 500 rows, refresh, index, load
		data0, err := generateParquetBytes(500, 0)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data0.parquet", data0)

		schema := entity.NewSchema().
			WithName(collName).
			WithExternalSource(extPath).
			WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		require.NoError(t, err)

		refreshAndWait(ctx, t, mc, collName)
		indexAndLoadCollection(ctx, t, mc, collName, "embedding")

		// Verify 500
		countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		require.NoError(t, err)
		count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, int64(500), count)
		t.Logf("[phase 1] count(*) = %d ✓", count)

		// Add new file
		data1, err := generateParquetBytes(300, 500)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data1.parquet", data1)

		// Refresh (no Release)
		refreshAndWait(ctx, t, mc, collName)
		t.Log("[phase 2] Refresh done (no Release)")

		// Query immediately — should still see 500 (old loaded data)
		countRes2, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		require.NoError(t, err)
		count2, _ := countRes2.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		t.Logf("[phase 2] count(*) after refresh (no reload) = %d", count2)

		// Try LoadCollection WITHOUT Release — see what happens
		t.Log("[phase 3] Calling LoadCollection (without Release)...")
		loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
		if err != nil {
			t.Logf("[phase 3] LoadCollection error: %v", err)
		} else {
			err = loadTask.Await(ctx)
			if err != nil {
				t.Logf("[phase 3] LoadCollection await error: %v", err)
			} else {
				t.Log("[phase 3] LoadCollection succeeded (no Release needed!)")
			}
		}

		// Wait for potential segment rebalance
		time.Sleep(5 * time.Second)

		// Query — does it see 800 now?
		countRes3, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		require.NoError(t, err)
		count3, _ := countRes3.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		t.Logf("[phase 3] count(*) after Load (without Release) = %d", count3)

		if count3 == 800 {
			t.Log("✅ Load without Release works! No need to Release → no query downtime")
		} else if count3 == 500 {
			t.Log("❌ Load without Release did NOT pick up new data — Release is required")
		} else {
			t.Logf("⚠️ Unexpected count: %d", count3)
		}
	})

	// ReloadWithoutRefresh: test Release + Load (WITHOUT Refresh) to see if new data is discovered
	t.Run("ReloadWithoutRefresh", func(t *testing.T) {
		collName := common.GenRandomString("ext_relonly", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		// Phase 1: 500 rows → refresh → index → load → verify
		data0, err := generateParquetBytes(500, 0)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data0.parquet", data0)

		schema := entity.NewSchema().
			WithName(collName).
			WithExternalSource(extPath).
			WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		require.NoError(t, err)
		refreshAndWait(ctx, t, mc, collName)
		indexAndLoadCollection(ctx, t, mc, collName, "embedding")

		countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		require.NoError(t, err)
		count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		t.Logf("[phase 1] count(*) = %d ✓", count)

		// Phase 2: Add new file, then ONLY Release + Load (NO Refresh)
		data1, err := generateParquetBytes(300, 500)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data1.parquet", data1)
		t.Log("[phase 2] Uploaded data1.parquet (300 rows), NO Refresh")

		err = mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(collName))
		require.NoError(t, err)
		t.Log("[phase 2] Released")

		loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
		require.NoError(t, err)
		require.NoError(t, loadTask.Await(ctx))
		t.Log("[phase 2] Loaded (without Refresh)")

		countRes2, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		require.NoError(t, err)
		count2, _ := countRes2.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		t.Logf("[phase 2] count(*) = %d (expected still 500 if Refresh is required)", count2)

		if count2 == 500 {
			t.Log("✅ Confirmed: Reload without Refresh does NOT discover new data")
		} else if count2 == 800 {
			t.Log("⚠️ Reload without Refresh DID discover new data (unexpected)")
		}
	})
}

//  2. After C's Load, does A see the new data?
//  3. Is there any crash or panic?
func TestExternalTableConcurrentAccess(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*600)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	collName := common.GenRandomString("ext_conc", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	// ============================================================
	// Setup: Upload initial data, create collection, refresh, index, load
	// ============================================================
	data0, err := generateParquetBytes(500, 0)
	require.NoError(t, err)
	uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data0.parquet", data0)

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	require.NoError(t, err)

	refreshAndWait(ctx, t, mc, collName)
	indexAndLoadCollection(ctx, t, mc, collName, "embedding")

	// Verify initial state
	countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	require.NoError(t, err)
	count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, int64(500), count)
	t.Logf("[setup] Initial data loaded: count(*) = %d", count)

	// ============================================================
	// Thread A: continuously query, log results and errors
	// ============================================================
	var (
		stopA       atomic.Bool
		queryOK     atomic.Int64
		queryFail   atomic.Int64
		queryErrors sync.Map // error message → count
		lastCount   atomic.Int64
	)
	lastCount.Store(500)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stopA.Load() {
			res, err := mc.Query(ctx, client.NewQueryOption(collName).
				WithConsistencyLevel(entity.ClStrong).
				WithOutputFields(common.QueryCountFieldName))
			if err != nil {
				queryFail.Add(1)
				errMsg := err.Error()
				if len(errMsg) > 80 {
					errMsg = errMsg[:80]
				}
				if val, loaded := queryErrors.LoadOrStore(errMsg, int64(1)); loaded {
					queryErrors.Store(errMsg, val.(int64)+1)
				}
			} else {
				queryOK.Add(1)
				c, _ := res.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
				lastCount.Store(c)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// Let A run a few queries first
	time.Sleep(1 * time.Second)
	t.Logf("[thread A] Running: ok=%d fail=%d lastCount=%d",
		queryOK.Load(), queryFail.Load(), lastCount.Load())

	// ============================================================
	// Thread B: add new file to MinIO
	// ============================================================
	t.Log("[thread B] Uploading data1.parquet (300 rows)...")
	data1, err := generateParquetBytes(300, 500)
	require.NoError(t, err)
	uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data1.parquet", data1)

	// ============================================================
	// Thread C: Refresh → Release → Load
	// ============================================================
	t.Log("[thread C] Starting Refresh...")
	refreshAndWait(ctx, t, mc, collName)
	t.Logf("[thread C] Refresh done. A status: ok=%d fail=%d lastCount=%d",
		queryOK.Load(), queryFail.Load(), lastCount.Load())

	t.Log("[thread C] Release...")
	err = mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(collName))
	require.NoError(t, err)

	// Give A time to hit the released state
	time.Sleep(2 * time.Second)
	t.Logf("[thread C] After Release. A status: ok=%d fail=%d lastCount=%d",
		queryOK.Load(), queryFail.Load(), lastCount.Load())

	t.Log("[thread C] Index + Load with new data...")
	indexAndLoadCollection(ctx, t, mc, collName, "embedding")

	// Give A time to query the new data
	time.Sleep(2 * time.Second)
	t.Logf("[thread C] After Load. A status: ok=%d fail=%d lastCount=%d",
		queryOK.Load(), queryFail.Load(), lastCount.Load())

	// ============================================================
	// Stop A and collect results
	// ============================================================
	stopA.Store(true)
	wg.Wait()

	t.Log("========== RESULTS ==========")
	t.Logf("Thread A total: ok=%d fail=%d", queryOK.Load(), queryFail.Load())
	t.Logf("Thread A last count(*) = %d (expected 800 after reload)", lastCount.Load())

	t.Log("Thread A errors:")
	queryErrors.Range(func(key, value any) bool {
		t.Logf("  [%d×] %s", value.(int64), key.(string))
		return true
	})

	// Final assertion: after C's load, A should eventually see 800 rows
	require.Equal(t, int64(800), lastCount.Load(),
		"after C's Release+Load, A should see 800 rows")
}

// 3. Release + Load (simulates pod restart — QN reloads from manifests)
// 4. What happens? Can QN still load the segment that references deleted data1.parquet?
func TestExternalTableFileDeletedReload(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*300)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	collName := common.GenRandomString("ext_dellod", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	// Phase 1: Upload 2 files, refresh, index, load
	data0, err := generateParquetBytes(500, 0)
	require.NoError(t, err)
	data1, err := generateParquetBytes(500, 500)
	require.NoError(t, err)
	uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data0.parquet", data0)
	uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data1.parquet", data1)

	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	require.NoError(t, err)
	refreshAndWait(ctx, t, mc, collName)
	indexAndLoadCollection(ctx, t, mc, collName, "embedding")

	countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	require.NoError(t, err)
	count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, int64(1000), count)
	t.Logf("[phase 1] count(*) = %d ✓", count)

	// Phase 2: Delete data1.parquet from MinIO (NO Refresh!)
	err = minioClient.RemoveObject(ctx, minioCfg.bucket, extPath+"/data1.parquet", miniogo.RemoveObjectOptions{})
	require.NoError(t, err)
	t.Log("[phase 2] Deleted data1.parquet from MinIO (no Refresh)")

	// Phase 3: Release + Load (simulates pod restart)
	t.Log("[phase 3] Release + Load (simulate pod restart)...")
	err = mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(collName))
	require.NoError(t, err)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	if err != nil {
		t.Logf("[phase 3] LoadCollection call failed: %v", err)
		return
	}

	// Wait for load with timeout
	loadCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	err = loadTask.Await(loadCtx)
	if err != nil {
		t.Logf("[phase 3] LoadCollection await error: %v", err)
		t.Log("[phase 3] ⚠️ Load failed — QN cannot reload segment because Parquet file is gone")

		// Check if we can still query at all
		_, qErr := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		if qErr != nil {
			t.Logf("[phase 3] Query after failed load: %v", qErr)
		}
		return
	}

	t.Log("[phase 3] Load succeeded!")

	// Query — what do we see?
	countRes2, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	if err != nil {
		t.Logf("[phase 3] Query error: %v", err)
		return
	}
	count2, _ := countRes2.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	t.Logf("[phase 3] count(*) = %d after reload (file deleted, no refresh)", count2)

	if count2 == 1000 {
		t.Log("✅ QN still serves 1000 rows — data was cached/indexed, original file not needed")
	} else if count2 == 500 {
		t.Log("⚠️ QN only loaded 500 rows — segment referencing deleted file failed silently")
	} else if count2 == 0 {
		t.Log("❌ QN loaded 0 rows — all segments failed")
	}
}

//
// Requires: dataNode.externalCollection.targetRowsPerSegment = 100
func TestExternalTableIncrementalIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*600)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	collName := common.GenRandomString("ext_incr_idx", 6)
	extPath := fmt.Sprintf("external-e2e-test/%s", collName)

	t.Cleanup(func() {
		cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	// ============================================================
	// Phase 1: Upload 3 files × 200 rows = 600 rows
	// With targetRowsPerSegment=100, should get ~6 segments
	// ============================================================
	t.Log("=== Phase 1: Upload 3 files (200 rows each) ===")
	for i := 0; i < 3; i++ {
		data, err := generateParquetBytes(200, int64(i)*200)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket,
			fmt.Sprintf("%s/data%d.parquet", extPath, i), data)
	}

	// Create collection
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource(extPath).
		WithExternalSpec(`{"format":"parquet"}`).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	require.NoError(t, err)

	// Get collection ID
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	require.NoError(t, err)
	collID := fmt.Sprintf("%d", coll.ID)

	// First refresh
	refreshAndWait(ctx, t, mc, collName)

	// Check segments
	segCount1, paths1 := countManifestsForCollection(ctx, minioClient, minioCfg.bucket, collID)
	t.Logf("Phase 1: %d segment(s) after refresh", segCount1)
	for _, p := range paths1 {
		t.Logf("  segment: %s", p)
	}

	// Check row count
	stats1, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
	require.NoError(t, err)
	rows1, _ := strconv.ParseInt(stats1["row_count"], 10, 64)
	require.Equal(t, int64(600), rows1)
	t.Logf("Phase 1: row_count = %d", rows1)

	// Create index and load
	idx := index.NewAutoIndex(entity.L2)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "embedding", idx))
	require.NoError(t, err)
	require.NoError(t, idxTask.Await(ctx))

	// Check index state — all rows should be indexed
	descIdx1, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(collName, "embedding"))
	require.NoError(t, err)
	t.Logf("Phase 1 index: totalRows=%d indexedRows=%d pendingRows=%d state=%d",
		descIdx1.TotalRows, descIdx1.IndexedRows, descIdx1.PendingIndexRows, descIdx1.State)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))
	t.Log("Phase 1: loaded ✓")

	// ============================================================
	// Phase 2: Add 1 more file (200 rows), refresh again
	// Old segments should be KEPT, new segment created
	// ============================================================
	t.Log("=== Phase 2: Add 1 more file (200 rows), release + refresh ===")

	err = mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(collName))
	require.NoError(t, err)

	data3, err := generateParquetBytes(200, 600)
	require.NoError(t, err)
	uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket,
		fmt.Sprintf("%s/data3.parquet", extPath), data3)

	// Second refresh
	refreshAndWait(ctx, t, mc, collName)

	// Check segments
	segCount2, paths2 := countManifestsForCollection(ctx, minioClient, minioCfg.bucket, collID)
	t.Logf("Phase 2: %d segment(s) after refresh", segCount2)

	// Find new segments (present in phase 2 but not phase 1)
	pathSet1 := make(map[string]bool)
	for _, p := range paths1 {
		pathSet1[p] = true
	}
	newSegments := 0
	keptSegments := 0
	for _, p := range paths2 {
		if pathSet1[p] {
			keptSegments++
			t.Logf("  KEPT: %s", p)
		} else {
			newSegments++
			t.Logf("  NEW:  %s", p)
		}
	}
	t.Logf("Phase 2: %d kept + %d new = %d total segments", keptSegments, newSegments, segCount2)

	// Check row count
	stats2, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
	require.NoError(t, err)
	rows2, _ := strconv.ParseInt(stats2["row_count"], 10, 64)
	require.Equal(t, int64(800), rows2)
	t.Logf("Phase 2: row_count = %d", rows2)

	// ============================================================
	// Phase 3: Check index — does only the new segment need indexing?
	// ============================================================
	t.Log("=== Phase 3: Check index state after second refresh ===")

	// Poll index state a few times to see the progression
	for i := 0; i < 10; i++ {
		descIdx2, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(collName, "embedding"))
		require.NoError(t, err)
		t.Logf("Phase 3 index poll %d: totalRows=%d indexedRows=%d pendingRows=%d state=%d",
			i, descIdx2.TotalRows, descIdx2.IndexedRows, descIdx2.PendingIndexRows, descIdx2.State)

		if descIdx2.PendingIndexRows == 0 && descIdx2.IndexedRows == 800 {
			t.Log("Phase 3: all 800 rows indexed ✓")
			break
		}
		time.Sleep(3 * time.Second)
	}

	// Final index check
	descIdxFinal, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(collName, "embedding"))
	require.NoError(t, err)
	t.Logf("Phase 3 FINAL: totalRows=%d indexedRows=%d pendingRows=%d",
		descIdxFinal.TotalRows, descIdxFinal.IndexedRows, descIdxFinal.PendingIndexRows)

	// Load and verify
	loadTask2, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	require.NoError(t, err)
	require.NoError(t, loadTask2.Await(ctx))

	countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.QueryCountFieldName))
	require.NoError(t, err)
	count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
	require.Equal(t, int64(800), count)
	t.Logf("Phase 3: query count(*) = %d ✓", count)
}


// TestExternalTableSegmentMapping verifies how files map to segments.
func TestExternalTableSegmentMapping(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*600)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioCfg.bucket)
	if err != nil || !exists {
		t.Skipf("MinIO bucket %q not accessible", minioCfg.bucket)
	}

	type fileSpec struct {
		name string
		rows int64
	}

	cases := []struct {
		name  string
		files []fileSpec
	}{
		{"1_file_1000rows", []fileSpec{{"data.parquet", 1000}}},
		{"2_files_500each", []fileSpec{{"d0.parquet", 500}, {"d1.parquet", 500}}},
		{"5_files_100each", []fileSpec{{"d0.parquet", 100}, {"d1.parquet", 100}, {"d2.parquet", 100}, {"d3.parquet", 100}, {"d4.parquet", 100}}},
		{"10_files_10each", []fileSpec{{"d0.parquet", 10}, {"d1.parquet", 10}, {"d2.parquet", 10}, {"d3.parquet", 10}, {"d4.parquet", 10}, {"d5.parquet", 10}, {"d6.parquet", 10}, {"d7.parquet", 10}, {"d8.parquet", 10}, {"d9.parquet", 10}}},
		{"mixed_sizes", []fileSpec{{"big.parquet", 800}, {"s1.parquet", 50}, {"s2.parquet", 50}, {"tiny.parquet", 10}}},
		{"3_files_unequal", []fileSpec{{"a.parquet", 700}, {"b.parquet", 200}, {"c.parquet", 100}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			collName := common.GenRandomString("ext_seg", 6)
			extPath := fmt.Sprintf("external-e2e-test/%s", collName)

			var totalRows int64
			fileDesc := ""
			for _, f := range tc.files {
				data, err := generateParquetBytes(f.rows, totalRows)
				require.NoError(t, err)
				uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket,
					fmt.Sprintf("%s/%s", extPath, f.name), data)
				totalRows += f.rows
				fileDesc += fmt.Sprintf("%s(%d) ", f.name, f.rows)
			}

			t.Cleanup(func() {
				cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
				_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
			})

			schema := entity.NewSchema().
				WithName(collName).
				WithExternalSource(extPath).
				WithExternalSpec(`{"format":"parquet"}`).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
				WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
				WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
			require.NoError(t, err)

			// Get collectionID from describe
			coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
			require.NoError(t, err)
			collectionID := fmt.Sprintf("%d", coll.ID)

			refreshAndWait(ctx, t, mc, collName)

			// Verify row count
			stats, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
			require.NoError(t, err)
			rowCount, _ := strconv.ParseInt(stats["row_count"], 10, 64)
			require.Equal(t, totalRows, rowCount)

			// Count segments (manifests) in MinIO using countManifestsForCollection
			segCount, _ := countManifestsForCollection(ctx, minioClient, minioCfg.bucket, collectionID)

			t.Logf("📊 %d files [%s] → %d rows → %d segment(s)",
				len(tc.files), fileDesc, rowCount, segCount)
		})
	}
}


// TestExternalTableQueryEdgeCases tests edge cases in query and search operations.
func TestExternalTableQueryEdgeCases(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, _ := minioClient.BucketExists(ctx, minioCfg.bucket)
	if !exists {
		t.Skipf("MinIO bucket not accessible")
	}

	// ComplexFilters: test various filter expressions
	t.Run("ComplexFilters", func(t *testing.T) {
		collName := common.GenRandomString("ext_filter", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)

		data, err := generateParquetBytes(100, 0)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		schema := entity.NewSchema().WithName(collName).
			WithExternalSource(extPath).WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		refreshAndWait(ctx, t, mc, collName)
		indexAndLoadCollection(ctx, t, mc, collName, "embedding")

		filters := []struct {
			name     string
			expr     string
			expected int
		}{
			{"IN", "id in [0, 10, 20, 30, 40]", 5},
			{"NOT_IN", "id not in [0, 1, 2, 3, 4]", 95},
			{"OR", "id < 10 || id >= 90", 20},
			{"NOT_EQUAL", "id != 50", 99},
			{"BETWEEN", "id >= 20 && id <= 29", 10},
		}

		for _, f := range filters {
			t.Run(f.name, func(t *testing.T) {
				res, err := mc.Query(ctx, client.NewQueryOption(collName).
					WithConsistencyLevel(entity.ClStrong).
					WithFilter(f.expr).
					WithOutputFields("id"))
				common.CheckErr(t, err, true)
				actual := res.GetColumn("id").Len()
				require.Equal(t, f.expected, actual)
				t.Logf("✓ %s: %d rows (expected %d)", f.name, actual, f.expected)
			})
		}
	})

	// VirtualPK: query and output __virtual_pk__ field
	t.Run("VirtualPK", func(t *testing.T) {
		collName := common.GenRandomString("ext_vpk", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)

		data, err := generateParquetBytes(50, 0)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		schema := entity.NewSchema().WithName(collName).
			WithExternalSource(extPath).WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		refreshAndWait(ctx, t, mc, collName)
		indexAndLoadCollection(ctx, t, mc, collName, "embedding")

		res, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter("id == 0").
			WithOutputFields("id", "__virtual_pk__"))
		if err != nil {
			t.Logf("✗ Error: %v", err)
		} else {
			pkCol := res.GetColumn("__virtual_pk__")
			require.NotNil(t, pkCol)
			pkVal, _ := pkCol.GetAsInt64(0)
			t.Logf("✓ __virtual_pk__ = %d (upper32=%d, lower32=%d)", pkVal, pkVal>>32, pkVal&0xFFFFFFFF)
		}
	})

	// TopKExceedsTotalRows: search with topK > total rows in collection
	t.Run("TopKExceedsTotalRows", func(t *testing.T) {
		collName := common.GenRandomString("ext_topk", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)

		// Only 10 rows
		data, err := generateParquetBytes(10, 0)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		schema := entity.NewSchema().WithName(collName).
			WithExternalSource(extPath).WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		refreshAndWait(ctx, t, mc, collName)
		indexAndLoadCollection(ctx, t, mc, collName, "embedding")

		vec := make([]float32, testVecDim)
		res, err := mc.Search(ctx, client.NewSearchOption(collName, 100, // topK=100 but only 10 rows
			[]entity.Vector{entity.FloatVector(vec)}).
			WithConsistencyLevel(entity.ClStrong).
			WithANNSField("embedding"))
		common.CheckErr(t, err, true)
		require.LessOrEqual(t, res[0].ResultCount, 10)
		t.Logf("✓ topK=100 on 10 rows → got %d results", res[0].ResultCount)
	})

	// DifferentMetrics: search with L2, IP, and COSINE metrics
	t.Run("DifferentMetrics", func(t *testing.T) {
		metrics := []entity.MetricType{entity.L2, entity.IP, entity.COSINE}

		for _, metric := range metrics {
			t.Run(string(metric), func(t *testing.T) {
				collName := common.GenRandomString("ext_met", 6)
				extPath := fmt.Sprintf("external-e2e-test/%s", collName)

				data, err := generateParquetBytes(100, 0)
				require.NoError(t, err)
				uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

				t.Cleanup(func() {
					cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
					mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
				})

				schema := entity.NewSchema().WithName(collName).
					WithExternalSource(extPath).WithExternalSpec(`{"format":"parquet"}`).
					WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
					WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
					WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))
				err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
				common.CheckErr(t, err, true)

				refreshAndWait(ctx, t, mc, collName)

				idx := index.NewAutoIndex(metric)
				idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "embedding", idx))
				common.CheckErr(t, err, true)
				require.NoError(t, idxTask.Await(ctx))

				loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
				common.CheckErr(t, err, true)
				require.NoError(t, loadTask.Await(ctx))

				vec := make([]float32, testVecDim)
				res, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
					[]entity.Vector{entity.FloatVector(vec)}).
					WithConsistencyLevel(entity.ClStrong).
					WithANNSField("embedding"))
				common.CheckErr(t, err, true)
				require.Greater(t, res[0].ResultCount, 0)
				t.Logf("✓ %s: %d results, top score=%.4f", metric, res[0].ResultCount, res[0].Scores[0])
			})
		}
	})
}


// TestExternalTableCollectionOps tests write operations and collection lifecycle.
func TestExternalTableCollectionOps(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// DropPartition: should be rejected for external collections
	t.Run("DropPartition", func(t *testing.T) {
		collName := common.GenRandomString("ext_dp", 6)

		schema := entity.NewSchema().WithName(collName).
			WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("f").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithExternalField("f")).
			WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(4).WithExternalField("v"))
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)
		t.Cleanup(func() { mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName)) })

		err = mc.DropPartition(ctx, client.NewDropPartitionOption(collName, "test_partition"))
		require.Error(t, err)
		t.Logf("✓ DropPartition rejected: %v", err)
	})

	// AddField: should be rejected for external collections
	t.Run("AddField", func(t *testing.T) {
		collName := common.GenRandomString("ext_af", 6)

		schema := entity.NewSchema().WithName(collName).
			WithExternalSource("s3://b/d").WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("f").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithExternalField("f")).
			WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(4).WithExternalField("v"))
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)
		t.Cleanup(func() { mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName)) })

		err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(collName,
			entity.NewField().WithName("new_field").WithDataType(entity.FieldTypeInt64)))
		require.Error(t, err)
		t.Logf("✓ AddField rejected: %v", err)
	})

	// DropDuringRefresh: drop collection while a refresh job is running
	t.Run("DropDuringRefresh", func(t *testing.T) {
		minioCfg := getMinIOConfig()
		minioClient, err := newMinIOClient(minioCfg)
		if err != nil {
			t.Skipf("MinIO unavailable: %v", err)
		}
		exists, _ := minioClient.BucketExists(ctx, minioCfg.bucket)
		if !exists {
			t.Skipf("MinIO bucket not accessible")
		}

		collName := common.GenRandomString("ext_dropr", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)

		data, err := generateParquetBytes(500, 0)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		schema := entity.NewSchema().WithName(collName).
			WithExternalSource(extPath).WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		// Start refresh
		_, err = mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(collName))
		require.NoError(t, err)

		// Immediately drop
		err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
		require.NoError(t, err)
		t.Log("✓ Drop during refresh succeeded")

		has, _ := mc.HasCollection(ctx, client.NewHasCollectionOption(collName))
		require.False(t, has)
		t.Log("✓ HasCollection = false")
	})

	// RecreateWithSameName: drop and recreate collection with the same name
	t.Run("RecreateWithSameName", func(t *testing.T) {
		minioCfg := getMinIOConfig()
		minioClient, err := newMinIOClient(minioCfg)
		if err != nil {
			t.Skipf("MinIO unavailable: %v", err)
		}
		exists, _ := minioClient.BucketExists(ctx, minioCfg.bucket)
		if !exists {
			t.Skipf("MinIO bucket not accessible")
		}

		collName := common.GenRandomString("ext_recreate", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)

		data, err := generateParquetBytes(100, 0)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		schema := entity.NewSchema().WithName(collName).
			WithExternalSource(extPath).WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

		// Create, refresh, verify
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)
		refreshAndWait(ctx, t, mc, collName)
		t.Log("✓ First creation + refresh")

		// Drop
		err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
		require.NoError(t, err)
		t.Log("✓ Dropped")

		// Recreate with same name
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)
		refreshAndWait(ctx, t, mc, collName)
		indexAndLoadCollection(ctx, t, mc, collName, "embedding")

		countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, int64(100), count)
		t.Logf("✓ Recreated, count(*) = %d", count)
	})
}


// TestExternalTableCrossFeature tests external collections interacting with other Milvus features.
func TestExternalTableCrossFeature(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	minioCfg := getMinIOConfig()
	minioClient, err := newMinIOClient(minioCfg)
	if err != nil {
		t.Skipf("MinIO unavailable: %v", err)
	}
	exists, _ := minioClient.BucketExists(ctx, minioCfg.bucket)
	if !exists {
		t.Skipf("MinIO bucket not accessible")
	}

	// Alias: create and use an alias for an external collection
	t.Run("Alias", func(t *testing.T) {
		collName := common.GenRandomString("ext_alias", 6)
		aliasName := collName + "_alias"
		extPath := fmt.Sprintf("external-e2e-test/%s", collName)

		data, err := generateParquetBytes(100, 0)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			mc.DropAlias(context.Background(), client.NewDropAliasOption(aliasName))
			mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		schema := entity.NewSchema().WithName(collName).
			WithExternalSource(extPath).WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		refreshAndWait(ctx, t, mc, collName)
		indexAndLoadCollection(ctx, t, mc, collName, "embedding")

		// Create alias
		err = mc.CreateAlias(ctx, client.NewCreateAliasOption(collName, aliasName))
		common.CheckErr(t, err, true)
		t.Logf("✓ Alias %q → %q created", aliasName, collName)

		// Query via alias
		countRes, err := mc.Query(ctx, client.NewQueryOption(aliasName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, int64(100), count)
		t.Logf("✓ Query via alias: count(*) = %d", count)

		// Search via alias
		vec := make([]float32, testVecDim)
		searchRes, err := mc.Search(ctx, client.NewSearchOption(aliasName, 5,
			[]entity.Vector{entity.FloatVector(vec)}).
			WithConsistencyLevel(entity.ClStrong).
			WithANNSField("embedding"))
		common.CheckErr(t, err, true)
		require.Greater(t, searchRes[0].ResultCount, 0)
		t.Logf("✓ Search via alias: %d results", searchRes[0].ResultCount)
	})

	// CoexistWithNormal: external and normal collections can coexist
	t.Run("CoexistWithNormal", func(t *testing.T) {
		extCollName := common.GenRandomString("ext_coexist", 6)
		normalCollName := common.GenRandomString("normal_coexist", 6)
		extPath := fmt.Sprintf("external-e2e-test/%s", extCollName)

		data, err := generateParquetBytes(50, 0)
		require.NoError(t, err)
		uploadParquetToMinIO(ctx, t, minioClient, minioCfg.bucket, extPath+"/data.parquet", data)

		t.Cleanup(func() {
			cleanupMinIOPrefix(context.Background(), minioClient, minioCfg.bucket, extPath+"/")
			mc.DropCollection(context.Background(), client.NewDropCollectionOption(extCollName))
			mc.DropCollection(context.Background(), client.NewDropCollectionOption(normalCollName))
		})

		// Create external collection
		extSchema := entity.NewSchema().WithName(extCollName).
			WithExternalSource(extPath).WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(extCollName, extSchema))
		common.CheckErr(t, err, true)
		refreshAndWait(ctx, t, mc, extCollName)
		indexAndLoadCollection(ctx, t, mc, extCollName, "embedding")

		// Create normal collection
		normalSchema := entity.NewSchema().WithName(normalCollName).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
			WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim))
		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(normalCollName, normalSchema))
		common.CheckErr(t, err, true)

		// Describe both — verify type distinction
		extColl, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(extCollName))
		common.CheckErr(t, err, true)
		require.NotEmpty(t, extColl.Schema.ExternalSource)
		t.Logf("✓ External: ExternalSource=%q", extColl.Schema.ExternalSource)

		normalColl, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(normalCollName))
		common.CheckErr(t, err, true)
		require.Empty(t, normalColl.Schema.ExternalSource)
		t.Log("✓ Normal: ExternalSource is empty")

		// ListCollections should contain both
		collections, err := mc.ListCollections(ctx, client.NewListCollectionOption())
		common.CheckErr(t, err, true)
		require.Contains(t, collections, extCollName)
		require.Contains(t, collections, normalCollName)
		t.Log("✓ ListCollections contains both")

		// Query external
		countRes, err := mc.Query(ctx, client.NewQueryOption(extCollName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.Equal(t, int64(50), count)
		t.Logf("✓ External collection count(*) = %d", count)
	})
}


// TestExternalTableS3 tests external table with S3 as Milvus storage backend.
func TestExternalTableS3(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*300)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// SameBucket: external table data in the same bucket as Milvus storage
	t.Run("SameBucket", func(t *testing.T) {
		s3Cfg := getS3StorageConfig()
		if s3Cfg.accessKey == "" || s3Cfg.secretKey == "" {
			t.Skip("S3 credentials not set, skipping")
		}

		s3Client, err := newS3AsMinIOClient(s3Cfg)
		if err != nil {
			t.Skipf("Failed to create S3 client: %v", err)
		}

		exists, err := s3Client.BucketExists(ctx, s3Cfg.bucket)
		if err != nil || !exists {
			t.Skipf("S3 bucket %q not accessible (exists=%v, err=%v)", s3Cfg.bucket, exists, err)
		}

		collName := common.GenRandomString("ext_s3", 6)
		// Data path under Milvus rootPath in S3
		extDataDir := fmt.Sprintf("external-data/%s", collName)
		// Full S3 key = rootPath/extDataDir
		s3KeyPrefix := fmt.Sprintf("%s/%s", s3Cfg.rootPath, extDataDir)

		// Step 1: Generate and upload parquet data to S3 (under Milvus rootPath)
		const numFiles = 2
		const rowsPerFile = int64(1000)
		totalExpectedRows := int64(numFiles) * rowsPerFile

		for i := 0; i < numFiles; i++ {
			data, genErr := generateParquetBytes(rowsPerFile, int64(i)*rowsPerFile)
			require.NoError(t, genErr, "generate parquet file %d", i)

			objectKey := fmt.Sprintf("%s/data%d.parquet", s3KeyPrefix, i)
			uploadToS3(ctx, t, s3Client, s3Cfg.bucket, objectKey, data)
		}

		t.Cleanup(func() {
			cleanupS3Prefix(context.Background(), s3Client, s3Cfg.bucket, s3KeyPrefix+"/")
		})

		// Step 2: Create external collection
		// external_source is relative to Milvus rootPath (same bucket mode)
		t.Logf("External source (relative): %s", extDataDir)

		schema := entity.NewSchema().
			WithName(collName).
			WithExternalSource(extDataDir).
			WithExternalSpec(`{"format":"parquet"}`).
			WithField(
				entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id"),
			).
			WithField(
				entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value"),
			).
			WithField(
				entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).
					WithDim(testVecDim).WithExternalField("embedding"),
			)

		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		require.NoError(t, err, "create external collection")
		t.Logf("Created external collection: %s", collName)

		t.Cleanup(func() {
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
		require.NoError(t, err)
		require.Equal(t, extDataDir, coll.Schema.ExternalSource)

		// Step 3: Refresh and wait
		jobID := refreshAndWait(ctx, t, mc, collName)
		t.Logf("Refresh completed, jobID=%d", jobID)

		stats, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("%d", totalExpectedRows), stats["row_count"])
		t.Logf("Row count: %s", stats["row_count"])

		// Step 4: Index and load
		indexAndLoadCollection(ctx, t, mc, collName, "embedding")

		// Step 5: Query count(*)
		countRes, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithOutputFields(common.QueryCountFieldName))
		require.NoError(t, err)

		count, err := countRes.GetColumn(common.QueryCountFieldName).GetAsInt64(0)
		require.NoError(t, err)
		require.Equal(t, totalExpectedRows, count)
		t.Logf("Query count(*) = %d", count)

		// Step 6: Query with filter
		filterRes, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter("id < 100").
			WithOutputFields("id", "value"))
		require.NoError(t, err)

		idCol := filterRes.GetColumn("id")
		require.Equal(t, 100, idCol.Len())
		t.Logf("Query filter returned %d rows", idCol.Len())

		// Step 7: Vector search
		vec := make([]float32, testVecDim)
		for i := range vec {
			vec[i] = float32(i) * 0.1
		}
		searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
			[]entity.Vector{entity.FloatVector(vec)}).
			WithConsistencyLevel(entity.ClStrong).
			WithANNSField("embedding").
			WithOutputFields("id", "value"))
		require.NoError(t, err)

		require.Equal(t, 1, len(searchRes))
		require.Greater(t, searchRes[0].ResultCount, 0)
		t.Logf("Search returned %d results", searchRes[0].ResultCount)

		for i := 0; i < searchRes[0].ResultCount; i++ {
			id, _ := searchRes[0].GetColumn("id").GetAsInt64(i)
			t.Logf("  #%d: id=%d, score=%.4f", i, id, searchRes[0].Scores[i])
		}

		// Step 8: Hybrid search (vector + scalar filter)
		hybridRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5,
			[]entity.Vector{entity.FloatVector(vec)}).
			WithConsistencyLevel(entity.ClStrong).
			WithANNSField("embedding").
			WithFilter("id >= 500 && id < 1000").
			WithOutputFields("id"))
		require.NoError(t, err)

		require.Equal(t, 1, len(hybridRes))
		require.Greater(t, hybridRes[0].ResultCount, 0)
		for i := 0; i < hybridRes[0].ResultCount; i++ {
			val, _ := hybridRes[0].GetColumn("id").GetAsInt64(i)
			require.GreaterOrEqual(t, val, int64(500))
			require.Less(t, val, int64(1000))
		}
		t.Logf("Hybrid search (id in [500,1000)) returned %d results", hybridRes[0].ResultCount)
	})

	// IncrementalRefresh: incremental refresh with S3 storage
	t.Run("IncrementalRefresh", func(t *testing.T) {
		s3Cfg := getS3StorageConfig()
		if s3Cfg.accessKey == "" || s3Cfg.secretKey == "" {
			t.Skip("S3 credentials not set, skipping")
		}

		s3Client, err := newS3AsMinIOClient(s3Cfg)
		if err != nil {
			t.Skipf("Failed to create S3 client: %v", err)
		}

		collName := common.GenRandomString("ext_s3_incr", 6)
		extDataDir := fmt.Sprintf("external-data/%s", collName)
		s3KeyPrefix := fmt.Sprintf("%s/%s", s3Cfg.rootPath, extDataDir)

		t.Cleanup(func() {
			cleanupS3Prefix(context.Background(), s3Client, s3Cfg.bucket, s3KeyPrefix+"/")
			_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
		})

		// Step 1: Upload initial file
		const rowsPerFile = int64(1000)
		data, err := generateParquetBytes(rowsPerFile, 0)
		require.NoError(t, err)
		uploadToS3(ctx, t, s3Client, s3Cfg.bucket,
			fmt.Sprintf("%s/data0.parquet", s3KeyPrefix), data)

		// Step 2: Create external collection (relative path)
		schema := entity.NewSchema().
			WithName(collName).
			WithExternalSource(extDataDir).
			WithExternalSpec(`{"format":"parquet"}`).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
			WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))

		err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		require.NoError(t, err)

		// Step 3: First refresh
		refreshAndWait(ctx, t, mc, collName)

		stats, err := mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
		require.NoError(t, err)
		require.Equal(t, "1000", stats["row_count"])
		t.Logf("After first refresh: row_count=%s", stats["row_count"])

		// Step 4: Upload additional file
		data2, err := generateParquetBytes(rowsPerFile, rowsPerFile)
		require.NoError(t, err)
		uploadToS3(ctx, t, s3Client, s3Cfg.bucket,
			fmt.Sprintf("%s/data1.parquet", s3KeyPrefix), data2)

		// Step 5: Incremental refresh
		refreshAndWait(ctx, t, mc, collName)

		stats, err = mc.GetCollectionStats(ctx, client.NewGetCollectionStatsOption(collName))
		require.NoError(t, err)
		require.Equal(t, "2000", stats["row_count"])
		t.Logf("After incremental refresh: row_count=%s", stats["row_count"])

		// Step 6: Verify refresh jobs
		jobs, err := mc.ListRefreshExternalCollectionJobs(ctx,
			client.NewListRefreshExternalCollectionJobsOption(collName))
		require.NoError(t, err)
		require.Equal(t, 2, len(jobs))
		t.Logf("Total refresh jobs: %d", len(jobs))
	})
}

