package testcases

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestCreateExternalCollection tests creating an external collection
func TestCreateExternalCollection(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("external", 6)

	// Create schema with external source
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource("s3://test-bucket/data/").
		WithExternalSpec(`{"format": "parquet","extfs":{"access_key_id":"dummy","access_key_value":"dummy","region":"us-east-1","cloud_provider":"aws"}}`).
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

	// Create collection
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
	var spec struct {
		Format string            `json:"format"`
		Extfs  map[string]string `json:"extfs"`
	}
	require.NoError(t, json.Unmarshal([]byte(coll.Schema.ExternalSpec), &spec))
	require.Equal(t, "parquet", spec.Format)
	require.Equal(t, "aws", spec.Extfs["cloud_provider"])
	require.Equal(t, "us-east-1", spec.Extfs["region"])
	require.Equal(t, "***", spec.Extfs["access_key_id"])
	require.Equal(t, "***", spec.Extfs["access_key_value"])

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
}

// TestCreateExternalCollectionMissingExternalField tests that creating external collection
// without external_field mapping fails
func TestCreateExternalCollectionMissingExternalField(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("external", 6)

	// Create schema with external source but missing external_field
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource("s3://test-bucket/data/").
		WithExternalSpec(`{"format": "parquet","extfs":{"access_key_id":"dummy","access_key_value":"dummy","region":"us-east-1","cloud_provider":"aws"}}`).
		WithField(
			entity.NewField().
				WithName("text").
				WithDataType(entity.FieldTypeVarChar).
				WithMaxLength(256),
			// Missing WithExternalField()
		).
		WithField(
			entity.NewField().
				WithName("embedding").
				WithDataType(entity.FieldTypeFloatVector).
				WithDim(common.DefaultDim).
				WithExternalField("embedding_column"),
		)

	// Create collection should fail
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "must have external_field mapping")
}

// TestCreateExternalCollectionWithPrimaryKey tests that creating external collection
// with primary key field fails
func TestCreateExternalCollectionWithPrimaryKey(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("external", 6)

	// Create schema with external source and primary key (not allowed)
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource("s3://test-bucket/data/").
		WithExternalSpec(`{"format": "parquet","extfs":{"access_key_id":"dummy","access_key_value":"dummy","region":"us-east-1","cloud_provider":"aws"}}`).
		WithField(
			entity.NewField().
				WithName("id").
				WithDataType(entity.FieldTypeInt64).
				WithIsPrimaryKey(true).
				WithExternalField("id_column"),
		).
		WithField(
			entity.NewField().
				WithName("embedding").
				WithDataType(entity.FieldTypeFloatVector).
				WithDim(common.DefaultDim).
				WithExternalField("embedding_column"),
		)

	// Create collection should fail - external collections don't support primary key
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "does not support user-defined primary key")
}

// TestCreateExternalCollectionWithDynamicField tests that creating external collection
// with dynamic field enabled fails
func TestCreateExternalCollectionWithDynamicField(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("external", 6)

	// Create schema with external source and dynamic field (not allowed)
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource("s3://test-bucket/data/").
		WithExternalSpec(`{"format": "parquet","extfs":{"access_key_id":"dummy","access_key_value":"dummy","region":"us-east-1","cloud_provider":"aws"}}`).
		WithDynamicFieldEnabled(true).
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

	// Create collection should fail - external collections don't support dynamic field
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "does not support dynamic field")
}

// TestCreateExternalCollectionWithAutoID tests that creating external collection
// with auto ID field fails
func TestCreateExternalCollectionWithAutoID(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("external", 6)

	// Create schema with external source and auto ID (not allowed)
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource("s3://test-bucket/data/").
		WithExternalSpec(`{"format": "parquet","extfs":{"access_key_id":"dummy","access_key_value":"dummy","region":"us-east-1","cloud_provider":"aws"}}`).
		WithField(
			entity.NewField().
				WithName("id").
				WithDataType(entity.FieldTypeInt64).
				WithIsAutoID(true).
				WithExternalField("id_column"),
		).
		WithField(
			entity.NewField().
				WithName("embedding").
				WithDataType(entity.FieldTypeFloatVector).
				WithDim(common.DefaultDim).
				WithExternalField("embedding_column"),
		)

	// Create collection should fail - external collections don't support auto ID
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "does not support auto id")
}

// TestCreateExternalCollectionWithPartitionKey tests that creating external collection
// with partition key field fails
func TestCreateExternalCollectionWithPartitionKey(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("external", 6)

	// Create schema with external source and partition key (not allowed)
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource("s3://test-bucket/data/").
		WithExternalSpec(`{"format": "parquet","extfs":{"access_key_id":"dummy","access_key_value":"dummy","region":"us-east-1","cloud_provider":"aws"}}`).
		WithField(
			entity.NewField().
				WithName("category").
				WithDataType(entity.FieldTypeVarChar).
				WithMaxLength(256).
				WithIsPartitionKey(true).
				WithExternalField("category_column"),
		).
		WithField(
			entity.NewField().
				WithName("embedding").
				WithDataType(entity.FieldTypeFloatVector).
				WithDim(common.DefaultDim).
				WithExternalField("embedding_column"),
		)

	// Create collection should fail - external collections don't support partition key
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "does not support partition key")
}

// TestCreateExternalCollectionMultipleVectorFields tests creating external collection
// with multiple vector fields
func TestCreateExternalCollectionMultipleVectorFields(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("external", 6)

	// Create schema with external source and multiple vector fields
	schema := entity.NewSchema().
		WithName(collName).
		WithExternalSource("s3://test-bucket/data/").
		WithExternalSpec(`{"format": "parquet","extfs":{"access_key_id":"dummy","access_key_value":"dummy","region":"us-east-1","cloud_provider":"aws"}}`).
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

	// Create collection
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
}

// findFieldByName finds a field by name in the fields slice
func findFieldByName(fields []*entity.Field, name string) *entity.Field {
	for _, f := range fields {
		if f.Name == name {
			return f
		}
	}
	return nil
}
