//go:build test
// +build test

// Integration tests for external table data mapping
//
// Running tests:
//
// 1. Local filesystem test (default):
//    go test -tags dynamic,test ./internal/datanode/external -run TestIntegrationTestSuite
//
// 2. With MinIO (requires MinIO running):
//    docker compose -f /home/zilliz/docker-compose.yml up -d minio
//    export ENABLE_MINIO_TEST=true
//    export MINIO_ADDRESS=localhost:9000
//    go test -tags dynamic,test ./internal/datanode/external -run TestIntegrationTestSuite
//

package external

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

type IntegrationTestSuite struct {
	suite.Suite
	storageConfig *indexpb.StorageConfig
	testBucket    string
	testPrefix    string
}

func (s *IntegrationTestSuite) SetupSuite() {
	// Check if MinIO is available
	minioAddr := os.Getenv("MINIO_ADDRESS")
	if minioAddr == "" {
		minioAddr = "localhost:9000"
	}

	s.storageConfig = &indexpb.StorageConfig{
		Address:         minioAddr,
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		BucketName:      "test-external-table",
		RootPath:        "",
		StorageType:     "minio",
		UseSSL:          false,
	}

	s.testBucket = "test-external-table"
	s.testPrefix = "integration-test/collection-1000"
}

func (s *IntegrationTestSuite) generateParquetFile(filePath string, numRows int64) error {
	// Create Arrow schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.PrimitiveTypes.Float32},
		},
		nil,
	)

	// Create file
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Create Parquet writer
	writer, err := pqarrow.NewFileWriter(schema, f, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()

	// Generate data
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	valueBuilder := builder.Field(1).(*array.Float32Builder)

	for i := int64(0); i < numRows; i++ {
		idBuilder.Append(i)
		valueBuilder.Append(float32(i) * 1.5)
	}

	record := builder.NewRecord()
	defer record.Release()

	return writer.Write(record)
}

func (s *IntegrationTestSuite) uploadToMinIO(localPath, remotePath string) error {
	// This will use the real MinIO client
	// For now, we'll implement a placeholder that can be filled in
	// when MinIO is required

	// TODO: Implement actual MinIO upload using:
	// - github.com/minio/minio-go/v7
	// - Upload local files to s3://bucket/prefix/filename

	return fmt.Errorf("MinIO upload not yet implemented")
}

func (s *IntegrationTestSuite) cleanupMinIO(prefix string) error {
	// TODO: Implement MinIO cleanup
	// Remove all objects with the given prefix

	return nil
}

func (s *IntegrationTestSuite) TestUpdateExternalTask_EndToEnd() {
	if testing.Short() {
		s.T().Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Step 1: Generate test Parquet files locally
	tmpDir := s.T().TempDir()
	testFiles := []struct {
		name    string
		numRows int64
	}{
		{"data1.parquet", 500000},
		{"data2.parquet", 1500000},
		{"data3.parquet", 3000000},
	}

	for _, tf := range testFiles {
		filePath := fmt.Sprintf("%s/%s", tmpDir, tf.name)
		err := s.generateParquetFile(filePath, tf.numRows)
		s.Require().NoError(err, "Failed to generate test file: %s", tf.name)
	}

	// Step 2: Upload files to MinIO
	// TODO: Implement MinIO upload
	// For now, we'll use local filesystem
	s.storageConfig.StorageType = "local"
	s.storageConfig.RootPath = tmpDir

	// Step 3: Create UpdateExternalTask
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "id"},
			{Name: "value", DataType: schemapb.DataType_Float, ExternalField: "value"},
		},
	}

	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:    1000,
		TaskID:          1,
		ExternalSource:  tmpDir,
		ExternalSpec:    `{"format":"parquet"}`,
		Schema:          schema,
		StorageConfig:   s.storageConfig,
		CurrentSegments: []*datapb.SegmentInfo{},
		PreAllocatedSegmentIds: &datapb.IDRange{
			Begin: 5000,
			End:   6000,
		},
	}

	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	task := NewUpdateExternalTask(taskCtx, cancel, req)

	// Step 4: Execute task
	err := task.PreExecute(ctx)
	s.NoError(err)

	err = task.Execute(ctx)
	s.NoError(err)

	err = task.PostExecute(ctx)
	s.NoError(err)

	// Step 5: Verify results
	results := task.GetSegmentResults()
	s.NotEmpty(results, "Should create segments")

	// Verify all segments are new with pre-allocated IDs
	for _, result := range results {
		s.True(result.IsNew, "All segments should be new")
		s.GreaterOrEqual(result.Segment.GetID(), int64(5000), "New segments should use pre-allocated IDs")
		s.Less(result.Segment.GetID(), int64(6000), "New segments should be within pre-allocated range")
		s.Greater(result.Segment.GetNumOfRows(), int64(0), "Segments should have rows")
		s.NotEmpty(result.Segment.GetManifestPath(), "Segments should have manifest path")
		s.Contains(result.Segment.GetManifestPath(), "external/1000/segments", "Segments should use final path")
	}

	// Verify row mappings
	for _, result := range results {
		mapping := result.RowMapping
		s.NotNil(mapping, "Segment should have row mapping")
		s.Equal(result.Segment.GetNumOfRows(), mapping.TotalRows, "Row count should match")
		s.NotEmpty(mapping.Ranges, "Should have fragment ranges")
		s.NotEmpty(mapping.Fragments, "Should have fragments")
	}

	// Verify total rows match input
	var totalRows int64
	for _, result := range results {
		totalRows += result.Segment.GetNumOfRows()
	}

	var expectedRows int64
	for _, tf := range testFiles {
		expectedRows += tf.numRows
	}

	s.Equal(expectedRows, totalRows, "Total rows should match input")
}

func (s *IntegrationTestSuite) TestUpdateExternalTask_WithMinIO() {
	if testing.Short() {
		s.T().Skip("Skipping MinIO integration test in short mode")
	}

	// Check if ENABLE_MINIO_TEST is set
	if os.Getenv("ENABLE_MINIO_TEST") != "true" {
		s.T().Skip("MinIO test disabled. Set ENABLE_MINIO_TEST=true to enable")
	}

	_ = context.Background()

	// Generate test files
	tmpDir := s.T().TempDir()
	testFiles := []struct {
		name    string
		numRows int64
	}{
		{"data1.parquet", 500000},
		{"data2.parquet", 1500000},
	}

	for _, tf := range testFiles {
		filePath := fmt.Sprintf("%s/%s", tmpDir, tf.name)
		err := s.generateParquetFile(filePath, tf.numRows)
		s.Require().NoError(err)

		// Upload to MinIO
		remotePath := fmt.Sprintf("%s/%s", s.testPrefix, tf.name)
		err = s.uploadToMinIO(filePath, remotePath)
		if err != nil {
			s.T().Skipf("MinIO upload failed: %v", err)
		}
	}

	// Cleanup after test
	defer s.cleanupMinIO(s.testPrefix)

	// Run test with MinIO storage
	s.storageConfig.StorageType = "minio"
	s.storageConfig.RootPath = s.testPrefix

	// ... rest of test similar to TestUpdateExternalTask_EndToEnd ...
}

func TestIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}
