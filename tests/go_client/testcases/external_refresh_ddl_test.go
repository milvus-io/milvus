package testcases

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// Reproduces #55380's ordering with real flushed segments requiring a BM25
// schema-bump task, a Refresh source override and a database-exclusive alias DDL.
func TestRefreshDDLCallbackConcurrentSchemaChange(t *testing.T) {
	ctx := hp.CreateContext(t, 5*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	cfg := getMinIOConfig()
	storage, err := newMinIOClient(cfg)
	require.NoError(t, err)
	external := common.GenRandomString("refresh_ddl", 6)
	prefix := "external-e2e-test/" + external
	for i, dir := range []string{"a", "b"} {
		data, err := generateParquetBytes(externalDataSchemaBasic, 20, int64(i*1000), testVecDim)
		require.NoError(t, err)
		_, err = storage.PutObject(ctx, cfg.bucket, prefix+"/"+dir+"/data.parquet", bytes.NewReader(data), int64(len(data)), miniogo.PutObjectOptions{})
		require.NoError(t, err)
	}
	t.Cleanup(func() { cleanupMinIOPrefix(context.Background(), storage, cfg.bucket, prefix+"/") })
	schema := entity.NewSchema().WithName(external).WithExternalSource(extTestURI(cfg, prefix+"/a")).WithExternalSpec(extTestSpec(cfg, "parquet")).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithExternalField("id")).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeFloat).WithExternalField("value")).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim).WithExternalField("embedding"))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(external, schema)))
	t.Cleanup(func() { _ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(external)) })
	first, err := mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(external))
	require.NoError(t, err)
	waitRefreshTerminal(t, ctx, mc, first.JobID, entity.RefreshStateCompleted)
	indexAndLoadCollection(ctx, t, mc, external, "embedding")
	for round := 0; round < 3; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			holder := common.GenRandomString("schema_holder", 6)
			holderSchema := entity.NewSchema().WithName(holder).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
				WithField(entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).WithMaxLength(1024).WithEnableAnalyzer(true)).
				WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(testVecDim))
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(holder, holderSchema)))
			t.Cleanup(func() { _ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(holder)) })
			_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(holder).WithColumns(
				column.NewColumnInt64("id", []int64{1, 2}), column.NewColumnVarChar("text", []string{"one tiny", "two tiny"}),
				column.NewColumnFloatVector("embedding", testVecDim, [][]float32{make([]float32, testVecDim), make([]float32, testVecDim)})))
			require.NoError(t, err)
			flush, err := mc.Flush(ctx, client.NewFlushOption(holder))
			require.NoError(t, err)
			require.NoError(t, flush.Await(ctx))
			indexAndLoadCollection(ctx, t, mc, holder, "embedding")
			done := make(chan error, 3)
			go func() {
				fn := entity.NewFunction().WithName("bm25").WithType(entity.FunctionTypeBM25).WithInputFields("text").WithOutputFields("sparse")
				done <- mc.AddFunctionField(ctx, client.NewAddFunctionFieldOption(holder, entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector), fn, index.NewSparseInvertedIndex(entity.BM25, 0.2)))
			}()
			time.Sleep(50 * time.Millisecond)
			jobIDs := make(chan int64, 1)
			dir := "b"
			if round == 1 {
				dir = "a"
			}
			source := extTestURI(cfg, prefix+"/"+dir)
			go func() {
				result, err := mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(external).WithExternalSource(source).WithExternalSpec(extTestSpec(cfg, "parquet")))
				if err == nil {
					jobIDs <- result.JobID
				}
				done <- err
			}()
			time.Sleep(60 * time.Millisecond)
			alias := common.GenRandomString("refresh_alias", 6)
			go func() { done <- mc.CreateAlias(ctx, client.NewCreateAliasOption(holder, alias)) }()
			t.Cleanup(func() { _ = mc.DropAlias(context.Background(), client.NewDropAliasOption(alias)) })
			for i := 0; i < 3; i++ {
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(120 * time.Second):
					t.Fatal("schema DDL/Refresh/alias failed to make progress")
				}
			}
			waitRefreshTerminal(t, ctx, mc, <-jobIDs, entity.RefreshStateCompleted)
			desc, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(external))
			require.NoError(t, err)
			require.Equal(t, source, desc.Schema.ExternalSource)
			desc, err = mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(holder))
			require.NoError(t, err)
			require.Len(t, desc.Schema.Functions, 1)
		})
	}
}
