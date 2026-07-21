package testcases

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func setupFileResourceObject(t *testing.T, ctx context.Context, fileName, content string) (minioConfig, string) {
	t.Helper()

	cfg := getMinIOConfig()
	minioClient, err := newMinIOClient(cfg)
	require.NoError(t, err)

	exists, err := minioClient.BucketExists(ctx, cfg.bucket)
	require.NoError(t, err)
	require.True(t, exists, "MinIO bucket %q must exist", cfg.bucket)

	prefix := fmt.Sprintf("file-resource-e2e/%s", common.GenRandomString("go", 8))
	path := fmt.Sprintf("%s/%s", prefix, fileName)
	data := []byte(content)
	_, err = minioClient.PutObject(ctx, cfg.bucket, path, bytes.NewReader(data), int64(len(data)), miniogo.PutObjectOptions{
		ContentType: "text/plain; charset=utf-8",
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cleanupMinIOPrefix(cleanupCtx, minioClient, cfg.bucket, prefix+"/")
	})
	return cfg, path
}

func removeFileResourceEventually(ctx context.Context, mc *base.MilvusClient, name string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := mc.RemoveFileResource(ctx, client.NewRemoveFileResourceOption(name))
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func remoteSynonymSchema(collectionName, resourceName string) *entity.Schema {
	analyzerParams := map[string]any{
		"tokenizer": "standard",
		"filter": []any{
			map[string]any{
				"type":   "synonym",
				"expand": true,
				"synonyms_file": map[string]any{
					"type":          "remote",
					"resource_name": resourceName,
					"file_name":     "synonyms.txt",
				},
			},
		},
	}

	return entity.NewSchema().
		WithName(collectionName).
		WithAutoID(true).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).
			WithIsPrimaryKey(true).WithIsAutoID(true)).
		WithField(entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(1024).WithEnableAnalyzer(true).WithAnalyzerParams(analyzerParams)).
		WithField(entity.NewField().WithName("sparse_vector").WithDataType(entity.FieldTypeSparseVector)).
		WithFunction(entity.NewFunction().WithName("bm25").WithType(entity.FunctionTypeBM25).
			WithInputFields("text").WithOutputFields("sparse_vector"))
}

func createFileResourceMilvusClient(ctx context.Context, t *testing.T) *base.MilvusClient {
	t.Helper()
	return hp.CreateMilvusClient(ctx, t, &client.ClientConfig{
		Address:  hp.GetAddr(),
		Username: hp.GetUser(),
		Password: hp.GetPassword(),
	})
}

func TestFileResourceCRUDAndValidation(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createFileResourceMilvusClient(ctx, t)
	_, path := setupFileResourceObject(t, ctx, "synonyms.txt", "search, retrieval, query\n")
	_, otherPath := setupFileResourceObject(t, ctx, "synonyms.txt", "milvus, vector-database\n")
	name := common.GenRandomString("go_file_resource", 8)

	require.NoError(t, mc.AddFileResource(ctx, client.NewAddFileResourceOption(name, path)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = removeFileResourceEventually(cleanupCtx, mc, name)
	})

	resources, err := mc.ListFileResources(ctx, client.NewListFileResourcesOption())
	require.NoError(t, err)
	var found bool
	for _, resource := range resources {
		if resource.Name == name {
			found = true
			require.Positive(t, resource.ID)
			require.Equal(t, path, resource.Path)
		}
	}
	require.True(t, found, "registered resource %q must be returned by list", name)

	// Identical add and repeated remove are idempotent.
	require.NoError(t, mc.AddFileResource(ctx, client.NewAddFileResourceOption(name, path)))
	err = mc.AddFileResource(ctx, client.NewAddFileResourceOption(name, otherPath))
	require.ErrorContains(t, err, "already exists")

	err = mc.AddFileResource(ctx, client.NewAddFileResourceOption(common.GenRandomString("missing", 8), path+".missing"))
	require.ErrorContains(t, err, "path not exist")
	err = mc.AddFileResource(ctx, client.NewAddFileResourceOption(common.GenRandomString("empty_path", 8), ""))
	require.Error(t, err)

	require.NoError(t, mc.RemoveFileResource(ctx, client.NewRemoveFileResourceOption(name)))
	require.NoError(t, mc.RemoveFileResource(ctx, client.NewRemoveFileResourceOption(name)))
}

func TestFileResourceEmptyNameRejected(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createFileResourceMilvusClient(ctx, t)
	assertMissingParameter := func(err error) {
		require.Error(t, err)
		require.Equal(t, int32(1101), client.ErrorCode(err))
		require.ErrorContains(t, err, "missing parameter")
	}

	err := mc.AddFileResource(ctx, client.NewAddFileResourceOption("", "unused.txt"))
	assertMissingParameter(err)

	err = mc.RemoveFileResource(ctx, client.NewRemoveFileResourceOption(""))
	assertMissingParameter(err)
}

func TestFileResourceRemoteAnalyzerAndCollectionLifecycle(t *testing.T) {
	ctx := hp.CreateContext(t, 2*time.Minute)
	mc := createFileResourceMilvusClient(ctx, t)
	_, path := setupFileResourceObject(t, ctx, "synonyms.txt", "search, retrieval, query\n")
	resourceName := common.GenRandomString("go_remote_analyzer", 8)
	collectionName := common.GenRandomString("go_remote_analyzer", 8)

	require.NoError(t, mc.AddFileResource(ctx, client.NewAddFileResourceOption(resourceName, path)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = removeFileResourceEventually(cleanupCtx, mc, resourceName)
	})

	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName,
		remoteSynonymSchema(collectionName, resourceName))))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName))
	})

	indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, "sparse_vector",
		index.NewSparseInvertedIndex(entity.BM25, 0.1)))
	require.NoError(t, err)
	require.NoError(t, indexTask.Await(ctx))
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))

	results, err := mc.RunAnalyzer(ctx, client.NewRunAnalyzerOption("search").WithField(collectionName, "text"))
	require.NoError(t, err)
	require.Len(t, results, 1)
	actualTokens := make([]string, 0, len(results[0].Tokens))
	for _, token := range results[0].Tokens {
		actualTokens = append(actualTokens, token.Text)
	}
	require.ElementsMatch(t, []string{"search", "retrieval", "query"}, actualTokens)

	err = mc.RemoveFileResource(ctx, client.NewRemoveFileResourceOption(resourceName))
	require.ErrorContains(t, err, "is still in use")

	require.NoError(t, mc.DropCollection(ctx, client.NewDropCollectionOption(collectionName)))
	require.NoError(t, removeFileResourceEventually(ctx, mc, resourceName))
}

func TestFileResourceReferenceReleasedAcrossDatabaseLifecycle(t *testing.T) {
	ctx := hp.CreateContext(t, 2*time.Minute)
	mc := createFileResourceMilvusClient(ctx, t)
	_, path := setupFileResourceObject(t, ctx, "synonyms.txt", "search, retrieval, query\n")
	resourceName := common.GenRandomString("go_file_resource_db", 8)
	databaseName := common.GenRandomString("go_file_resource_db", 8)
	collectionName := common.GenRandomString("go_file_resource_db", 8)

	require.NoError(t, mc.AddFileResource(ctx, client.NewAddFileResourceOption(resourceName, path)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = removeFileResourceEventually(cleanupCtx, mc, resourceName)
	})

	require.NoError(t, mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(databaseName)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = mc.UseDatabase(cleanupCtx, client.NewUseDatabaseOption(common.DefaultDb))
		_ = mc.DropDatabase(cleanupCtx, client.NewDropDatabaseOption(databaseName))
	})
	require.NoError(t, mc.UseDatabase(ctx, client.NewUseDatabaseOption(databaseName)))

	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName,
		remoteSynonymSchema(collectionName, resourceName))))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = mc.UseDatabase(cleanupCtx, client.NewUseDatabaseOption(databaseName))
		_ = mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName))
		_ = mc.UseDatabase(cleanupCtx, client.NewUseDatabaseOption(common.DefaultDb))
	})

	err := mc.RemoveFileResource(ctx, client.NewRemoveFileResourceOption(resourceName))
	require.ErrorContains(t, err, "is still in use")

	require.NoError(t, mc.DropCollection(ctx, client.NewDropCollectionOption(collectionName)))
	require.NoError(t, mc.UseDatabase(ctx, client.NewUseDatabaseOption(common.DefaultDb)))
	require.NoError(t, mc.DropDatabase(ctx, client.NewDropDatabaseOption(databaseName)))
	require.NoError(t, removeFileResourceEventually(ctx, mc, resourceName))
}
