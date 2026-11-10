package testcases

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestExternalTableAzureWorkloadIdentityE2E exercises both native Parquet reads
// and Iceberg planning through the shared C++ filesystem. The Milvus server,
// not the test process, must have Azure workload identity configured.
// See ../README_azure_workload_identity.md for the read-only fixture contract.
func TestExternalTableAzureWorkloadIdentityE2E(t *testing.T) {
	if os.Getenv("AZURE_WORKLOAD_IDENTITY_TEST_ENABLED") != "true" {
		t.Skip("set AZURE_WORKLOAD_IDENTITY_TEST_ENABLED=true and configure Azure fixtures")
	}
	requiredEnv := func(name string) string {
		t.Helper()
		value := os.Getenv(name)
		require.NotEmpty(t, value, "%s is required when the Azure E2E test is enabled", name)
		return value
	}
	account := requiredEnv("AZURE_STORAGE_ACCOUNT")
	parquetSource := requiredEnv("AZURE_PARQUET_SOURCE")
	icebergSource := requiredEnv("AZURE_ICEBERG_SOURCE")
	rows, err := strconv.ParseInt(requiredEnv("AZURE_FIXTURE_ROWS"), 10, 64)
	require.NoError(t, err)
	require.Positive(t, rows)

	for _, fixture := range []struct {
		format string
		source string
	}{
		{format: "parquet", source: parquetSource},
		{format: "iceberg-table", source: icebergSource},
	} {
		t.Run(fixture.format, func(t *testing.T) {
			ctx := hp.CreateContext(t, 15*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t)
			name := common.GenRandomString("azure_wi_", 8)
			extfs := map[string]string{
				"cloud_provider": "azure",
				"use_iam":        "true",
				"access_key_id":  account,
			}
			spec := func(fields map[string]string) string {
				t.Helper()
				data, err := json.Marshal(map[string]interface{}{"format": fixture.format, "extfs": fields})
				require.NoError(t, err)
				return string(data)
			}
			schema := func(externalSpec string) *entity.Schema {
				return entity.NewSchema().WithName(name).
					WithExternalSource(fixture.source).WithExternalSpec(externalSpec).
					WithField(entity.NewField().WithName("pk").WithDataType(entity.FieldTypeInt64).WithExternalField("pk")).
					WithField(entity.NewField().WithName("label").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithExternalField("label")).
					WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4).WithExternalField("vector"))
			}
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)); err != nil {
					t.Logf("collection cleanup: %v", err)
				}
			})

			// These must fail at validation, before native filesystem construction.
			delete(extfs, "access_key_id")
			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema(spec(extfs))))
			require.ErrorContains(t, err, "storage account name")
			extfs["access_key_id"] = account
			extfs["access_key_value"] = "not-a-real-key"
			err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema(spec(extfs))))
			require.ErrorContains(t, err, "credential modes are mutually exclusive")
			delete(extfs, "access_key_value")

			externalSpec := spec(extfs)
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema(externalSpec))))
			refresh, err := mc.RefreshExternalCollection(ctx,
				client.NewRefreshExternalCollectionOption(name).WithExternalSpec(externalSpec))
			require.NoError(t, err)
			waitForRefreshComplete(ctx, t, mc, refresh.JobID, 5*time.Minute)

			build, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.COSINE)))
			require.NoError(t, err)
			require.NoError(t, build.Await(ctx))
			load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
			require.NoError(t, err)
			require.NoError(t, load.Await(ctx))

			checkReads := func() {
				t.Helper()
				countResult, err := mc.Query(ctx, client.NewQueryOption(name).
					WithConsistencyLevel(entity.ClStrong).WithOutputFields("count(*)"))
				require.NoError(t, err)
				count, err := countResult.GetColumn("count(*)").GetAsInt64(0)
				require.NoError(t, err)
				require.Equal(t, rows, count)
				result, err := mc.Query(ctx, client.NewQueryOption(name).
					WithConsistencyLevel(entity.ClStrong).
					WithFilter("pk == 0").WithOutputFields("pk", "label", "vector"))
				require.NoError(t, err)
				require.Equal(t, 1, result.ResultCount)
				label, err := result.GetColumn("label").GetAsString(0)
				require.NoError(t, err)
				require.Equal(t, "azure-workload-identity", label)
				vector, err := result.GetColumn("vector").Get(0)
				require.NoError(t, err)
				require.Equal(t, []float32{1, 0, 0, 0}, vector)
				search, err := mc.Search(ctx, client.NewSearchOption(name, 1,
					[]entity.Vector{entity.FloatVector{1, 0, 0, 0}}).
					WithConsistencyLevel(entity.ClStrong).
					WithANNSField("vector").WithFilter("pk == 0").WithOutputFields("pk", "label"))
				require.NoError(t, err)
				require.Len(t, search, 1)
				require.Equal(t, 1, search[0].ResultCount)
				// External tables return a synthesized virtual primary key in IDs;
				// verify the source column through the requested output fields.
				pk, err := search[0].GetColumn("pk").GetAsInt64(0)
				require.NoError(t, err)
				require.Equal(t, int64(0), pk)
			}
			checkReads()

			// Omit the spec on the second refresh to exercise persisted credentials.
			// This tests spec reuse, not OAuth token expiry or rotation.
			refresh, err = mc.RefreshExternalCollection(ctx, client.NewRefreshExternalCollectionOption(name))
			require.NoError(t, err)
			waitForRefreshComplete(ctx, t, mc, refresh.JobID, 5*time.Minute)
			checkReads()
			t.Logf("Azure %s create/refresh/index/load/query/search completed (%d rows)", fixture.format, rows)
		})
	}
}
