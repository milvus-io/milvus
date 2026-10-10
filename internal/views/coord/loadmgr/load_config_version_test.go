package loadmgr

import (
	"context"
	"slices"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestLoadInfoVersionCanonicalPersistedIdentity(t *testing.T) {
	cfg := sampleConfig()
	cfg.LoadFields = append(cfg.LoadFields, &messagespb.LoadFieldConfig{FieldId: 201, IndexId: 301})
	original := cfg.Clone()
	version := loadInfoVersion(cfg)
	require.Equal(t, original, cfg, "identity computation must not mutate a published config")
	reordered := cfg.Clone()
	slices.Reverse(reordered.PartitionIDs)
	slices.Reverse(reordered.LoadFields)
	slices.Reverse(reordered.Replicas)
	require.Equal(t, version, loadInfoVersion(reordered))
	reordered.Replicas[0].Priority = commonpb.LoadPriority_LOW
	require.Equal(t, version, loadInfoVersion(reordered), "unpersisted scheduling priority is not load identity")
	for name, change := range map[string]func(*LoadConfig){
		"index":          func(c *LoadConfig) { c.LoadFields[0].IndexId++ },
		"field":          func(c *LoadConfig) { c.LoadFields[0].FieldId++ },
		"partition":      func(c *LoadConfig) { c.PartitionIDs[0]++ },
		"replica":        func(c *LoadConfig) { c.Replicas[0].ReplicaID++ },
		"resource group": func(c *LoadConfig) { c.Replicas[0].ResourceGroup = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			next := cfg.Clone()
			change(next)
			require.NotEqual(t, version, loadInfoVersion(next))
		})
	}
}

func TestLoadInfoVersionSurvivesCatalogRecoveryAndDoesNotAlias(t *testing.T) {
	var collection *querypb.CollectionLoadInfo
	var partitions []*querypb.PartitionLoadInfo
	var replicas []*querypb.Replica
	patches := []*mockey.Mocker{
		mockey.Mock((*querycoord.Catalog).GetCollections).To(func(*querycoord.Catalog, context.Context) ([]*querypb.CollectionLoadInfo, error) {
			if collection == nil {
				return nil, nil
			}
			return []*querypb.CollectionLoadInfo{collection}, nil
		}).Build(),
		mockey.Mock((*querycoord.Catalog).GetPartitions).To(func(*querycoord.Catalog, context.Context, []int64) (map[int64][]*querypb.PartitionLoadInfo, error) {
			return map[int64][]*querypb.PartitionLoadInfo{100: partitions}, nil
		}).Build(),
		mockey.Mock((*querycoord.Catalog).GetReplicas).To(func(*querycoord.Catalog, context.Context) ([]*querypb.Replica, error) { return replicas, nil }).Build(),
		mockey.Mock((*querycoord.Catalog).SaveCollection).To(func(_ *querycoord.Catalog, _ context.Context, c *querypb.CollectionLoadInfo, parts ...*querypb.PartitionLoadInfo) error {
			collection = proto.Clone(c).(*querypb.CollectionLoadInfo)
			partitions = nil
			for _, p := range parts {
				partitions = append(partitions, proto.Clone(p).(*querypb.PartitionLoadInfo))
			}
			return nil
		}).Build(),
		mockey.Mock((*querycoord.Catalog).SaveReplica).To(func(_ *querycoord.Catalog, _ context.Context, values ...*querypb.Replica) error {
			replicas = nil
			for _, r := range values {
				replicas = append(replicas, proto.Clone(r).(*querypb.Replica))
			}
			return nil
		}).Build(),
	}
	defer func() {
		for _, patch := range patches {
			patch.UnPatch()
		}
	}()
	catalog := querycoord.NewCatalog(nil)
	store, err := RecoverLoadConfigStore(t.Context(), catalog)
	require.NoError(t, err)
	cfg := sampleConfig()
	require.NoError(t, store.Put(t.Context(), cfg))
	oldVersion := store.Snapshot().ConfigVersion(cfg.CollectionID)
	recovered, err := RecoverLoadConfigStore(t.Context(), catalog)
	require.NoError(t, err)
	require.Equal(t, oldVersion, recovered.Snapshot().ConfigVersion(cfg.CollectionID))
	cfg.LoadFields[0].IndexId++ // First Put after restart must not reuse the old identity.
	require.NoError(t, recovered.Put(t.Context(), cfg))
	newVersion := recovered.Snapshot().ConfigVersion(cfg.CollectionID)
	require.NotEqual(t, oldVersion, newVersion)
	var replayed uint64
	stop := recovered.RegisterLoadConfigListener(func(_ int64, _ *LoadConfig, v uint64) { replayed = v })
	defer stop()
	require.Equal(t, newVersion, replayed)
	require.NoError(t, recovered.Put(t.Context(), cfg))
	require.Equal(t, newVersion, replayed, "equivalent writes keep the identity")
	again, err := RecoverLoadConfigStore(t.Context(), catalog)
	require.NoError(t, err)
	require.Equal(t, newVersion, again.Snapshot().ConfigVersion(cfg.CollectionID))
}
