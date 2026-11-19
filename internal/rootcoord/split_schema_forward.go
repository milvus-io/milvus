package rootcoord

import (
	"context"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// This runs inside the original ALTER callback on each cluster, before waiting
// for target-schema views. It appends locally to sources absent from ALTER's
// replicated broadcast set; no nested broadcast/resource lock is acquired.
func forwardSchemaToSplitSources(ctx context.Context, resources *qvresource.Resources, published *model.Collection, schema *schemapb.CollectionSchema) error {
	if len(published.SplitLineages) == 0 {
		return nil
	}
	if resources == nil || resources.SplitSources == nil {
		return merr.WrapErrServiceNotReadyMsg("split source lifecycle is not ready for schema forwarding")
	}
	sources, err := resources.SplitSources(ctx, published.CollectionID)
	if err != nil {
		return merr.Wrap(err, "resolve split sources for schema forwarding")
	}
	for _, source := range sources {
		if !slices.ContainsFunc(published.SplitLineages, func(lineage *model.ShardSplitLineage) bool {
			return lineage.SourceVChannel == source
		}) {
			return merr.WrapErrDataIntegrityMsg("schema forwarding source %s has no lineage in collection %d", source, published.CollectionID)
		}
		msg := message.NewSchemaChangeMessageBuilderV2().WithVChannel(source).WithUnreplicable().
			WithHeader(&message.SchemaChangeMessageHeader{CollectionId: published.CollectionID}).
			WithBody(&message.SchemaChangeMessageBody{Schema: schema}).MustBuildMutable()
		if _, err := streaming.WAL().RawAppend(ctx, msg); err != nil {
			return merr.Wrapf(err, "forward schema version %d to split source %s", schema.GetVersion(), source)
		}
	}
	return nil
}
