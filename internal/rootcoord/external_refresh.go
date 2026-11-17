package rootcoord

import (
	"context"
	"math"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// PrepareExternalRefreshSchema reads the authoritative schema under the
// collection guard. Omitted overrides preserve its existing schema version.
func (c *Core) PrepareExternalRefreshSchema(ctx context.Context, id int64, source, spec string) (*schemapb.CollectionSchema, error) {
	coll, err := c.meta.GetCollectionByIDWithMaxTs(ctx, id)
	if err != nil {
		return nil, err
	}
	schema := coll.ToCollectionSchemaPB()
	if !coll.Available() || !typeutil.IsExternalCollection(schema) {
		return nil, merr.WrapErrCollectionIllegalSchema(coll.Name, "refresh requires an external collection")
	}
	if (source == "") != (spec == "") {
		return nil, merr.WrapErrParameterInvalidMsg("external_source and external_spec must be provided together")
	}
	if source != "" {
		if schema.Version == math.MaxInt32 {
			return nil, merr.WrapErrServiceInternalMsg("schema version exhausted")
		}
		schema.Version++
		schema.ExternalSource = source
		schema.ExternalSpec = spec
	}
	return schema, nil
}

// PublishExternalRefreshSchema uses the original Refresh's fences and guard.
// DataCoord has already published the matching data/schema snapshot atomically.
func (c *Core) PublishExternalRefreshSchema(ctx context.Context, result message.BroadcastResultRefreshExternalCollectionMessageV2) error {
	id := result.Message.Header().GetCollectionId()
	channels := make([]string, 0, len(result.Results))
	for channel := range result.Results {
		channels = append(channels, channel)
	}
	adapter := message.NewAlterCollectionMessageBuilderV2().WithHeader(&message.AlterCollectionMessageHeader{CollectionId: id, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}}}).WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: result.Message.MustBody().GetSchema()}}).WithBroadcast(channels).MustBuildBroadcast()
	callback := &DDLCallback{Core: c}
	if err := callback.prepareAndPublishCollectionSchema(ctx, message.BroadcastResultAlterCollectionMessageV2{Message: message.MustAsBroadcastAlterCollectionMessageV2(adapter), Results: result.Results}); err != nil {
		return err
	}
	coll, err := c.meta.GetCollectionByIDWithMaxTs(ctx, id)
	if err != nil {
		return err
	}
	expirations, err := c.getCacheExpireForCollection(ctx, coll.DBName, coll.Name)
	if err != nil {
		return err
	}
	return callback.ExpireCaches(ctx, expirations)
}
