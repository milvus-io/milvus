package recovery

import (
	"context"
	"maps"
	"slices"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// alignCollectionSchema replaces the latest retained normal schema of the
// coordinator's version once when upgrading recovery metadata. Other versions
// remain available for segment recovery and WAL replay.
func (r *recoveryStorageImpl) alignCollectionSchema(ctx context.Context, vchannels map[string]*streamingpb.VChannelMeta) error {
	if r.checkpoint.Magic == utility.RecoveryMagicSchemaMetadataAligned || r.checkpoint.Magic == utility.RecoveryMagicHotWALSwitch {
		return nil
	}
	aligned := make(map[string]*streamingpb.VChannelMeta)
	collections := make(map[int64]*schemapb.CollectionSchema)
	for name, meta := range vchannels {
		if meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
			continue
		}
		id := meta.GetCollectionInfo().GetCollectionId()
		metadata, ok := collections[id]
		if !ok {
			var err error
			metadata, err = r.describeCollectionSchema(ctx, id)
			if err != nil {
				if !errors.Is(err, merr.ErrCollectionNotFound) {
					return merr.Wrapf(err, "align collection %d schema", id)
				}
				// A dropped collection can still own retained WAL data. Keep its
				// encoding schemas until replay applies its DropCollection.
				r.Logger().Info(ctx, "collection absent during schema metadata alignment", mlog.Int64("collectionID", id))
			}
			collections[id] = metadata
		}
		if metadata == nil {
			continue
		}
		index := -1
		for i := len(meta.CollectionInfo.Schemas) - 1; i >= 0; i-- {
			entry := meta.CollectionInfo.Schemas[i]
			if entry.GetState() == streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL && entry.GetSchema().GetVersion() == metadata.GetVersion() {
				index = i
				break
			}
		}
		if index < 0 {
			continue
		}
		updated := proto.Clone(meta).(*streamingpb.VChannelMeta)
		matched := updated.CollectionInfo.Schemas[index]
		matched.Schema = proto.Clone(metadata).(*schemapb.CollectionSchema)
		matched.Schema.Properties = slices.DeleteFunc(matched.Schema.Properties, func(property *commonpb.KeyValuePair) bool {
			return property.GetKey() == common.CollectionOnTruncatingKey
		})
		aligned[name] = updated
	}
	checkpoint := r.checkpoint.Clone()
	checkpoint.Magic = utility.RecoveryMagicSchemaMetadataAligned
	// SaveRecoverySnapshot writes schema keys before the base and the magic
	// last. An interrupted migration retains the old magic and repeats safely.
	if err := resource.Resource().StreamingNodeCatalog().SaveRecoverySnapshot(ctx, r.channel.Name, &metastore.WALRecoverySnapshot{
		VChannels: aligned, ConsumeCheckpoint: checkpoint.IntoProto(),
	}); err != nil {
		return merr.Wrap(err, "persist collection schema alignment")
	}
	maps.Copy(vchannels, aligned)
	r.installCheckpoint(checkpoint)
	r.Logger().Info(ctx, "aligned collection schema", mlog.Int("vchannels", len(aligned)), mlog.Int64("magic", checkpoint.Magic))
	return nil
}

func (r *recoveryStorageImpl) describeCollectionSchema(ctx context.Context, collectionID int64) (*schemapb.CollectionSchema, error) {
	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := coord.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
		Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection, SourceID: paramtable.GetNodeID()},
		CollectionID: collectionID,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}
	if resp.GetSchema() == nil {
		return nil, merr.WrapErrServiceInternalMsg("collection %d describe response has no schema", collectionID)
	}
	return resp.GetSchema(), nil
}
