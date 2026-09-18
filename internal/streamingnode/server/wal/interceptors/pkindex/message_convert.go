package pkindex

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func toPKs(keys primarykey.Keys) []authority.PK {
	pks := make([]authority.PK, 0, keys.Len())
	for _, v := range keys.Int64Values {
		pks = append(pks, authority.Int64PK(v))
	}
	for _, v := range keys.StringValues {
		pks = append(pks, authority.VarCharPK(v))
	}
	return pks
}

func toIDs(pks []authority.PK) *schemapb.IDs {
	if len(pks) == 0 {
		return &schemapb.IDs{}
	}
	if pks[0].IsVarChar() {
		values := make([]string, 0, len(pks))
		for _, pk := range pks {
			values = append(values, pk.VarChar())
		}
		return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: values}}}
	}
	values := make([]int64, 0, len(pks))
	for _, pk := range pks {
		values = append(values, pk.Int64())
	}
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: values}}}
}

// newCompanionDelete builds the delete that removes the old rows of pks.
// It covers all partitions: the old row may live in any of them. The time tick
// and the row timestamps are assigned downstream and at consume time.
func newCompanionDelete(vchannel string, t *target, pks []authority.PK) (message.MutableMessage, error) {
	return message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: t.collectionID,
			Rows:         uint64(len(pks)),
		}).
		WithBody(&msgpb.DeleteRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Delete),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ShardName:    vchannel,
			CollectionID: t.collectionID,
			PartitionID:  common.AllPartitionsID,
			PrimaryKeys:  toIDs(pks),
			NumRows:      int64(len(pks)),
			// The consumer rewrites one timestamp per row, so the length must match.
			Timestamps: make([]uint64, len(pks)),
		}).
		WithCipher(t.cipher.Load()).
		BuildMutable()
}
