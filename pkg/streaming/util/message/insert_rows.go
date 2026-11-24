package message

import (
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// normalizeInsertOrUpsertBody expands compact wire fields in a newly decoded
// body. Execution sees logical RowIDs and explicit delete keys, while historical
// Insert and all other message formats retain their existing decoding behavior.
func normalizeInsertOrUpsertBody(body proto.Message) error {
	var rows *messagespb.InsertRows
	switch source := body.(type) {
	case *messagespb.InsertMessageBody:
		rows = source.GetRows()
	case *messagespb.UpsertMessageBody:
		rows = source.GetRows()
		for _, effect := range source.GetDeleteEffects() {
			if _, ok := effect.GetKeySource().(*messagespb.UpsertDeleteEffect_AllInsertedKeys); ok {
				effect.KeySource = &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: rows.GetPrimaryKeys()}
			}
		}
	default:
		return nil
	}
	delta := rows.GetDeltaRowIds()
	if delta == nil {
		return nil
	}
	ids := make([]int64, len(delta.Deltas)+1)
	ids[0] = delta.Base
	for index, step := range delta.Deltas {
		previous := ids[index]
		if step > 0 && previous > math.MaxInt64-step || step < 0 && previous < math.MinInt64-step {
			return merr.WrapErrServiceInternalMsg("delta RowID reconstruction overflows int64")
		}
		ids[index+1] = previous + step
	}
	rows.RowIdsEncoding = &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: ids}}
	return nil
}
