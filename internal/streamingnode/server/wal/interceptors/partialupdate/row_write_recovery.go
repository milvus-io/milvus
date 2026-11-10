package partialupdate

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// recoverRowWriteBody restores the pending write set from a durable new-format
// body. It does not enable appending that format through the 3.0 writer path.
// RowIDs and ordinary columns are irrelevant to lock/version tracking.
func (i *appendInterceptor) recoverRowWriteBody(ctx context.Context, txnID message.TxnID, msg message.ImmutableMessage) error {
	var collectionID int64
	var schemaVersion int32
	var rows *messagespb.InsertRows
	var effects []*messagespb.UpsertDeleteEffect
	var proof *messagespb.PartialUpdateCAS
	payload, err := message.DecodePayload(ctx, msg)
	if err != nil {
		return decodePayloadError("recovered row write", err)
	}
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		typed, err := message.AsImmutableInsertMessageV2(msg)
		if err != nil {
			return err
		}
		collectionID, schemaVersion = typed.Header().GetCollectionId(), typed.Header().GetSchemaVersion()
		body := &messagespb.InsertMessageBody{}
		if err := proto.Unmarshal(payload, body); err != nil {
			return err
		}
		rows = body.GetRows()
	case message.MessageTypeUpsert:
		typed, err := message.AsImmutableUpsertMessageV2(msg)
		if err != nil {
			return err
		}
		collectionID, schemaVersion = typed.Header().GetCollectionId(), typed.Header().GetSchemaVersion()
		body := &messagespb.UpsertMessageBody{}
		if err := proto.Unmarshal(payload, body); err != nil {
			return err
		}
		rows, effects, proof = body.GetRows(), body.GetDeleteEffects(), body.GetPartialUpdateCas()
	default:
		panic("unexpected recovered row-write message")
	}
	descriptor, err := primaryKeyDescriptorOf(i.pkDescriptorGetter, collectionID)
	if err != nil {
		return err
	}
	keys, err := primaryKeysFromIDs(rows.GetPrimaryKeys())
	if err != nil {
		return err
	}
	if err := validatePrimaryKeysScalarType(keys, descriptor.DataType); err != nil {
		return err
	}
	keys = keys.clone()
	for _, effect := range effects {
		ids := effect.GetPrimaryKeys()
		if _, referenced := effect.GetKeySource().(*messagespb.UpsertDeleteEffect_AllInsertedKeys); referenced {
			ids = rows.GetPrimaryKeys()
		}
		deleted, err := primaryKeysFromIDs(ids)
		if err != nil {
			return err
		}
		if err := validatePrimaryKeysScalarType(deleted, descriptor.DataType); err != nil {
			return err
		}
		keys.append(deleted)
	}
	if message.HasPartialUpdateCAS(msg) {
		if proof == nil {
			return status.NewUnrecoverableError("recovered row-write CAS proof is missing")
		}
		encoded, err := message.EncodeProto(proof)
		if err != nil {
			return err
		}
		meta, err := message.DecodePartialUpdateCASMetadata(encoded)
		if err != nil {
			return err
		}
		if err := i.state.recordTxnCAS(txnID, meta, casInsertScope{collectionID: collectionID, schemaVersion: schemaVersion}); err != nil {
			return err
		}
	}
	i.state.recordTxnWritesTyped(txnID, keys)
	return nil
}
