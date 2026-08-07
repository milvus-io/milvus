package partialupdate

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// latestCollectionSchemaVersion asks ShardManager for its current PK descriptor
// when an insert header does not carry an explicit version.
const latestCollectionSchemaVersion int32 = -1

// extractPKs extracts row-content primary keys from WAL delete messages.
// ok=false means callers must handle inserts and collection-wide writes separately.
func extractPKs(msg message.MutableMessage) ([]any, bool, error) {
	if msg == nil {
		return nil, false, nil
	}

	switch msg.MessageType() {
	case message.MessageTypeDelete:
		deleteMsg, err := message.AsMutableDeleteMessageV1(msg)
		if err != nil {
			return nil, true, status.NewUnrecoverableError("decode partial update delete message failed: %v", err)
		}
		body, err := deleteMsg.Body()
		if err != nil {
			return nil, true, status.NewUnrecoverableError("decode partial update delete body failed: %v", err)
		}
		pks, err := pksFromIDs(body.GetPrimaryKeys())
		return pks, true, err
	default:
		return nil, false, nil
	}
}

// extractPKsFromInsert extracts the PK column identified by fieldID from one
// partial-update insert chunk.
func extractPKsFromInsert(msg message.MutableMessage, fieldID int64) ([]any, error) {
	if msg == nil || fieldID <= 0 {
		return nil, status.NewUnrecoverableError("partial update insert primary key field id is invalid")
	}
	insertMsg, err := message.AsMutableInsertMessageV1(msg)
	if err != nil {
		return nil, status.NewUnrecoverableError("decode partial update insert message failed: %v", err)
	}
	body, err := insertMsg.Body()
	if err != nil {
		return nil, status.NewUnrecoverableError("decode partial update insert body failed: %v", err)
	}
	return extractPKsFromFieldData(body.GetFieldsData(), fieldID)
}

// extractPKsFromOrdinaryInsert returns exact PKs when schema is available, or
// a collection fence ID for a schema-less legacy insert accepted by shard.
func extractPKsFromOrdinaryInsert(msg message.MutableMessage, descriptorGetter primaryKeyDescriptorGetter) (pks []any, fenceCollectionID int64, err error) {
	if descriptorGetter == nil {
		return nil, 0, status.NewUnrecoverableError("partial update primary key descriptor getter is unavailable")
	}
	insertMsg, err := message.AsMutableInsertMessageV1(msg)
	if err != nil {
		return nil, 0, status.NewUnrecoverableError("decode insert message for partial update tracking failed: %v", err)
	}
	header := insertMsg.Header()
	schemaVersion := latestCollectionSchemaVersion
	if header.SchemaVersion != nil {
		schemaVersion = header.GetSchemaVersion()
	}
	descriptor, err := descriptorGetter.GetPrimaryKeyDescriptor(header.GetCollectionId(), schemaVersion)
	if err != nil {
		if errors.Is(err, shards.ErrCollectionSchemaVersionNotMatch) {
			return nil, 0, status.NewSchemaVersionMismatch(
				"schema version mismatch while tracking partial update writes, collection: %d, schema version: %d",
				header.GetCollectionId(), header.GetSchemaVersion())
		}
		// Shard accepts schema-less legacy inserts during rolling upgrades.
		// Fence the collection when their exact PK field cannot be recovered.
		if header.SchemaVersion == nil && errors.Is(err, shards.ErrCollectionSchemaNotFound) {
			if header.GetCollectionId() == 0 {
				return nil, 0, status.NewUnrecoverableError("partial update ordinary insert collection id is empty")
			}
			return nil, header.GetCollectionId(), nil
		}
		return nil, 0, status.NewUnrecoverableError("get primary key descriptor for partial update tracking failed: %v", err)
	}
	if !typeutil.IsPrimaryFieldType(descriptor.DataType) {
		return nil, 0, status.NewUnrecoverableError(
			"partial update primary key field %d has unsupported data type %s",
			descriptor.FieldID,
			descriptor.DataType.String(),
		)
	}
	pks, err = extractPKsFromInsert(msg, descriptor.FieldID)
	return pks, 0, err
}

func extractPKsFromFieldData(fields []*schemapb.FieldData, fieldID int64) ([]any, error) {
	for _, field := range fields {
		if field.GetFieldId() != fieldID {
			continue
		}
		scalars := field.GetScalars()
		switch scalars.GetData().(type) {
		case *schemapb.ScalarField_LongData:
			return pksFromIDs(&schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: scalars.GetLongData()},
			})
		case *schemapb.ScalarField_StringData:
			return pksFromIDs(&schemapb.IDs{
				IdField: &schemapb.IDs_StrId{StrId: scalars.GetStringData()},
			})
		default:
			return nil, status.NewUnrecoverableError("partial update insert primary key field %d must be int64 or varchar", fieldID)
		}
	}
	return nil, status.NewUnrecoverableError("partial update insert primary key field %d is missing", fieldID)
}

// extractCollectionFenceID extracts collections affected by wide data mutations.
func extractCollectionFenceID(msg message.MutableMessage) (int64, bool) {
	if msg == nil {
		return 0, false
	}

	switch msg.MessageType() {
	case message.MessageTypeTruncateCollection:
		truncateMsg, err := message.AsMutableTruncateCollectionMessageV2(msg)
		if err != nil {
			return 0, true
		}
		collectionID := truncateMsg.Header().GetCollectionId()
		if collectionID == 0 {
			return 0, true
		}
		return collectionID, true
	default:
		return 0, false
	}
}

// extractDropCollectionID identifies the collection whose fence can be
// discarded after DropCollection is durably appended.
func extractDropCollectionID(msg message.MutableMessage) (int64, bool) {
	if msg == nil || msg.MessageType() != message.MessageTypeDropCollection {
		return 0, false
	}
	dropMsg, err := message.AsMutableDropCollectionMessageV1(msg)
	if err != nil {
		return 0, true
	}
	return dropMsg.Header().GetCollectionId(), true
}

func pksFromIDs(ids *schemapb.IDs) ([]any, error) {
	if ids == nil {
		return nil, status.NewUnrecoverableError("partial update primary keys are nil")
	}
	switch values := ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		if values == nil || values.IntId == nil {
			return nil, status.NewUnrecoverableError("partial update int64 primary keys are nil")
		}
	case *schemapb.IDs_StrId:
		if values == nil || values.StrId == nil {
			return nil, status.NewUnrecoverableError("partial update varchar primary keys are nil")
		}
	default:
		return nil, status.NewUnrecoverableError("unsupported partial update primary key ids type %T", values)
	}

	size := typeutil.GetSizeOfIDs(ids)
	if size == 0 {
		return nil, status.NewUnrecoverableError("partial update primary keys are empty")
	}
	pks := make([]any, size)
	for idx := range pks {
		pk := typeutil.GetPK(ids, int64(idx))
		if pk == nil {
			return nil, status.NewUnrecoverableError("partial update primary key at offset %d is empty", idx)
		}
		pks[idx] = pk
	}
	return pks, nil
}
