package partialupdate

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// latestCollectionSchemaVersion asks ShardManager for the PK descriptor of the
// current schema without comparing it with the version an insert carries.
// Milvus cannot alter the primary key field, so every schema version of a
// collection has the same PK field; the shard interceptor, not PK extraction,
// rejects a schema version mismatch on a live append. Recovered transaction
// bodies are not re-validated (see the design doc).
const latestCollectionSchemaVersion int32 = -1

type casInsertScope struct {
	collectionID  int64
	schemaVersion int32
}

// extractPKsFromDelete extracts row-content primary keys from WAL delete messages.
// ok=false means callers must handle inserts and collection-wide writes separately.
func extractPKsFromDelete(msg message.MutableMessage) ([]any, bool, error) {
	keys, ok, err := extractPKsFromDeleteWithContext(context.Background(), msg)
	if !ok {
		return nil, false, err
	}
	return keys.toAny(), true, err
}

func extractPKsFromDeleteWithContext(ctx context.Context, msg message.BasicMessage) (primaryKeys, bool, error) {
	if msg == nil {
		return primaryKeys{}, false, nil
	}
	if msg.MessageType() != message.MessageTypeDelete {
		return primaryKeys{}, false, nil
	}

	body, err := decodeDeleteBody(ctx, msg)
	if err != nil {
		return primaryKeys{}, true, err
	}
	keys, err := primaryKeysFromIDs(body.GetPrimaryKeys())
	return keys, true, err
}

// extractPKsFromInsert extracts the PK column identified by fieldID from one
// partial-update insert chunk.
func extractPKsFromInsert(msg message.MutableMessage, fieldID int64) ([]any, error) {
	keys, err := extractPKsFromInsertWithContext(context.Background(), msg, fieldID)
	return keys.toAny(), err
}

func extractPKsFromInsertWithContext(ctx context.Context, msg message.BasicMessage, fieldID int64) (primaryKeys, error) {
	if msg == nil || fieldID <= 0 {
		return primaryKeys{}, status.NewUnrecoverableError("insert primary key field id is invalid")
	}
	body, err := decodeInsertBody(ctx, msg)
	if err != nil {
		return primaryKeys{}, err
	}
	return extractPKsFromFieldData(body.GetFieldsData(), fieldID)
}

// extractPKsFromCASInsert derives collection and PK identity from the Insert
// header and ShardManager instead of trusting attempt-scoped CAS proof.
func extractPKsFromCASInsert(
	msg message.MutableMessage,
	descriptorGetter primaryKeyDescriptorGetter,
) ([]any, casInsertScope, error) {
	keys, scope, _, err := extractPKsFromCASInsertWithContext(context.Background(), msg, descriptorGetter)
	return keys.toAny(), scope, err
}

func extractPKsFromCASInsertWithContext(
	ctx context.Context,
	msg message.BasicMessage,
	descriptorGetter primaryKeyDescriptorGetter,
) (primaryKeys, casInsertScope, string, error) {
	if descriptorGetter == nil {
		return primaryKeys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"partial update primary key descriptor getter is unavailable",
		)
	}
	header, err := insertHeader(msg)
	if err != nil {
		return primaryKeys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"decode partial update insert message failed: %v",
			err,
		)
	}
	if header.GetCollectionId() == 0 {
		return primaryKeys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"partial update CAS insert collection id is empty",
		)
	}
	if header.SchemaVersion == nil {
		return primaryKeys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"partial update CAS insert schema version is missing",
		)
	}

	scope := casInsertScope{
		collectionID:  header.GetCollectionId(),
		schemaVersion: header.GetSchemaVersion(),
	}
	descriptor, err := primaryKeyDescriptorOf(descriptorGetter, scope.collectionID)
	if err != nil {
		return primaryKeys{}, casInsertScope{}, "", err
	}

	body, err := decodeInsertBody(ctx, msg)
	if err != nil {
		return primaryKeys{}, casInsertScope{}, "", err
	}
	keys, err := extractPKsFromDescriptor(body.GetFieldsData(), descriptor)
	if err != nil {
		return primaryKeys{}, casInsertScope{}, "", err
	}
	encoded := body.GetBase().GetProperties()["_puc"]
	if encoded == "" {
		return primaryKeys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"partial update CAS body metadata is missing",
		)
	}
	return keys, scope, encoded, nil
}

// extractPKsFromOrdinaryInsert extracts the PKs of an ordinary insert, with or
// without a schema version, through the PK field of its collection's schema.
func extractPKsFromOrdinaryInsert(msg message.MutableMessage, descriptorGetter primaryKeyDescriptorGetter) ([]any, error) {
	keys, err := extractPKsFromOrdinaryInsertWithContext(context.Background(), msg, descriptorGetter)
	return keys.toAny(), err
}

func extractPKsFromOrdinaryInsertWithContext(
	ctx context.Context,
	msg message.BasicMessage,
	descriptorGetter primaryKeyDescriptorGetter,
) (primaryKeys, error) {
	header, err := insertHeader(msg)
	if err != nil {
		return primaryKeys{}, status.NewUnrecoverableError("decode insert message for partial update tracking failed: %v", err)
	}
	if header.GetCollectionId() == 0 {
		return primaryKeys{}, status.NewUnrecoverableError("insert collection id is empty")
	}
	if descriptorGetter == nil {
		return primaryKeys{}, status.NewUnrecoverableError("partial update primary key descriptor getter is unavailable")
	}
	descriptor, err := primaryKeyDescriptorOf(descriptorGetter, header.GetCollectionId())
	if err != nil {
		return primaryKeys{}, err
	}

	body, err := decodeInsertBody(ctx, msg)
	if err != nil {
		return primaryKeys{}, err
	}
	keys, err := extractPKsFromFieldData(body.GetFieldsData(), descriptor.FieldID)
	if err == nil {
		err = validatePrimaryKeysScalarType(keys, descriptor.DataType)
	}
	return keys, err
}

// primaryKeyDescriptorOf resolves the PK field from the schema ShardManager
// holds for the collection. Every collection on a supported path has one:
// CreateCollection carries the schema and vchannel recovery meta has persisted
// it since 2.6.0. A missing collection or schema is therefore an invariant
// violation, and the write fails before the WAL append instead of being
// exempted from row locks.
func primaryKeyDescriptorOf(
	descriptorGetter primaryKeyDescriptorGetter,
	collectionID int64,
) (shards.PrimaryKeyDescriptor, error) {
	descriptor, err := descriptorGetter.GetPrimaryKeyDescriptor(collectionID, latestCollectionSchemaVersion)
	switch {
	case err == nil:
	case errors.Is(err, shards.ErrCollectionNotFound):
		return shards.PrimaryKeyDescriptor{}, status.NewUnrecoverableError(
			"partial update cannot derive primary keys: collection %d not found", collectionID)
	case errors.Is(err, shards.ErrCollectionSchemaNotFound):
		return shards.PrimaryKeyDescriptor{}, status.NewUnrecoverableError(
			"partial update cannot derive primary keys: collection %d schema not provided by create collection message", collectionID)
	default:
		return shards.PrimaryKeyDescriptor{}, status.NewUnrecoverableError(
			"get primary key descriptor for partial update failed, collection: %d: %v", collectionID, err)
	}
	if descriptor.FieldID <= 0 || !typeutil.IsPrimaryFieldType(descriptor.DataType) {
		return shards.PrimaryKeyDescriptor{}, status.NewUnrecoverableError(
			"partial update primary key descriptor is invalid, field: %d, type: %s",
			descriptor.FieldID,
			descriptor.DataType.String(),
		)
	}
	return descriptor, nil
}

// Recovery bodies are immutable. Decode their headers with the matching typed
// reader rather than passing them to a mutable-message conversion.
func insertHeader(msg message.BasicMessage) (*message.InsertMessageHeader, error) {
	if immutable, ok := msg.(message.ImmutableMessage); ok {
		insert, err := message.AsImmutableInsertMessageV1(immutable)
		if err != nil {
			return nil, err
		}
		return insert.Header(), nil
	}
	insert, err := message.AsMutableInsertMessageV1(msg)
	if err != nil {
		return nil, err
	}
	return insert.Header(), nil
}

func decodeInsertBody(ctx context.Context, msg message.BasicMessage) (*msgpb.InsertRequest, error) {
	if _, err := insertHeader(msg); err != nil {
		return nil, status.NewUnrecoverableError("decode partial update insert message failed: %v", err)
	}
	payload, err := message.DecodePayload(ctx, msg)
	if err != nil {
		return nil, decodePayloadError("insert", err)
	}
	body := &msgpb.InsertRequest{}
	if err := proto.Unmarshal(payload, body); err != nil {
		return nil, status.NewUnrecoverableError("decode partial update insert body failed: %v", err)
	}
	return body, nil
}

func decodeDeleteBody(ctx context.Context, msg message.BasicMessage) (*msgpb.DeleteRequest, error) {
	var headerErr error
	if immutable, ok := msg.(message.ImmutableMessage); ok {
		_, headerErr = message.AsImmutableDeleteMessageV1(immutable)
	} else {
		_, headerErr = message.AsMutableDeleteMessageV1(msg)
	}
	if headerErr != nil {
		return nil, status.NewUnrecoverableError("decode partial update delete message failed: %v", headerErr)
	}
	payload, err := message.DecodePayload(ctx, msg)
	if err != nil {
		return nil, decodePayloadError("delete", err)
	}
	body := &msgpb.DeleteRequest{}
	if err := proto.Unmarshal(payload, body); err != nil {
		return nil, status.NewUnrecoverableError("decode partial update delete body failed: %v", err)
	}
	return body, nil
}

func extractPKsFromFieldData(fields []*schemapb.FieldData, fieldID int64) (primaryKeys, error) {
	field, err := findPKFieldData(fields, fieldID)
	if err != nil {
		return primaryKeys{}, err
	}
	return primaryKeysFromFieldData(field, fieldID)
}

func extractPKsFromDescriptor(
	fields []*schemapb.FieldData,
	descriptor shards.PrimaryKeyDescriptor,
) (primaryKeys, error) {
	field, err := findPKFieldData(fields, descriptor.FieldID)
	if err != nil {
		return primaryKeys{}, err
	}
	if field.GetType() != descriptor.DataType {
		return primaryKeys{}, status.NewUnrecoverableError(
			"primary key field type %s does not match schema type %s",
			field.GetType().String(),
			descriptor.DataType.String(),
		)
	}
	keys, err := primaryKeysFromFieldData(field, descriptor.FieldID)
	if err != nil {
		return primaryKeys{}, err
	}
	if err := validatePrimaryKeysScalarType(keys, descriptor.DataType); err != nil {
		return primaryKeys{}, err
	}
	return keys, nil
}

func findPKFieldData(fields []*schemapb.FieldData, fieldID int64) (*schemapb.FieldData, error) {
	var matched *schemapb.FieldData
	for _, field := range fields {
		if field == nil || field.GetFieldId() != fieldID {
			continue
		}
		if matched != nil {
			return nil, status.NewUnrecoverableError(
				"insert primary key field %d is duplicated",
				fieldID,
			)
		}
		matched = field
	}
	if matched == nil {
		return nil, status.NewUnrecoverableError(
			"insert primary key field %d is missing",
			fieldID,
		)
	}
	return matched, nil
}

func primaryKeysFromFieldData(field *schemapb.FieldData, fieldID int64) (primaryKeys, error) {
	if field == nil {
		return primaryKeys{}, status.NewUnrecoverableError(
			"insert primary key field %d is missing",
			fieldID,
		)
	}
	scalars := field.GetScalars()
	switch values := scalars.GetData().(type) {
	case *schemapb.ScalarField_LongData:
		if values == nil {
			return primaryKeys{}, status.NewUnrecoverableError("int64 primary keys are nil")
		}
		return primaryKeysFromIDs(&schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: values.LongData},
		})
	case *schemapb.ScalarField_StringData:
		if values == nil {
			return primaryKeys{}, status.NewUnrecoverableError("varchar primary keys are nil")
		}
		return primaryKeysFromIDs(&schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: values.StringData},
		})
	default:
		return primaryKeys{}, status.NewUnrecoverableError(
			"insert primary key field %d must be int64 or varchar",
			fieldID,
		)
	}
}

func primaryKeysFromIDs(ids *schemapb.IDs) (primaryKeys, error) {
	if ids == nil {
		return primaryKeys{}, status.NewUnrecoverableError("primary keys are nil")
	}
	switch values := ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		if values == nil || values.IntId == nil {
			return primaryKeys{}, status.NewUnrecoverableError("int64 primary keys are nil")
		}
		if len(values.IntId.GetData()) == 0 {
			return primaryKeys{}, status.NewUnrecoverableError("primary keys are empty")
		}
		return primaryKeys{
			kind:        primaryKeyKindInt64,
			int64Values: values.IntId.GetData(),
		}, nil
	case *schemapb.IDs_StrId:
		if values == nil || values.StrId == nil {
			return primaryKeys{}, status.NewUnrecoverableError("varchar primary keys are nil")
		}
		if len(values.StrId.GetData()) == 0 {
			return primaryKeys{}, status.NewUnrecoverableError("primary keys are empty")
		}
		return primaryKeys{
			kind:         primaryKeyKindString,
			stringValues: values.StrId.GetData(),
		}, nil
	default:
		return primaryKeys{}, status.NewUnrecoverableError("unsupported primary key ids type %T", values)
	}
}

func validatePrimaryKeysScalarType(pks primaryKeys, dataType schemapb.DataType) error {
	var expected primaryKeyKind
	switch dataType {
	case schemapb.DataType_Int64:
		expected = primaryKeyKindInt64
	case schemapb.DataType_VarChar:
		expected = primaryKeyKindString
	default:
		return status.NewUnrecoverableError(
			"primary key has unsupported data type %s",
			dataType.String(),
		)
	}
	if pks.kind != expected {
		return status.NewUnrecoverableError(
			"primary key payload does not match schema type %s",
			dataType.String(),
		)
	}
	return nil
}

func decodePayloadError(kind string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return status.NewInner("decode partial update %s payload failed: %v", kind, err)
}

// extractCollectionFenceID extracts collections affected by wide data mutations.
func extractCollectionFenceID(msg message.BasicMessage) (int64, bool) {
	if msg == nil {
		return 0, false
	}

	switch msg.MessageType() {
	case message.MessageTypeTruncateCollection:
		if immutable, ok := msg.(message.ImmutableMessage); ok {
			truncateMsg, err := message.AsImmutableTruncateCollectionMessageV2(immutable)
			if err != nil {
				return 0, true
			}
			return truncateMsg.Header().GetCollectionId(), true
		}
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
func extractDropCollectionID(msg message.BasicMessage) (int64, bool) {
	if msg == nil || msg.MessageType() != message.MessageTypeDropCollection {
		return 0, false
	}
	if immutable, ok := msg.(message.ImmutableMessage); ok {
		dropMsg, err := message.AsImmutableDropCollectionMessageV1(immutable)
		if err != nil {
			return 0, true
		}
		return dropMsg.Header().GetCollectionId(), true
	}
	dropMsg, err := message.AsMutableDropCollectionMessageV1(msg)
	if err != nil {
		return 0, true
	}
	return dropMsg.Header().GetCollectionId(), true
}
