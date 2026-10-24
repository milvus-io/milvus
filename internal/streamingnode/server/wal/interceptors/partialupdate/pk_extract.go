package partialupdate

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
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
	return keys.ToAny(), true, err
}

func extractPKsFromDeleteWithContext(ctx context.Context, msg message.BasicMessage) (primarykey.Keys, bool, error) {
	if msg == nil {
		return primarykey.Keys{}, false, nil
	}
	if msg.MessageType() != message.MessageTypeDelete {
		return primarykey.Keys{}, false, nil
	}
	body, err := decodeDeleteBody(ctx, msg)
	if err != nil {
		return primarykey.Keys{}, true, err
	}
	keys, err := primarykey.KeysOfDelete(body)
	return keys, true, err
}

// extractPKsFromInsert extracts the PK column identified by fieldID from one
// partial-update insert chunk.
func extractPKsFromInsert(msg message.MutableMessage, fieldID int64) ([]any, error) {
	keys, err := extractPKsFromInsertWithContext(context.Background(), msg, fieldID)
	return keys.ToAny(), err
}

func extractPKsFromInsertWithContext(ctx context.Context, msg message.MutableMessage, fieldID int64) (primarykey.Keys, error) {
	if msg == nil || fieldID <= 0 {
		return primarykey.Keys{}, status.NewUnrecoverableError("insert primary key field id is invalid")
	}
	body, err := utility.DecodeInsertBody(ctx, msg)
	if err != nil {
		return primarykey.Keys{}, err
	}
	return primarykey.KeysOfInsertField(body, fieldID)
}

// extractPKsFromCASInsert derives collection and PK identity from the Insert
// header and ShardManager instead of trusting attempt-scoped CAS proof.
func extractPKsFromCASInsert(
	msg message.MutableMessage,
	descriptorGetter primaryKeyDescriptorGetter,
) ([]any, casInsertScope, error) {
	keys, scope, _, err := extractPKsFromCASInsertWithContext(context.Background(), msg, descriptorGetter)
	return keys.ToAny(), scope, err
}

func extractPKsFromCASInsertWithContext(
	ctx context.Context,
	msg message.BasicMessage,
	descriptorGetter primaryKeyDescriptorGetter,
) (primarykey.Keys, casInsertScope, string, error) {
	if descriptorGetter == nil {
		return primarykey.Keys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"partial update primary key descriptor getter is unavailable",
		)
	}
	header, err := insertHeader(msg)
	if err != nil {
		return primarykey.Keys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"decode partial update insert message failed: %v",
			err,
		)
	}
	if header.GetCollectionId() == 0 {
		return primarykey.Keys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"partial update CAS insert collection id is empty",
		)
	}
	if header.SchemaVersion == nil {
		return primarykey.Keys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"partial update CAS insert schema version is missing",
		)
	}

	scope := casInsertScope{
		collectionID:  header.GetCollectionId(),
		schemaVersion: header.GetSchemaVersion(),
	}
	descriptor, err := primaryKeyDescriptorOf(descriptorGetter, scope.collectionID)
	if err != nil {
		return primarykey.Keys{}, casInsertScope{}, "", err
	}

	body, err := decodeInsertBody(ctx, msg)
	if err != nil {
		return primarykey.Keys{}, casInsertScope{}, "", err
	}
	keys, err := primarykey.KeysOfInsertDeclared(body, descriptor.FieldID, descriptor.DataType)
	if err != nil {
		return primarykey.Keys{}, casInsertScope{}, "", err
	}
	encoded := body.GetBase().GetProperties()["_puc"]
	if encoded == "" {
		return primarykey.Keys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"partial update CAS body metadata is missing",
		)
	}
	return keys, scope, encoded, nil
}

// extractPKsFromOrdinaryInsert extracts the PKs of an ordinary insert, with or
// without a schema version, through the PK field of its collection's schema.
func extractPKsFromOrdinaryInsert(msg message.MutableMessage, descriptorGetter primaryKeyDescriptorGetter) ([]any, error) {
	keys, err := extractPKsFromOrdinaryInsertWithContext(context.Background(), msg, descriptorGetter)
	return keys.ToAny(), err
}

func extractPKsFromOrdinaryInsertWithContext(
	ctx context.Context,
	msg message.BasicMessage,
	descriptorGetter primaryKeyDescriptorGetter,
) (primarykey.Keys, error) {
	header, err := insertHeader(msg)
	if err != nil {
		return primarykey.Keys{}, status.NewUnrecoverableError("decode insert message for partial update tracking failed: %v", err)
	}
	if header.GetCollectionId() == 0 {
		return primarykey.Keys{}, status.NewUnrecoverableError("insert collection id is empty")
	}
	if descriptorGetter == nil {
		return primarykey.Keys{}, status.NewUnrecoverableError("partial update primary key descriptor getter is unavailable")
	}
	descriptor, err := primaryKeyDescriptorOf(descriptorGetter, header.GetCollectionId())
	if err != nil {
		return primarykey.Keys{}, err
	}

	body, err := decodeInsertBody(ctx, msg)
	if err != nil {
		return primarykey.Keys{}, err
	}
	return primarykey.KeysOfInsert(body, descriptor.FieldID, descriptor.DataType)
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

// Recovery messages are immutable. Decode them through their typed read-only
// interface rather than treating WAL payloads as mutable append messages.
func insertHeader(msg message.BasicMessage) (*message.InsertMessageHeader, error) {
	// immutableMessageImpl embeds messageImpl and also satisfies MutableMessage;
	// test the immutable form first to avoid the mutable converter's concrete cast.
	if immutable, ok := msg.(message.ImmutableMessage); ok {
		insert, err := message.AsImmutableInsertMessageV1(immutable)
		if err != nil {
			return nil, err
		}
		return insert.Header(), nil
	}
	mutable, ok := msg.(message.MutableMessage)
	if !ok {
		return nil, status.NewUnrecoverableError("insert message is neither mutable nor immutable")
	}
	insert, err := message.AsMutableInsertMessageV1(mutable)
	if err != nil {
		return nil, err
	}
	return insert.Header(), nil
}

func decodeInsertBody(ctx context.Context, msg message.BasicMessage) (*msgpb.InsertRequest, error) {
	if immutable, ok := msg.(message.ImmutableMessage); ok {
		insert, err := message.AsImmutableInsertMessageV1(immutable)
		if err != nil {
			return nil, status.NewUnrecoverableError("decode recovered insert failed: %v", err)
		}
		return decodeRecoveredBody(ctx, "insert", insert)
	}
	return utility.DecodeInsertBody(ctx, msg.(message.MutableMessage))
}

func decodeDeleteBody(ctx context.Context, msg message.BasicMessage) (*msgpb.DeleteRequest, error) {
	if immutable, ok := msg.(message.ImmutableMessage); ok {
		delete, err := message.AsImmutableDeleteMessageV1(immutable)
		if err != nil {
			return nil, status.NewUnrecoverableError("decode recovered delete failed: %v", err)
		}
		return decodeRecoveredBody(ctx, "delete", delete)
	}
	return utility.DecodeDeleteBody(ctx, msg.(message.MutableMessage))
}

func decodeRecoveredBody[B proto.Message](ctx context.Context, kind string, msg interface {
	Body(context.Context) (B, error)
},
) (B, error) {
	body, err := msg.Body(ctx)
	switch {
	case err == nil:
		return body, nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return body, err
	case errors.Is(err, message.ErrMalformedBody):
		return body, status.NewUnrecoverableError("decode recovered %s body failed: %v", kind, err)
	default:
		return body, status.NewInner("decode recovered %s payload failed: %v", kind, err)
	}
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
		truncateMsg, err := message.AsMutableTruncateCollectionMessageV2(msg.(message.MutableMessage))
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
	dropMsg, err := message.AsMutableDropCollectionMessageV1(msg.(message.MutableMessage))
	if err != nil {
		return 0, true
	}
	return dropMsg.Header().GetCollectionId(), true
}
