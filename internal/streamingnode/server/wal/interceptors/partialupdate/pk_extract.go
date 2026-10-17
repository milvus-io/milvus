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

// latestCollectionSchemaVersion asks ShardManager for its current PK descriptor
// when an insert header does not carry an explicit version.
const latestCollectionSchemaVersion int32 = -1

type casInsertScope struct {
	collectionID  int64
	schemaVersion int32
}

// extractPKs extracts row-content primary keys from WAL delete messages.
// ok=false means callers must handle inserts and collection-wide writes separately.
func extractPKs(msg message.MutableMessage) ([]any, bool, error) {
	keys, ok, err := extractPKsWithContext(context.Background(), msg)
	if !ok {
		return nil, false, err
	}
	return keys.ToAny(), true, err
}

func extractPKsWithContext(ctx context.Context, msg message.BasicMessage) (primarykey.Keys, bool, error) {
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
	descriptor, err := descriptorGetter.GetPrimaryKeyDescriptor(
		scope.collectionID,
		scope.schemaVersion,
	)
	if err != nil {
		if errors.Is(err, shards.ErrCollectionSchemaVersionNotMatch) {
			return primarykey.Keys{}, casInsertScope{}, "", status.NewSchemaVersionMismatch(
				"schema version mismatch while validating partial update CAS, collection: %d, schema version: %d",
				scope.collectionID,
				scope.schemaVersion,
			)
		}
		return primarykey.Keys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"get primary key descriptor for partial update CAS failed: %v",
			err,
		)
	}
	if descriptor.FieldID <= 0 || !typeutil.IsPrimaryFieldType(descriptor.DataType) {
		return primarykey.Keys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"partial update primary key descriptor is invalid, field: %d, type: %s",
			descriptor.FieldID,
			descriptor.DataType.String(),
		)
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

// Legacy inserts still use the latest authoritative schema when available.
// Without a schema they retain their old write behavior and publish a fence;
// exact row-lock/CAS coordination is only promised for versioned inserts.
func extractPKsFromOrdinaryInsert(msg message.MutableMessage, descriptorGetter primaryKeyDescriptorGetter) (pks []any, fenceCollectionID int64, err error) {
	keys, fenceCollectionID, err := extractPKsFromOrdinaryInsertWithContext(context.Background(), msg, descriptorGetter)
	return keys.ToAny(), fenceCollectionID, err
}

func extractPKsFromOrdinaryInsertWithContext(
	ctx context.Context,
	msg message.BasicMessage,
	descriptorGetter primaryKeyDescriptorGetter,
) (pks primarykey.Keys, fenceCollectionID int64, err error) {
	header, err := insertHeader(msg)
	if err != nil {
		return primarykey.Keys{}, 0, status.NewUnrecoverableError("decode insert message for partial update tracking failed: %v", err)
	}
	if header.GetCollectionId() == 0 {
		return primarykey.Keys{}, 0, status.NewUnrecoverableError("insert collection id is empty")
	}
	if descriptorGetter == nil {
		return primarykey.Keys{}, 0, status.NewUnrecoverableError("partial update primary key descriptor getter is unavailable")
	}
	schemaVersion := latestCollectionSchemaVersion
	if header.SchemaVersion != nil {
		schemaVersion = header.GetSchemaVersion()
	}
	descriptor, err := descriptorGetter.GetPrimaryKeyDescriptor(header.GetCollectionId(), schemaVersion)
	if err != nil {
		if header.SchemaVersion == nil && errors.Is(err, shards.ErrCollectionSchemaNotFound) {
			return primarykey.Keys{}, header.GetCollectionId(), nil
		}
		if errors.Is(err, shards.ErrCollectionSchemaVersionNotMatch) {
			return primarykey.Keys{}, 0, status.NewSchemaVersionMismatch(
				"schema version mismatch while tracking partial update writes, collection: %d, schema version: %d",
				header.GetCollectionId(), header.GetSchemaVersion())
		}
		return primarykey.Keys{}, 0, status.NewUnrecoverableError("get primary key descriptor for partial update tracking failed: %v", err)
	}
	if !typeutil.IsPrimaryFieldType(descriptor.DataType) {
		return primarykey.Keys{}, 0, status.NewUnrecoverableError(
			"partial update primary key field %d has unsupported data type %s",
			descriptor.FieldID,
			descriptor.DataType.String(),
		)
	}

	body, err := decodeInsertBody(ctx, msg)
	if err != nil {
		return primarykey.Keys{}, 0, err
	}
	keys, err := primarykey.KeysOfInsert(body, descriptor.FieldID, descriptor.DataType)
	return keys, 0, err
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
