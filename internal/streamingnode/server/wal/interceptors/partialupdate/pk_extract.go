package partialupdate

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	insertBaseFieldNumber       protowire.Number = 1
	insertFieldsDataFieldNumber protowire.Number = 13
	deletePrimaryKeysField      protowire.Number = 12
	msgBasePropertiesField      protowire.Number = 6
	fieldDataTypeField          protowire.Number = 1
	fieldDataFieldIDField       protowire.Number = 5
	fieldDataScalarsField       protowire.Number = 3
	scalarLongDataField         protowire.Number = 3
	scalarStringDataField       protowire.Number = 6
	idsIntIDField               protowire.Number = 1
	idsStringIDField            protowire.Number = 2
	longArrayDataField          protowire.Number = 1
	stringArrayDataField        protowire.Number = 1
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
	return keys.toAny(), ok, err
}

func extractPKsWithContext(ctx context.Context, msg message.MutableMessage) (primaryKeys, bool, error) {
	if msg == nil {
		return primaryKeys{}, false, nil
	}

	switch msg.MessageType() {
	case message.MessageTypeDelete:
		payload, err := message.DecodePayload(ctx, msg)
		if err != nil {
			return primaryKeys{}, true, decodePayloadError("delete", err)
		}
		pks, err := scanDeletePKs(payload)
		return pks, true, err
	default:
		return primaryKeys{}, false, nil
	}
}

// extractPKsFromInsert extracts the PK column identified by fieldID from one
// partial-update insert chunk.
func extractPKsFromInsert(msg message.MutableMessage, fieldID int64) ([]any, error) {
	keys, err := extractPKsFromInsertWithContext(context.Background(), msg, fieldID)
	return keys.toAny(), err
}

func extractPKsFromInsertWithContext(ctx context.Context, msg message.MutableMessage, fieldID int64) (primaryKeys, error) {
	if msg == nil || fieldID <= 0 {
		return primaryKeys{}, status.NewUnrecoverableError("partial update insert primary key field id is invalid")
	}
	payload, err := message.DecodePayload(ctx, msg)
	if err != nil {
		return primaryKeys{}, decodePayloadError("insert", err)
	}
	keys, err := scanInsertPKs(payload, fieldID, false)
	return keys, err
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
	msg message.MutableMessage,
	descriptorGetter primaryKeyDescriptorGetter,
) (primaryKeys, casInsertScope, string, error) {
	if descriptorGetter == nil {
		return primaryKeys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"partial update primary key descriptor getter is unavailable",
		)
	}
	insertMsg, err := message.AsMutableInsertMessageV1(msg)
	if err != nil {
		return primaryKeys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"decode partial update insert message failed: %v",
			err,
		)
	}
	header := insertMsg.Header()
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
	descriptor, err := descriptorGetter.GetPrimaryKeyDescriptor(
		scope.collectionID,
		scope.schemaVersion,
	)
	if err != nil {
		if errors.Is(err, shards.ErrCollectionSchemaVersionNotMatch) {
			return primaryKeys{}, casInsertScope{}, "", status.NewSchemaVersionMismatch(
				"schema version mismatch while validating partial update CAS, collection: %d, schema version: %d",
				scope.collectionID,
				scope.schemaVersion,
			)
		}
		return primaryKeys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"get primary key descriptor for partial update CAS failed: %v",
			err,
		)
	}
	if descriptor.FieldID <= 0 || !typeutil.IsPrimaryFieldType(descriptor.DataType) {
		return primaryKeys{}, casInsertScope{}, "", status.NewUnrecoverableError(
			"partial update primary key descriptor is invalid, field: %d, type: %s",
			descriptor.FieldID,
			descriptor.DataType.String(),
		)
	}
	payload, err := message.DecodePayload(ctx, msg)
	if err != nil {
		return primaryKeys{}, casInsertScope{}, "", decodePayloadError("insert", err)
	}
	result, err := scanInsertPayload(payload, descriptor.FieldID, true)
	if err != nil {
		return primaryKeys{}, casInsertScope{}, "", err
	}
	if err := validatePrimaryKeysType(result.pks, result.pkDataType, descriptor.DataType); err != nil {
		return primaryKeys{}, casInsertScope{}, "", err
	}
	return result.pks, scope, result.casMetaEncoded, nil
}

// extractPKsFromOrdinaryInsert returns exact PKs when schema is available, or
// a collection fence ID for a schema-less legacy insert accepted by shard.
func extractPKsFromOrdinaryInsert(msg message.MutableMessage, descriptorGetter primaryKeyDescriptorGetter) (pks []any, fenceCollectionID int64, err error) {
	keys, fenceCollectionID, err := extractPKsFromOrdinaryInsertWithContext(context.Background(), msg, descriptorGetter)
	return keys.toAny(), fenceCollectionID, err
}

func extractPKsFromOrdinaryInsertWithContext(
	ctx context.Context,
	msg message.MutableMessage,
	descriptorGetter primaryKeyDescriptorGetter,
) (pks primaryKeys, fenceCollectionID int64, err error) {
	if descriptorGetter == nil {
		return primaryKeys{}, 0, status.NewUnrecoverableError("partial update primary key descriptor getter is unavailable")
	}
	insertMsg, err := message.AsMutableInsertMessageV1(msg)
	if err != nil {
		return primaryKeys{}, 0, status.NewUnrecoverableError("decode insert message for partial update tracking failed: %v", err)
	}
	header := insertMsg.Header()
	schemaVersion := latestCollectionSchemaVersion
	if header.SchemaVersion != nil {
		schemaVersion = header.GetSchemaVersion()
	}
	descriptor, err := descriptorGetter.GetPrimaryKeyDescriptor(header.GetCollectionId(), schemaVersion)
	if err != nil {
		if errors.Is(err, shards.ErrCollectionSchemaVersionNotMatch) {
			return primaryKeys{}, 0, status.NewSchemaVersionMismatch(
				"schema version mismatch while tracking partial update writes, collection: %d, schema version: %d",
				header.GetCollectionId(), header.GetSchemaVersion())
		}
		// Shard accepts schema-less legacy inserts during rolling upgrades.
		// Fence the collection when their exact PK field cannot be recovered.
		if header.SchemaVersion == nil && errors.Is(err, shards.ErrCollectionSchemaNotFound) {
			if header.GetCollectionId() == 0 {
				return primaryKeys{}, 0, status.NewUnrecoverableError("partial update ordinary insert collection id is empty")
			}
			return primaryKeys{}, header.GetCollectionId(), nil
		}
		return primaryKeys{}, 0, status.NewUnrecoverableError("get primary key descriptor for partial update tracking failed: %v", err)
	}
	if !typeutil.IsPrimaryFieldType(descriptor.DataType) {
		return primaryKeys{}, 0, status.NewUnrecoverableError(
			"partial update primary key field %d has unsupported data type %s",
			descriptor.FieldID,
			descriptor.DataType.String(),
		)
	}
	payload, err := message.DecodePayload(ctx, msg)
	if err != nil {
		return primaryKeys{}, 0, decodePayloadError("insert", err)
	}
	result, err := scanInsertPayload(payload, descriptor.FieldID, false)
	if err == nil {
		// Ordinary inserts preserve the legacy behavior of deriving the PK kind
		// from the scalar oneof. Only CAS inserts require the declared
		// FieldData.Type to be present and consistent with the descriptor.
		err = validatePrimaryKeysScalarType(result.pks, descriptor.DataType)
	}
	return result.pks, 0, err
}

type insertPayloadData struct {
	pks            primaryKeys
	pkDataType     schemapb.DataType
	casMetaEncoded string
}

func scanInsertPayload(payload []byte, fieldID int64, includeCAS bool) (insertPayloadData, error) {
	if fieldID <= 0 {
		return insertPayloadData{}, status.NewUnrecoverableError(
			"partial update insert primary key field id is invalid",
		)
	}
	var result insertPayloadData
	foundPK := false
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return insertPayloadData{}, malformedPayloadError("insert", tagLength)
		}
		payload = payload[tagLength:]
		if wireType != protowire.BytesType {
			valueLength := protowire.ConsumeFieldValue(number, wireType, payload)
			if valueLength < 0 {
				return insertPayloadData{}, malformedPayloadError("insert", valueLength)
			}
			payload = payload[valueLength:]
			continue
		}

		value, valueLength := protowire.ConsumeBytes(payload)
		if valueLength < 0 {
			return insertPayloadData{}, malformedPayloadError("insert", valueLength)
		}
		payload = payload[valueLength:]
		switch number {
		case insertBaseFieldNumber:
			if includeCAS && result.casMetaEncoded == "" {
				encoded, err := scanCASMetadataFromMsgBase(value)
				if err != nil {
					return insertPayloadData{}, err
				}
				result.casMetaEncoded = encoded
			}
		case insertFieldsDataFieldNumber:
			matched, dataType, pks, err := scanFieldDataPKs(value, fieldID)
			if err != nil {
				return insertPayloadData{}, err
			}
			if matched {
				if foundPK {
					return insertPayloadData{}, status.NewUnrecoverableError(
						"partial update insert primary key field %d is duplicated",
						fieldID,
					)
				}
				foundPK = true
				result.pkDataType = dataType
				result.pks = pks
			}
		}
	}
	if !foundPK {
		return insertPayloadData{}, status.NewUnrecoverableError(
			"partial update insert primary key field %d is missing",
			fieldID,
		)
	}
	if includeCAS && result.casMetaEncoded == "" {
		return insertPayloadData{}, status.NewUnrecoverableError(
			"partial update CAS body metadata is missing",
		)
	}
	return result, nil
}

func scanInsertPKs(payload []byte, fieldID int64, includeCAS bool) (primaryKeys, error) {
	result, err := scanInsertPayload(payload, fieldID, includeCAS)
	if err != nil {
		return primaryKeys{}, err
	}
	return result.pks, nil
}

func validatePrimaryKeysType(pks primaryKeys, declaredType, dataType schemapb.DataType) error {
	if declaredType != dataType {
		return status.NewUnrecoverableError(
			"partial update primary key field type %s does not match schema type %s",
			declaredType.String(),
			dataType.String(),
		)
	}
	return validatePrimaryKeysScalarType(pks, dataType)
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
			"partial update primary key has unsupported data type %s",
			dataType.String(),
		)
	}
	if pks.kind != expected {
		return status.NewUnrecoverableError(
			"partial update primary key payload does not match schema type %s",
			dataType.String(),
		)
	}
	return nil
}

func scanDeletePKs(payload []byte) (primaryKeys, error) {
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return primaryKeys{}, malformedPayloadError("delete", tagLength)
		}
		payload = payload[tagLength:]
		if number != deletePrimaryKeysField || wireType != protowire.BytesType {
			valueLength := protowire.ConsumeFieldValue(number, wireType, payload)
			if valueLength < 0 {
				return primaryKeys{}, malformedPayloadError("delete", valueLength)
			}
			payload = payload[valueLength:]
			continue
		}
		ids, valueLength := protowire.ConsumeBytes(payload)
		if valueLength < 0 {
			return primaryKeys{}, malformedPayloadError("delete", valueLength)
		}
		return scanIDs(ids)
	}
	return primaryKeys{}, status.NewUnrecoverableError("partial update primary keys are nil")
}

func scanFieldDataPKs(payload []byte, fieldID int64) (bool, schemapb.DataType, primaryKeys, error) {
	var encodedScalars []byte
	matched := false
	var dataType schemapb.DataType
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return false, schemapb.DataType_None, primaryKeys{}, malformedPayloadError("field data", tagLength)
		}
		payload = payload[tagLength:]
		switch {
		case number == fieldDataTypeField && wireType == protowire.VarintType:
			value, valueLength := protowire.ConsumeVarint(payload)
			if valueLength < 0 {
				return false, schemapb.DataType_None, primaryKeys{}, malformedPayloadError("field data", valueLength)
			}
			dataType = schemapb.DataType(value)
			payload = payload[valueLength:]
		case number == fieldDataFieldIDField && wireType == protowire.VarintType:
			value, valueLength := protowire.ConsumeVarint(payload)
			if valueLength < 0 {
				return false, schemapb.DataType_None, primaryKeys{}, malformedPayloadError("field data", valueLength)
			}
			if int64(value) == fieldID {
				matched = true
			}
			payload = payload[valueLength:]
		case number == fieldDataScalarsField && wireType == protowire.BytesType:
			value, valueLength := protowire.ConsumeBytes(payload)
			if valueLength < 0 {
				return false, schemapb.DataType_None, primaryKeys{}, malformedPayloadError("field data", valueLength)
			}
			encodedScalars = value
			payload = payload[valueLength:]
		default:
			valueLength := protowire.ConsumeFieldValue(number, wireType, payload)
			if valueLength < 0 {
				return false, schemapb.DataType_None, primaryKeys{}, malformedPayloadError("field data", valueLength)
			}
			payload = payload[valueLength:]
		}
	}
	if !matched {
		return false, schemapb.DataType_None, primaryKeys{}, nil
	}
	if encodedScalars == nil {
		return true, dataType, primaryKeys{}, status.NewUnrecoverableError(
			"partial update insert primary key field %d must be int64 or varchar",
			fieldID,
		)
	}
	pks, err := scanScalarPKs(encodedScalars)
	return true, dataType, pks, err
}

func scanScalarPKs(payload []byte) (primaryKeys, error) {
	var result primaryKeys
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return primaryKeys{}, malformedPayloadError("scalar field", tagLength)
		}
		payload = payload[tagLength:]
		if wireType != protowire.BytesType {
			valueLength := protowire.ConsumeFieldValue(number, wireType, payload)
			if valueLength < 0 {
				return primaryKeys{}, malformedPayloadError("scalar field", valueLength)
			}
			payload = payload[valueLength:]
			continue
		}
		value, valueLength := protowire.ConsumeBytes(payload)
		if valueLength < 0 {
			return primaryKeys{}, malformedPayloadError("scalar field", valueLength)
		}
		payload = payload[valueLength:]
		switch number {
		case scalarLongDataField:
			if result.kind != primaryKeyKindNone {
				return primaryKeys{}, status.NewUnrecoverableError("partial update primary key scalar payload is ambiguous")
			}
			values, err := scanLongArray(value)
			if err != nil {
				return primaryKeys{}, err
			}
			result = primaryKeys{kind: primaryKeyKindInt64, int64Values: values}
		case scalarStringDataField:
			if result.kind != primaryKeyKindNone {
				return primaryKeys{}, status.NewUnrecoverableError("partial update primary key scalar payload is ambiguous")
			}
			values, err := scanStringArray(value)
			if err != nil {
				return primaryKeys{}, err
			}
			result = primaryKeys{kind: primaryKeyKindString, stringValues: values}
		}
	}
	if result.Len() == 0 {
		return primaryKeys{}, status.NewUnrecoverableError("partial update primary keys are empty")
	}
	return result, nil
}

func scanIDs(payload []byte) (primaryKeys, error) {
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return primaryKeys{}, malformedPayloadError("primary key ids", tagLength)
		}
		payload = payload[tagLength:]
		if wireType != protowire.BytesType {
			valueLength := protowire.ConsumeFieldValue(number, wireType, payload)
			if valueLength < 0 {
				return primaryKeys{}, malformedPayloadError("primary key ids", valueLength)
			}
			payload = payload[valueLength:]
			continue
		}
		value, valueLength := protowire.ConsumeBytes(payload)
		if valueLength < 0 {
			return primaryKeys{}, malformedPayloadError("primary key ids", valueLength)
		}
		switch number {
		case idsIntIDField:
			values, err := scanLongArray(value)
			return primaryKeys{kind: primaryKeyKindInt64, int64Values: values}, err
		case idsStringIDField:
			values, err := scanStringArray(value)
			return primaryKeys{kind: primaryKeyKindString, stringValues: values}, err
		}
		payload = payload[valueLength:]
	}
	return primaryKeys{}, status.NewUnrecoverableError("unsupported partial update primary key ids type")
}

func scanLongArray(payload []byte) ([]int64, error) {
	values := make([]int64, 0)
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return nil, malformedPayloadError("int64 primary keys", tagLength)
		}
		payload = payload[tagLength:]
		if number != longArrayDataField {
			valueLength := protowire.ConsumeFieldValue(number, wireType, payload)
			if valueLength < 0 {
				return nil, malformedPayloadError("int64 primary keys", valueLength)
			}
			payload = payload[valueLength:]
			continue
		}
		switch wireType {
		case protowire.VarintType:
			value, valueLength := protowire.ConsumeVarint(payload)
			if valueLength < 0 {
				return nil, malformedPayloadError("int64 primary keys", valueLength)
			}
			values = append(values, int64(value))
			payload = payload[valueLength:]
		case protowire.BytesType:
			packed, valueLength := protowire.ConsumeBytes(payload)
			if valueLength < 0 {
				return nil, malformedPayloadError("int64 primary keys", valueLength)
			}
			payload = payload[valueLength:]
			for len(packed) > 0 {
				value, packedLength := protowire.ConsumeVarint(packed)
				if packedLength < 0 {
					return nil, malformedPayloadError("int64 primary keys", packedLength)
				}
				values = append(values, int64(value))
				packed = packed[packedLength:]
			}
		default:
			return nil, status.NewUnrecoverableError("partial update int64 primary keys have invalid wire type")
		}
	}
	if len(values) == 0 {
		return nil, status.NewUnrecoverableError("partial update primary keys are empty")
	}
	return values, nil
}

func scanStringArray(payload []byte) ([]string, error) {
	values := make([]string, 0)
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return nil, malformedPayloadError("varchar primary keys", tagLength)
		}
		payload = payload[tagLength:]
		if number != stringArrayDataField || wireType != protowire.BytesType {
			valueLength := protowire.ConsumeFieldValue(number, wireType, payload)
			if valueLength < 0 {
				return nil, malformedPayloadError("varchar primary keys", valueLength)
			}
			payload = payload[valueLength:]
			continue
		}
		value, valueLength := protowire.ConsumeBytes(payload)
		if valueLength < 0 {
			return nil, malformedPayloadError("varchar primary keys", valueLength)
		}
		values = append(values, string(value))
		payload = payload[valueLength:]
	}
	if len(values) == 0 {
		return nil, status.NewUnrecoverableError("partial update primary keys are empty")
	}
	return values, nil
}

func scanCASMetadataFromMsgBase(payload []byte) (string, error) {
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return "", malformedPayloadError("message base", tagLength)
		}
		payload = payload[tagLength:]
		if number != msgBasePropertiesField || wireType != protowire.BytesType {
			valueLength := protowire.ConsumeFieldValue(number, wireType, payload)
			if valueLength < 0 {
				return "", malformedPayloadError("message base", valueLength)
			}
			payload = payload[valueLength:]
			continue
		}
		entry, valueLength := protowire.ConsumeBytes(payload)
		if valueLength < 0 {
			return "", malformedPayloadError("message base", valueLength)
		}
		key, value, err := scanStringMapEntry(entry)
		if err != nil {
			return "", err
		}
		if key == "_puc" {
			if value == "" {
				return "", status.NewUnrecoverableError("partial update CAS body metadata is missing")
			}
			return value, nil
		}
		payload = payload[valueLength:]
	}
	return "", nil
}

func scanStringMapEntry(payload []byte) (string, string, error) {
	var key, value string
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return "", "", malformedPayloadError("message property", tagLength)
		}
		payload = payload[tagLength:]
		if wireType != protowire.BytesType {
			valueLength := protowire.ConsumeFieldValue(number, wireType, payload)
			if valueLength < 0 {
				return "", "", malformedPayloadError("message property", valueLength)
			}
			payload = payload[valueLength:]
			continue
		}
		bytesValue, valueLength := protowire.ConsumeBytes(payload)
		if valueLength < 0 {
			return "", "", malformedPayloadError("message property", valueLength)
		}
		switch number {
		case 1:
			key = string(bytesValue)
		case 2:
			value = string(bytesValue)
		}
		payload = payload[valueLength:]
	}
	return key, value, nil
}

func malformedPayloadError(name string, parseCode int) error {
	return status.NewUnrecoverableError(
		"decode partial update %s body failed: %v",
		name,
		protowire.ParseError(parseCode),
	)
}

func decodePayloadError(kind string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return status.NewInner("decode partial update %s payload failed: %v", kind, err)
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
