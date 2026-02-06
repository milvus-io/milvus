package mlog

import "go.uber.org/zap/zapcore"

// Well-known field keys for consistent logging across Milvus components.
// All keys use lowercase with underscores for readability and gRPC metadata compatibility.
const (
	KeyNodeID         = "node_id"
	KeyModule         = "module"
	KeyTraceID        = "trace_id"
	KeySpanID         = "span_id"
	KeyDbID           = "db_id"
	KeyDbName         = "db_name"
	KeyCollectionID   = "collection_id"
	KeyCollectionName = "collection_name"
	KeyPartitionID    = "partition_id"
	KeyPartitionName  = "partition_name"
	KeySegmentID      = "segment_id"
	KeyVChannel       = "vchannel"
	KeyPChannel       = "pchannel"
	KeyMessageID      = "message_id"
	KeyMessage        = "message"
)

// Well-known field constructors for consistent logging across Milvus components.
// These functions provide type-safe field creation with predefined keys.

// FieldNodeID creates a field for node ID.
func FieldNodeID(val int64) Field { return Int64(KeyNodeID, val) }

// FieldModule creates a field for module name.
func FieldModule(val string) Field { return String(KeyModule, val) }

// FieldTraceID creates a field for trace ID.
func FieldTraceID(val string) Field { return String(KeyTraceID, val) }

// FieldSpanID creates a field for span ID.
func FieldSpanID(val string) Field { return String(KeySpanID, val) }

// FieldDbID creates a field for database ID.
func FieldDbID(val int64) Field { return Int64(KeyDbID, val) }

// FieldDbName creates a field for database name.
func FieldDbName(val string) Field { return String(KeyDbName, val) }

// FieldCollectionID creates a field for collection ID.
func FieldCollectionID(val int64) Field { return Int64(KeyCollectionID, val) }

// FieldCollectionName creates a field for collection name.
func FieldCollectionName(val string) Field { return String(KeyCollectionName, val) }

// FieldPartitionID creates a field for partition ID.
func FieldPartitionID(val int64) Field { return Int64(KeyPartitionID, val) }

// FieldPartitionName creates a field for partition name.
func FieldPartitionName(val string) Field { return String(KeyPartitionName, val) }

// FieldSegmentID creates a field for segment ID.
func FieldSegmentID(val int64) Field { return Int64(KeySegmentID, val) }

// FieldVChannel creates a field for virtual channel name.
func FieldVChannel(val string) Field { return String(KeyVChannel, val) }

// FieldPChannel creates a field for physical channel name.
func FieldPChannel(val string) Field { return String(KeyPChannel, val) }

// FieldMessageID creates a field for message ID.
func FieldMessageID(val zapcore.ObjectMarshaler) Field { return Object(KeyMessageID, val) }

// FieldMessage creates a field for message content.
func FieldMessage(val zapcore.ObjectMarshaler) Field { return Object(KeyMessage, val) }
