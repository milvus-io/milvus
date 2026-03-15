package mlog

import "go.uber.org/zap/zapcore"

// Well-known field keys for consistent logging across Milvus components.
// All keys use lowercase with underscores for readability and gRPC metadata compatibility.
const (
	keyNodeID         = "node_id"
	keyModule         = "module"
	keyTraceID        = "trace_id"
	keySpanID         = "span_id"
	keyDbID           = "db_id"
	keyDbName         = "db_name"
	keyCollectionID   = "collection_id"
	keyCollectionName = "collection_name"
	keyPartitionID    = "partition_id"
	keyPartitionName  = "partition_name"
	keySegmentID      = "segment_id"
	keyIndexID        = "index_id"
	keyFieldID        = "field_id"
	keyTaskID         = "task_id"
	keyBroadcastID    = "broadcast_id"
	keyJobID          = "job_id"
	keyBuildID        = "build_id"
	keyVChannel       = "vchannel"
	keyPChannel       = "pchannel"
	keyMessageID      = "message_id"
	keyMessage        = "message"
)

// FieldOption configures optional behavior for well-known field constructors.
type FieldOption struct {
	propagated bool
}

// OptPropagated returns a FieldOption that marks the field for RPC propagation.
// When applied, the field will be transmitted via gRPC metadata across service boundaries.
func OptPropagated() FieldOption {
	return FieldOption{propagated: true}
}

func hasPropagated(opts []FieldOption) bool {
	for _, opt := range opts {
		if opt.propagated {
			return true
		}
	}
	return false
}

// Well-known field constructors for consistent logging across Milvus components.
// These functions provide type-safe field creation with predefined keys.

// FieldNodeID creates a field for node ID.
func FieldNodeID(val int64) Field { return Int64(keyNodeID, val) }

// FieldModule creates a field for module name.
func FieldModule(val string) Field { return String(keyModule, val) }

// FieldTraceID creates a field for trace ID.
func FieldTraceID(val string) Field { return String(keyTraceID, val) }

// FieldSpanID creates a field for span ID.
func FieldSpanID(val string) Field { return String(keySpanID, val) }

// FieldDbID creates a field for database ID.
func FieldDbID(val int64, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedInt64Field(keyDbID, val)
	}
	return Int64(keyDbID, val)
}

// FieldDbName creates a field for database name.
func FieldDbName(val string, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedStringField(keyDbName, val)
	}
	return String(keyDbName, val)
}

// FieldCollectionID creates a field for collection ID.
func FieldCollectionID(val int64, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedInt64Field(keyCollectionID, val)
	}
	return Int64(keyCollectionID, val)
}

// FieldCollectionName creates a field for collection name.
func FieldCollectionName(val string, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedStringField(keyCollectionName, val)
	}
	return String(keyCollectionName, val)
}

// FieldPartitionID creates a field for partition ID.
func FieldPartitionID(val int64, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedInt64Field(keyPartitionID, val)
	}
	return Int64(keyPartitionID, val)
}

// FieldPartitionName creates a field for partition name.
func FieldPartitionName(val string, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedStringField(keyPartitionName, val)
	}
	return String(keyPartitionName, val)
}

// FieldSegmentID creates a field for segment ID.
func FieldSegmentID(val int64, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedInt64Field(keySegmentID, val)
	}
	return Int64(keySegmentID, val)
}

// FieldIndexID creates a field for index ID.
func FieldIndexID(val int64, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedInt64Field(keyIndexID, val)
	}
	return Int64(keyIndexID, val)
}

// FieldFieldID creates a field for field ID.
func FieldFieldID(val int64, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedInt64Field(keyFieldID, val)
	}
	return Int64(keyFieldID, val)
}

// FieldTaskID creates a field for task ID.
func FieldTaskID(val int64, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedInt64Field(keyTaskID, val)
	}
	return Int64(keyTaskID, val)
}

// FieldBroadcastID creates a field for broadcast ID.
func FieldBroadcastID(val int64, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedInt64Field(keyBroadcastID, val)
	}
	return Int64(keyBroadcastID, val)
}

// FieldJobID creates a field for job ID.
func FieldJobID(val int64, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedInt64Field(keyJobID, val)
	}
	return Int64(keyJobID, val)
}

// FieldBuildID creates a field for build ID.
func FieldBuildID(val int64, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedInt64Field(keyBuildID, val)
	}
	return Int64(keyBuildID, val)
}

// FieldVChannel creates a field for virtual channel name.
func FieldVChannel(val string, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedStringField(keyVChannel, val)
	}
	return String(keyVChannel, val)
}

// FieldPChannel creates a field for physical channel name.
func FieldPChannel(val string, opts ...FieldOption) Field {
	if hasPropagated(opts) {
		return propagatedStringField(keyPChannel, val)
	}
	return String(keyPChannel, val)
}

// FieldMessageID creates a field for message ID.
func FieldMessageID(val zapcore.ObjectMarshaler) Field { return Object(keyMessageID, val) }

// FieldMessage creates a field for message content.
func FieldMessage(val zapcore.ObjectMarshaler) Field { return Object(keyMessage, val) }
