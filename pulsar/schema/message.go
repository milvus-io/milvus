package schema

import "bytes"

type ErrorCode int32

const (
	ErrorCode_SUCCESS                 ErrorCode = 0
	ErrorCode_UNEXPECTED_ERROR        ErrorCode = 1
	ErrorCode_CONNECT_FAILED          ErrorCode = 2
	ErrorCode_PERMISSION_DENIED       ErrorCode = 3
	ErrorCode_COLLECTION_NOT_EXISTS   ErrorCode = 4
	ErrorCode_ILLEGAL_ARGUMENT        ErrorCode = 5
	ErrorCode_ILLEGAL_DIMENSION       ErrorCode = 7
	ErrorCode_ILLEGAL_INDEX_TYPE      ErrorCode = 8
	ErrorCode_ILLEGAL_COLLECTION_NAME ErrorCode = 9
	ErrorCode_ILLEGAL_TOPK            ErrorCode = 10
	ErrorCode_ILLEGAL_ROWRECORD       ErrorCode = 11
	ErrorCode_ILLEGAL_VECTOR_ID       ErrorCode = 12
	ErrorCode_ILLEGAL_SEARCH_RESULT   ErrorCode = 13
	ErrorCode_FILE_NOT_FOUND          ErrorCode = 14
	ErrorCode_META_FAILED             ErrorCode = 15
	ErrorCode_CACHE_FAILED            ErrorCode = 16
	ErrorCode_CANNOT_CREATE_FOLDER    ErrorCode = 17
	ErrorCode_CANNOT_CREATE_FILE      ErrorCode = 18
	ErrorCode_CANNOT_DELETE_FOLDER    ErrorCode = 19
	ErrorCode_CANNOT_DELETE_FILE      ErrorCode = 20
	ErrorCode_BUILD_INDEX_ERROR       ErrorCode = 21
	ErrorCode_ILLEGAL_NLIST           ErrorCode = 22
	ErrorCode_ILLEGAL_METRIC_TYPE     ErrorCode = 23
	ErrorCode_OUT_OF_MEMORY           ErrorCode = 24
)

type Status struct {
	Error_code ErrorCode
	Reason     string
}

type DataType int32

const (
	NONE         DataType = 0
	BOOL         DataType = 1
	INT8         DataType = 2
	INT16        DataType = 3
	INT32        DataType = 4
	INT64        DataType = 5
	FLOAT        DataType = 10
	DOUBLE       DataType = 11
	STRING       DataType = 20
	VectorBinary DataType = 100
	VectorFloat  DataType = 101
)

type AttrRecord struct {
	Int32Value  int32
	Int64Value  int64
	FloatValue  float32
	DoubleValue float64
}

type VectorRowRecord struct {
	FloatData  []float32
	BinaryData []byte
}

type VectorRecord struct {
	Records []*VectorRowRecord
}

type FieldValue struct {
	FieldName    string
	Type         DataType
	AttrRecord   *AttrRecord //what's the diff with VectorRecord
	VectorRecord *VectorRecord
}

type VectorParam struct {
	Json      string
	RowRecord *VectorRecord
}

type OpType int

const (
	Insert     OpType = 0
	Delete     OpType = 1
	Search     OpType = 2
	TimeSync   OpType = 3
	Key2Seg    OpType = 4
	Statistics OpType = 5
)

type Message interface {
	GetType() OpType
	Serialization() []byte
	Deserialization(serializationData []byte)
}

type InsertMsg struct {
	CollectionName string
	Fields         []*FieldValue
	EntityId       int64
	PartitionTag   string
	Timestamp      uint64
	ClientId       int64
	MsgType        OpType
}

type DeleteMsg struct {
	CollectionName string
	EntityId       int64
	Timestamp      int64
	ClientId       int64
	MsgType        OpType
}

type SearchMsg struct {
	CollectionName string
	PartitionTag   string
	VectorParam    *VectorParam
	Timestamp      int64
	ClientId       int64
	MsgType        OpType
}

type TimeSyncMsg struct {
	ClientId  int64
	Timestamp int64
	MsgType   OpType
}

type Key2SegMsg struct {
	EntityId int64
	Segments []string
	MsgType  OpType
}

func (ims *InsertMsg) GetType() OpType {
	return ims.MsgType
}

func (ims *InsertMsg) Serialization() []byte {
	var serialization_data bytes.Buffer
	return serialization_data.Bytes()
}

func (ims *InsertMsg) Deserialization(serializationData []byte) {

}

func (dms *DeleteMsg) GetType() OpType {
	return dms.MsgType
}

func (sms *SearchMsg) GetType() OpType {
	return sms.MsgType
}

func (tms *TimeSyncMsg) GetType() OpType {
	return tms.MsgType
}

func (kms *Key2SegMsg) GetType() OpType {
	return kms.MsgType
}
