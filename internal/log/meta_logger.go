package log

import (
	"encoding/json"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"go.uber.org/zap"
)

type Operator string

const (
	// CreateCollection operator
	Creator Operator = "create"
	Update  Operator = "update"
	Delete  Operator = "delete"
	Insert  Operator = "insert"
	Sealed  Operator = "sealed"
)

type MetaLogger struct {
	fields []zap.Field
	logger *zap.Logger
}

func NewMetaLogger() *MetaLogger {
	l := infoL()
	fields := []zap.Field{zap.Bool("MetaLogInfo", true)}
	return &MetaLogger{
		fields: fields,
		logger: l,
	}
}

func (m *MetaLogger) WithCollectionMeta(coll *model.Collection) *MetaLogger {
	payload, _ := json.Marshal(coll)
	m.fields = append(m.fields, zap.Binary("CollectionMeta", payload))
	return m
}

func (m *MetaLogger) WithIndexMeta(idx *model.Index) *MetaLogger {
	payload, _ := json.Marshal(idx)
	m.fields = append(m.fields, zap.Binary("IndexMeta", payload))
	return m
}

func (m *MetaLogger) WithCollectionID(collID int64) *MetaLogger {
	m.fields = append(m.fields, zap.Int64("CollectionID", collID))
	return m
}

func (m *MetaLogger) WithCollectionName(collname string) *MetaLogger {
	m.fields = append(m.fields, zap.String("CollectionName", collname))
	return m
}

func (m *MetaLogger) WithPartitionID(partID int64) *MetaLogger {
	m.fields = append(m.fields, zap.Int64("PartitionID", partID))
	return m
}

func (m *MetaLogger) WithPartitionName(partName string) *MetaLogger {
	m.fields = append(m.fields, zap.String("PartitionName", partName))
	return m
}

func (m *MetaLogger) WithFieldID(fieldID int64) *MetaLogger {
	m.fields = append(m.fields, zap.Int64("FieldID", fieldID))
	return m
}

func (m *MetaLogger) WithFieldName(fieldName string) *MetaLogger {
	m.fields = append(m.fields, zap.String("FieldName", fieldName))
	return m
}

func (m *MetaLogger) WithIndexID(idxID int64) *MetaLogger {
	m.fields = append(m.fields, zap.Int64("IndexID", idxID))
	return m
}

func (m *MetaLogger) WithIndexName(idxName string) *MetaLogger {
	m.fields = append(m.fields, zap.String("IndexName", idxName))
	return m
}

func (m *MetaLogger) WithBuildID(buildID int64) *MetaLogger {
	m.fields = append(m.fields, zap.Int64("BuildID", buildID))
	return m
}

func (m *MetaLogger) WithBuildIDS(buildIDs []int64) *MetaLogger {
	m.fields = append(m.fields, zap.Int64s("BuildIDs", buildIDs))
	return m
}

func (m *MetaLogger) WithSegmentID(segID int64) *MetaLogger {
	m.fields = append(m.fields, zap.Int64("SegmentID", segID))
	return m
}

func (m *MetaLogger) WithIndexFiles(files []string) *MetaLogger {
	m.fields = append(m.fields, zap.Strings("IndexFiles", files))
	return m
}

func (m *MetaLogger) WithIndexVersion(version int64) *MetaLogger {
	m.fields = append(m.fields, zap.Int64("IndexVersion", version))
	return m
}

func (m *MetaLogger) WithTSO(tso uint64) *MetaLogger {
	m.fields = append(m.fields, zap.Uint64("TSO", tso))
	return m
}

func (m *MetaLogger) WithAlias(alias string) *MetaLogger {
	m.fields = append(m.fields, zap.String("Alias", alias))
	return m
}

func (m *MetaLogger) WithOperation(op MetaOperation) *MetaLogger {
	m.fields = append(m.fields, zap.Int("Operation", int(op)))
	return m
}

func (m *MetaLogger) Info() {
	m.logger.Info("", m.fields...)
}
