package typeutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type RetrieveResults interface {
	PreHandle()
	ResultEmpty() bool
	AppendFieldData(fieldData *schemapb.FieldData)
}

type segcoreResults struct {
	result *segcorepb.RetrieveResults
}

func (r *segcoreResults) PreHandle() {
	r.result.Offset = nil
	r.result.FieldsData = nil
}

func (r *segcoreResults) AppendFieldData(fieldData *schemapb.FieldData) {
	r.result.FieldsData = append(r.result.FieldsData, fieldData)
}

func (r *segcoreResults) ResultEmpty() bool {
	return typeutil.GetSizeOfIDs(r.result.GetIds()) <= 0
}

func NewSegcoreResults(result *segcorepb.RetrieveResults) RetrieveResults {
	return &segcoreResults{result: result}
}

type internalResults struct {
	result *internalpb.RetrieveResults
}

func (r *internalResults) PreHandle() {
	r.result.FieldsData = nil
}

func (r *internalResults) AppendFieldData(fieldData *schemapb.FieldData) {
	r.result.FieldsData = append(r.result.FieldsData, fieldData)
}

func (r *internalResults) ResultEmpty() bool {
	return typeutil.GetSizeOfIDs(r.result.GetIds()) <= 0
}

func NewInternalResult(result *internalpb.RetrieveResults) RetrieveResults {
	return &internalResults{result: result}
}

type milvusResults struct {
	result *milvuspb.QueryResults
}

func (r *milvusResults) PreHandle() {
	r.result.FieldsData = nil
}

func (r *milvusResults) AppendFieldData(fieldData *schemapb.FieldData) {
	r.result.FieldsData = append(r.result.FieldsData, fieldData)
}

func (r *milvusResults) ResultEmpty() bool {
	// not very clear.
	return len(r.result.GetFieldsData()) <= 0
}

func NewMilvusResult(result *milvuspb.QueryResults) RetrieveResults {
	return &milvusResults{result: result}
}
