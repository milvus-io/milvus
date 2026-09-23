package walview

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
)

// MaterializeInsertRequest owns its returned body. RecoveryStorage may still
// retain text-only WAL inserts before its asynchronous pack writer materializes
// function outputs; querying that snapshot must not depend on writer timing.
func MaterializeInsertRequest(schema *schemapb.CollectionSchema, insert SegmentInsertMessage) (*msgpb.InsertRequest, error) {
	request := proto.Clone(insert.Message.MustBody()).(*msgpb.InsertRequest)
	request.PartitionID = insert.Assignment.GetPartitionId()
	request.SegmentID = insert.Assignment.GetSegmentAssignment().GetSegmentId()
	outputs, err := function.EmbeddingOutputFieldIDs(schema)
	if err != nil {
		return nil, err
	}
	if function.HasAllFieldDataByID(request.GetFieldsData(), outputs) {
		return request, nil
	}
	runners := function.NewFunctionRunnerLocalStore()
	defer runners.Close()
	if err := runners.FillEmbeddingData(request.GetCollectionID(), schema, request); err != nil {
		return nil, err
	}
	return request, nil
}
