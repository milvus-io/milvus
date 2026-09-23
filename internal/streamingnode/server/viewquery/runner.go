package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type SearchTaskRunner interface {
	Search(ctx context.Context, collection *segcore.CCollection, selected []segcore.CSegment, req *querypb.SearchRequest, serverID int64) (*internalpb.SearchResults, error)
}

type QueryTaskRunner interface {
	Query(ctx context.Context, collection *segcore.CCollection, selected []segcore.CSegment, req *querypb.QueryRequest, serverID int64) (*internalpb.RetrieveResults, error)
}

type searchTaskRunner struct{}

func (searchTaskRunner) Search(ctx context.Context, collection *segcore.CCollection, selected []segcore.CSegment, req *querypb.SearchRequest, serverID int64) (*internalpb.SearchResults, error) {
	qnCollection, qnSegments, err := buildGrowingSearchTaskInputs(collection, selected, req)
	if err != nil {
		return nil, err
	}
	task := tasks.NewSearchTask(ctx, qnCollection, nil, req, serverID)
	if err := task.ExecuteOnSegments(qnSegments); err != nil {
		return nil, err
	}
	return task.SearchResult(), nil
}

type queryTaskRunner struct{}

func (queryTaskRunner) Query(ctx context.Context, collection *segcore.CCollection, selected []segcore.CSegment, req *querypb.QueryRequest, serverID int64) (*internalpb.RetrieveResults, error) {
	qnCollection, qnSegments, err := buildGrowingQueryTaskInputs(collection, selected, req)
	if err != nil {
		return nil, err
	}
	task := tasks.NewQueryTask(ctx, qnCollection, nil, req)
	if err := task.PreExecute(); err != nil {
		return nil, err
	}
	if err := task.ExecuteOnSegments(qnSegments); err != nil {
		return nil, err
	}
	return task.Result(), nil
}

func buildGrowingSearchTaskInputs(collection *segcore.CCollection, selected []segcore.CSegment, req *querypb.SearchRequest) (*segments.Collection, []segments.Segment, error) {
	legacyReq := req.GetReq()
	return buildGrowingTaskInputs(collection, selected, legacyReq.GetCollectionID(), partitionIDFromSearchRequest(legacyReq), firstDMLChannel(req.GetDmlChannels()))
}

func buildGrowingQueryTaskInputs(collection *segcore.CCollection, selected []segcore.CSegment, req *querypb.QueryRequest) (*segments.Collection, []segments.Segment, error) {
	legacyReq := req.GetReq()
	return buildGrowingTaskInputs(collection, selected, legacyReq.GetCollectionID(), partitionIDFromQueryRequest(legacyReq), firstDMLChannel(req.GetDmlChannels()))
}

func buildGrowingTaskInputs(collection *segcore.CCollection, selected []segcore.CSegment, collectionID int64, partitionID int64, vchannel string) (*segments.Collection, []segments.Segment, error) {
	qnCollection, err := segments.NewCollectionFromCCollectionForViewQuery(collection)
	if err != nil {
		return nil, nil, err
	}
	qnSegments := make([]segments.Segment, 0, len(selected))
	for _, segment := range selected {
		qnSegments = append(qnSegments, segments.NewGrowingSegmentForViewQuery(segments.ViewQueryGrowingSegmentInfo{
			CollectionID: collectionID,
			PartitionID:  partitionID,
			VChannel:     vchannel,
		}, segment))
	}
	return qnCollection, qnSegments, nil
}

func firstDMLChannel(channels []string) string {
	if len(channels) == 0 {
		return ""
	}
	return channels[0]
}

func partitionIDFromSearchRequest(req *internalpb.SearchRequest) int64 {
	if len(req.GetPartitionIDs()) == 1 {
		return req.GetPartitionIDs()[0]
	}
	return 0
}

func partitionIDFromQueryRequest(req *internalpb.RetrieveRequest) int64 {
	if len(req.GetPartitionIDs()) == 1 {
		return req.GetPartitionIDs()[0]
	}
	return 0
}
