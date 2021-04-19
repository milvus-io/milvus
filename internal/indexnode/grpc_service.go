package indexnode

import (
	"context"
	"errors"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
)

const (
	reqTimeoutInterval = time.Second * 10
)

func (b *Builder) BuildIndex(ctx context.Context, request *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	t := NewIndexAddTask()
	t.req = request
	t.idAllocator = b.idAllocator
	t.buildQueue = b.sched.IndexBuildQueue
	t.table = b.metaTable
	t.kv = b.kv
	var cancel func()
	t.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("insert timeout")
		default:
			return b.sched.IndexAddQueue.Enqueue(t)
		}
	}
	ret := &indexpb.BuildIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}

	err := fn()
	if err != nil {
		ret.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		ret.Status.Reason = err.Error()
		return ret, nil
	}

	err = t.WaitToFinish()
	if err != nil {
		ret.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		ret.Status.Reason = err.Error()
		return ret, nil
	}
	ret.IndexID = t.indexID
	return ret, nil
}

func (b *Builder) GetIndexStates(ctx context.Context, request *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {
	indexID := request.IndexID
	ret, err := b.metaTable.GetIndexStates(indexID)
	ret.Status = &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}
	ret.IndexID = indexID
	if err != nil {
		ret.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		ret.Status.Reason = err.Error()
	}
	return ret, nil
}

func (b *Builder) GetIndexFilePaths(ctx context.Context, request *indexpb.IndexFilePathRequest) (*indexpb.IndexFilePathsResponse, error) {
	ret := &indexpb.IndexFilePathsResponse{
		Status:  &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS},
		IndexID: request.IndexID,
	}
	filePaths, err := b.metaTable.GetIndexFilePaths(request.IndexID)
	if err != nil {
		ret.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		ret.Status.Reason = err.Error()
	}
	ret.IndexFilePaths = filePaths
	return ret, nil
}
