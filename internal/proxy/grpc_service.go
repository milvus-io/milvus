package proxy

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"log"

	"github.com/pingcap/goleveldb/leveldb/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

func (p *Proxy) Insert(ctx context.Context, in *servicepb.RowBatch) (*servicepb.IntegerRangeResponse, error) {
	it := &InsertTask{
		baseInsertTask: baseInsertTask{
			BaseMsg: msgstream.BaseMsg{
				HashValues: in.HashKeys,
			},
			InsertRequest: internalpb.InsertRequest{
				MsgType:        internalpb.MsgType_kInsert,
				CollectionName: in.CollectionName,
				PartitionTag:   in.PartitionTag,
				RowData:        in.RowData,
			},
		},
		done:                  make(chan error),
		resultChan:            make(chan *servicepb.IntegerRangeResponse),
		manipulationMsgStream: p.manipulationMsgStream,
	}
	it.ctx, it.cancel = context.WithCancel(ctx)
	// TODO: req_id, segment_id, channel_id, proxy_id, timestamps, row_ids

	defer it.cancel()

	var t task = it
	p.taskSch.DmQueue.Enqueue(&t)
	for {
		select {
		case <-ctx.Done():
			log.Print("insert timeout!")
			return &servicepb.IntegerRangeResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
					Reason:    "insert timeout!",
				},
			}, errors.New("insert timeout!")
		case result := <-it.resultChan:
			return result, nil
		}
	}
}

func (p *Proxy) CreateCollection(ctx context.Context, req *schemapb.CollectionSchema) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (p *Proxy) DropCollection(ctx context.Context, req *servicepb.CollectionName) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (p *Proxy) HasCollection(ctx context.Context, req *servicepb.CollectionName) (*servicepb.BoolResponse, error) {
	return &servicepb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
		Value: true,
	}, nil
}

func (p *Proxy) DescribeCollection(ctx context.Context, req *servicepb.CollectionName) (*servicepb.CollectionDescription, error) {
	return &servicepb.CollectionDescription{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	}, nil
}

func (p *Proxy) ShowCollections(ctx context.Context, req *commonpb.Empty) (*servicepb.StringListResponse, error) {
	return &servicepb.StringListResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	}, nil
}

func (p *Proxy) CreatePartition(ctx context.Context, in *servicepb.PartitionName) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (p *Proxy) DropPartition(ctx context.Context, in *servicepb.PartitionName) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (p *Proxy) HasPartition(ctx context.Context, in *servicepb.PartitionName) (*servicepb.BoolResponse, error) {
	return &servicepb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
		Value: true,
	}, nil
}

func (p *Proxy) DescribePartition(ctx context.Context, in *servicepb.PartitionName) (*servicepb.PartitionDescription, error) {
	return &servicepb.PartitionDescription{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	}, nil
}

func (p *Proxy) ShowPartitions(ctx context.Context, req *servicepb.CollectionName) (*servicepb.StringListResponse, error) {
	return &servicepb.StringListResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	}, nil
}
