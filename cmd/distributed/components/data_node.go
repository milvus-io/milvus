package components

import (
	"context"
	"log"

	grpcdatanode "github.com/zilliztech/milvus-distributed/internal/distributed/datanode"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type DataNode struct {
	ctx context.Context
	svr *grpcdatanode.Server
}

func NewDataNode(ctx context.Context, factory msgstream.Factory) (*DataNode, error) {

	svr, err := grpcdatanode.New(ctx, factory)
	if err != nil {
		return nil, err
	}

	return &DataNode{
		ctx: ctx,
		svr: svr,
	}, nil
}

func (d *DataNode) Run() error {
	if err := d.svr.Run(); err != nil {
		panic(err)
	}
	log.Println("Data node successfully started ...")
	return nil
}

func (d *DataNode) Stop() error {
	if err := d.svr.Stop(); err != nil {
		return err
	}
	return nil
}
