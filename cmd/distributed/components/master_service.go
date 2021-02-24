package components

import (
	"context"
	"io"

	"github.com/opentracing/opentracing-go"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type MasterService struct {
	ctx context.Context
	svr *msc.Server

	tracer opentracing.Tracer
	closer io.Closer
}

func NewMasterService(ctx context.Context, factory msgstream.Factory) (*MasterService, error) {

	svr, err := msc.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}
	return &MasterService{
		ctx: ctx,
		svr: svr,
	}, nil
}

func (m *MasterService) Run() error {
	if err := m.svr.Run(); err != nil {
		return err
	}
	return nil
}

func (m *MasterService) Stop() error {
	if err := m.svr.Stop(); err != nil {
		return err
	}
	return nil
}
