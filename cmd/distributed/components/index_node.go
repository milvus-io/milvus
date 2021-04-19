package components

import (
	"context"
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	grpcindexnode "github.com/zilliztech/milvus-distributed/internal/distributed/indexnode"
)

type IndexNode struct {
	svr *grpcindexnode.Server

	tracer opentracing.Tracer
	closer io.Closer
}

func NewIndexNode(ctx context.Context) (*IndexNode, error) {
	var err error
	n := &IndexNode{}
	cfg := &config.Configuration{
		ServiceName: "indexnode",
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
	}
	n.tracer, n.closer, err = cfg.NewTracer()
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	opentracing.SetGlobalTracer(n.tracer)

	svr, err := grpcindexnode.NewServer(ctx)
	if err != nil {
		return nil, err
	}
	n.svr = svr
	return n, nil

}
func (n *IndexNode) Run() error {
	if err := n.svr.Run(); err != nil {
		return err
	}
	return nil
}
func (n *IndexNode) Stop() error {
	if err := n.svr.Stop(); err != nil {
		return err
	}
	return nil
}
