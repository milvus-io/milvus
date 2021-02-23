package components

import (
	"context"
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	grpcindexserver "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice"
)

type IndexService struct {
	svr *grpcindexserver.Server

	tracer opentracing.Tracer
	closer io.Closer
}

func NewIndexService(ctx context.Context) (*IndexService, error) {
	var err error
	s := &IndexService{}

	cfg := &config.Configuration{
		ServiceName: "indexservice",
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
	}
	s.tracer, s.closer, err = cfg.NewTracer()
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	opentracing.SetGlobalTracer(s.tracer)
	svr, err := grpcindexserver.NewServer(ctx)

	if err != nil {
		return nil, err
	}
	s.svr = svr
	return s, nil
}
func (s *IndexService) Run() error {
	if err := s.svr.Run(); err != nil {
		return err
	}
	return nil
}
func (s *IndexService) Stop() error {
	s.closer.Close()
	if err := s.svr.Stop(); err != nil {
		return err
	}
	return nil
}
