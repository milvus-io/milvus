package cdc

import (
	"context"

	"github.com/milvus-io/milvus/internal/cdc/configuration"
	"github.com/milvus-io/milvus/internal/cdc/controller"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

type CDCServer struct {
	ctx context.Context

	controller    controller.Controller
	configManager configuration.Manager
	watcher       configuration.Watcher
}

// NewCDCServer will return a CDCServer.
func NewCDCServer() *CDCServer {
	return &CDCServer{
		ctx: context.Background(),
	}
}

// Init initializes the CDCServer.
func (svr *CDCServer) Init() error {
	svr.configManager = configuration.NewManager()
	svr.controller = controller.NewController()
	svr.watcher = configuration.NewWatcher()
	log.Ctx(svr.ctx).Info("CDCServer init successfully")
	return nil
}

// Start starts CDCServer.
func (svr *CDCServer) Start() error {
	svr.controller.Start()
	svr.watcher.Start()
	log.Ctx(svr.ctx).Info("CDCServer start successfully")
	return nil
}

// Stop stops CDCServer.
func (svr *CDCServer) Stop() error {
	svr.controller.Stop()
	svr.watcher.Stop()
	log.Ctx(svr.ctx).Info("CDCServer stop successfully")
	return nil
}
