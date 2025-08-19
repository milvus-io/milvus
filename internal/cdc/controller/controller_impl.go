package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/cdc/configuration"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/cdcpb"
)

const checkInterval = 10 * time.Second

type controller struct {
	ctx      context.Context
	wg       sync.WaitGroup
	stopOnce sync.Once
	stopChan chan struct{}
}

func NewController() Controller {
	return &controller{
		ctx:      context.Background(),
		stopChan: make(chan struct{}),
	}
}

func (c *controller) Start() {
	log.Ctx(c.ctx).Info("CDC controller started")
	timer := time.NewTicker(checkInterval)
	defer timer.Stop()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.stopChan:
				log.Ctx(c.ctx).Info("CDC controller stopped")
				return
			case <-timer.C:
				c.run()
			}
		}
	}()
}

func (c *controller) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopChan)
		c.wg.Wait()
	})
}

func (c *controller) run() {
	configs := resource.Resource().ConfigManager().GetAllConfigurations()
	for _, config := range configs {
		log.Ctx(c.ctx).Info("processing configuration...", configuration.WrapConfigLog(config)...)
		switch config.GetState() {
		case cdcpb.ReplicateState_Init:
			err := resource.Resource().ReplicateManagerClient().BroadcastReplicateConfiguration(config.GetConfiguration())
			if err != nil {
				continue
			}
			err = resource.Resource().ReplicateManagerClient().StartReplications(config.GetConfiguration())
			if err != nil {
				continue
			}
			action := configuration.UpdateState(cdcpb.ReplicateState_Running)
			resource.Resource().ConfigManager().UpdateConfiguration(config.GetReplicateID(), action)
		case cdcpb.ReplicateState_Running:
			// TODO: sheep, update metrics
		case cdcpb.ReplicateState_Failed:
			// TODO: sheep, log and update metrics
		default:
			panic(fmt.Sprintf("unknown state: %s", config.GetState()))
		}
		log.Ctx(c.ctx).Info("configuration process done", configuration.WrapConfigLog(config)...)
	}
}
