package components

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	ms "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"
)

type DataService struct {
	ctx          context.Context
	server       *dataservice.Service
	masterClient *ms.GrpcClient
}

func NewDataService(ctx context.Context, factory msgstream.Factory) (*DataService, error) {
	service := dataservice.NewGrpcService(ctx, factory)

	masterservice.Params.Init()
	client, err := ms.NewGrpcClient(fmt.Sprintf("%s:%d", masterservice.Params.Address, masterservice.Params.Port), 30*time.Second)
	if err != nil {
		return nil, err
	}
	log.Println("master client create complete")
	if err = client.Init(); err != nil {
		return nil, err
	}
	if err = client.Start(); err != nil {
		return nil, err
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	tctx, tcancel := context.WithTimeout(ctx, 30*time.Second)
	defer func() {
		ticker.Stop()
		tcancel()
	}()

	for {
		var states *internalpb2.ComponentStates
		select {
		case <-ticker.C:
			states, err = client.GetComponentStates()
			if err != nil {
				continue
			}
		case <-tctx.Done():
			return nil, errors.New("master client connect timeout")
		}
		if states.State.StateCode == internalpb2.StateCode_INITIALIZING || states.State.StateCode == internalpb2.StateCode_HEALTHY {
			break
		}
	}
	service.SetMasterClient(client)

	return &DataService{
		ctx:          ctx,
		server:       service,
		masterClient: client,
	}, nil
}

func (s *DataService) Run() error {
	if err := s.server.Init(); err != nil {
		return err
	}
	if err := s.server.Start(); err != nil {
		return err
	}
	return nil
}

func (s *DataService) Stop() error {
	_ = s.masterClient.Stop()
	_ = s.server.Stop()
	return nil
}
