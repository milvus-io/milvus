package components

import (
	"context"
	"fmt"
	"log"
	"time"

	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	qs "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice"

	ds "github.com/zilliztech/milvus-distributed/internal/dataservice"
	ms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/queryservice"
)

type QueryService struct {
	ctx context.Context
	svr *qs.Server

	dataService   *dsc.Client
	masterService *msc.GrpcClient
}

func NewQueryService(ctx context.Context, factory msgstream.Factory) (*QueryService, error) {
	const retry = 10
	const interval = 200

	queryservice.Params.Init()
	svr, err := qs.NewServer(ctx, factory)
	if err != nil {
		panic(err)
	}
	log.Println("Queryservice id is", queryservice.Params.QueryServiceID)

	// --- Master Service Client ---
	ms.Params.Init()
	log.Printf("Master service address: %s:%d", ms.Params.Address, ms.Params.Port)
	log.Println("Init master service client ...")
	masterService, err := msc.NewGrpcClient(fmt.Sprintf("%s:%d", ms.Params.Address, ms.Params.Port), 20*time.Second)
	if err != nil {
		panic(err)
	}

	if err = masterService.Init(); err != nil {
		panic(err)
	}

	if err = masterService.Start(); err != nil {
		panic(err)
	}

	var cnt int
	for cnt = 0; cnt < retry; cnt++ {
		time.Sleep(time.Duration(cnt*interval) * time.Millisecond)
		if cnt != 0 {
			log.Println("Master service isn't ready ...")
			log.Printf("Retrying getting master service's states in ... %v ms", interval)
		}

		msStates, err := masterService.GetComponentStates()

		if err != nil {
			continue
		}
		if msStates.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			continue
		}
		if msStates.State.StateCode != internalpb2.StateCode_HEALTHY && msStates.State.StateCode != internalpb2.StateCode_INITIALIZING {
			continue
		}
		break
	}
	if cnt >= retry {
		panic("Master service isn't ready")
	}

	if err := svr.SetMasterService(masterService); err != nil {
		panic(err)
	}

	// --- Data service client ---
	ds.Params.Init()
	log.Printf("Data service address: %s:%d", ds.Params.Address, ds.Params.Port)
	log.Println("Init data service client ...")
	dataService := dsc.NewClient(fmt.Sprintf("%s:%d", ds.Params.Address, ds.Params.Port))
	if err = dataService.Init(); err != nil {
		panic(err)
	}
	if err = dataService.Start(); err != nil {
		panic(err)
	}

	for cnt = 0; cnt < retry; cnt++ {
		dsStates, err := dataService.GetComponentStates()
		if err != nil {
			continue
		}
		if dsStates.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			continue
		}
		if dsStates.State.StateCode != internalpb2.StateCode_INITIALIZING && dsStates.State.StateCode != internalpb2.StateCode_HEALTHY {
			continue
		}
		break
	}
	if cnt >= retry {
		panic("Data service isn't ready")
	}

	if err := svr.SetDataService(dataService); err != nil {
		panic(err)
	}

	return &QueryService{
		ctx:           ctx,
		svr:           svr,
		dataService:   dataService,
		masterService: masterService,
	}, nil
}

func (qs *QueryService) Run() error {
	if err := qs.svr.Init(); err != nil {
		panic(err)
	}

	if err := qs.svr.Start(); err != nil {
		panic(err)
	}
	log.Println("Data node successfully started ...")
	return nil
}

func (qs *QueryService) Stop() error {
	_ = qs.dataService.Stop()
	_ = qs.masterService.Stop()
	return qs.svr.Stop()
}
