package components

import (
	"context"
	"fmt"
	"log"

	ds "github.com/zilliztech/milvus-distributed/internal/dataservice"
	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"
	isc "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	ps "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice"
	psc "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice/client"
	is "github.com/zilliztech/milvus-distributed/internal/indexservice"
	ms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type MasterService struct {
	ctx context.Context
	svr *msc.GrpcServer

	proxyService *psc.Client
	dataService  *dsc.Client
	indexService *isc.Client
}

func NewMasterService(ctx context.Context) (*MasterService, error) {
	const reTryCnt = 3

	svr, err := msc.NewGrpcServer(ctx)
	if err != nil {
		return nil, err
	}
	log.Printf("master service address : %s:%d", ms.Params.Address, ms.Params.Port)

	cnt := 0

	ps.Params.Init()
	log.Printf("proxy service address : %s", ps.Params.ServiceAddress)
	proxyService := psc.NewClient(ps.Params.ServiceAddress)
	if err = proxyService.Init(); err != nil {
		panic(err)
	}

	for cnt = 0; cnt < reTryCnt; cnt++ {
		pxStates, err := proxyService.GetComponentStates()
		if err != nil {
			log.Printf("get state from proxy service, retry count = %d, error = %s", cnt, err.Error())
			continue
		}
		if pxStates.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			log.Printf("get state from proxy service, retry count = %d, error = %s", cnt, pxStates.Status.Reason)
			continue
		}
		if pxStates.State.StateCode != internalpb2.StateCode_INITIALIZING && pxStates.State.StateCode != internalpb2.StateCode_HEALTHY {
			continue
		}
		break
	}

	if err = svr.SetProxyService(proxyService); err != nil {
		panic(err)
	}

	ds.Params.Init()
	log.Printf("data service address : %s:%d", ds.Params.Address, ds.Params.Port)
	dataService := dsc.NewClient(fmt.Sprintf("%s:%d", ds.Params.Address, ds.Params.Port))
	if err = dataService.Init(); err != nil {
		panic(err)
	}
	if err = dataService.Start(); err != nil {
		panic(err)
	}
	for cnt = 0; cnt < reTryCnt; cnt++ {
		dsStates, err := dataService.GetComponentStates()
		if err != nil {
			log.Printf("retry cout = %d, error = %s", cnt, err.Error())
			continue
		}
		if dsStates.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			log.Printf("retry cout = %d, error = %s", cnt, dsStates.Status.Reason)
			continue
		}
		if dsStates.State.StateCode != internalpb2.StateCode_INITIALIZING && dsStates.State.StateCode != internalpb2.StateCode_HEALTHY {
			continue
		}
		break
	}
	if cnt >= reTryCnt {
		panic("connect to data service failed")
	}

	if err = svr.SetDataService(dataService); err != nil {
		panic(err)
	}

	is.Params.Init()
	log.Printf("index service address : %s", is.Params.Address)
	indexService := isc.NewClient(is.Params.Address)

	if err = svr.SetIndexService(indexService); err != nil {
		return nil, err
	}
	return &MasterService{
		ctx: ctx,
		svr: svr,

		proxyService: proxyService,
		dataService:  dataService,
		indexService: indexService,
	}, nil
}

func (m *MasterService) Run() error {
	if err := m.svr.Init(); err != nil {
		return err
	}

	if err := m.svr.Start(); err != nil {
		return err
	}
	return nil
}

func (m *MasterService) Stop() error {
	_ = m.proxyService.Stop()
	_ = m.indexService.Stop()
	_ = m.dataService.Stop()
	return m.svr.Stop()
}
