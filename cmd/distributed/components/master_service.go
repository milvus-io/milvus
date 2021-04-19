package components

import (
	"context"
	"fmt"
	"log"
	"time"

	ds "github.com/zilliztech/milvus-distributed/internal/dataservice"
	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"
	isc "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	ps "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice"
	psc "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice/client"
	qsc "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice/client"
	is "github.com/zilliztech/milvus-distributed/internal/indexservice"
	ms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	qs "github.com/zilliztech/milvus-distributed/internal/queryservice"
)

type MasterService struct {
	ctx context.Context
	svr *msc.GrpcServer

	proxyService *psc.Client
	dataService  *dsc.Client
	indexService *isc.Client
	queryService *qsc.Client
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
	if err = indexService.Init(); err != nil {
		return nil, err
	}

	if err = svr.SetIndexService(indexService); err != nil {
		return nil, err
	}

	qs.Params.Init()
	queryService, err := qsc.NewClient(qs.Params.Address, time.Duration(ms.Params.Timeout)*time.Second)
	if err != nil {
		return nil, err
	}
	if err = queryService.Init(); err != nil {
		return nil, err
	}
	if err = queryService.Start(); err != nil {
		return nil, err
	}
	if err = svr.SetQueryService(queryService); err != nil {
		return nil, err
	}

	return &MasterService{
		ctx: ctx,
		svr: svr,

		proxyService: proxyService,
		dataService:  dataService,
		indexService: indexService,
		queryService: queryService,
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
	if m != nil {
		if m.proxyService != nil {
			_ = m.proxyService.Stop()
		}
		if m.indexService != nil {
			_ = m.indexService.Stop()
		}
		if m.dataService != nil {
			_ = m.dataService.Stop()
		}
		if m.queryService != nil {
			_ = m.queryService.Stop()
		}
		if m.svr != nil {
			return m.svr.Stop()
		}
	}
	return nil
}
