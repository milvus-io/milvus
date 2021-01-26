package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	ds "github.com/zilliztech/milvus-distributed/internal/dataservice"
	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"
	isc "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	psc "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice"
	is "github.com/zilliztech/milvus-distributed/internal/indexservice"
	ms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

const reTryCnt = 3

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svr, err := msc.NewGrpcServer(ctx)
	if err != nil {
		panic(err)
	}
	log.Printf("master service address : %s:%d", ms.Params.Address, ms.Params.Port)

	cnt := 0

	psc.Params.Init()
	log.Printf("proxy service address : %s", psc.Params.ServiceAddress)
	proxyService := psc.NewClient(psc.Params.ServiceAddress)
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
		panic(err)
	}

	if err = svr.Init(); err != nil {
		panic(err)
	}

	if err = svr.Start(); err != nil {
		panic(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sc
	log.Printf("Got %s signal to exit", sig.String())
	_ = indexService.Stop()
	_ = dataService.Stop()
	_ = svr.Stop()
}
