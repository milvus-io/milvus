package main

import (
	"context"
	"fmt"
	"log"

	ds "github.com/zilliztech/milvus-distributed/internal/dataservice"
	dds "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/queryservice"
)

const reTryCnt = 3

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := queryservice.NewQueryService(ctx)
	if err != nil {
		panic(err)
	}
	log.Printf("query service address : %s:%d", queryservice.Params.Address, queryservice.Params.Port)

	cnt := 0
	// init data service client
	ds.Params.Init()
	log.Printf("data service address : %s:%d", ds.Params.Address, ds.Params.Port)
	dataClient := dds.NewClient(fmt.Sprintf("%s:%d", ds.Params.Address, ds.Params.Port))
	if err = dataClient.Init(); err != nil {
		panic(err)
	}
	for cnt = 0; cnt < reTryCnt; cnt++ {
		dsStates, err := dataClient.GetComponentStates()
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

	//// init index service client
	//is.Params.Init()
	//log.Printf("index service address : %s:%d", is.Params.Address, is.Params.Port)
	//indexClient := dis.NewClient(fmt.Sprintf("%s:%d", is.Params.Address, is.Params.Port))
	//// TODO: retry to check index service status
	//
	//if err = svr(dataService); err != nil {
	//	panic(err)
	//}
	//
	//log.Printf("index service address : %s", is.Params.Address)
	//indexService := isc.NewClient(is.Params.Address)
	//
	//if err = svr.SetIndexService(indexService); err != nil {
	//	panic(err)
	//}
	//
	//if err = svr.Start(); err != nil {
	//	panic(err)
	//}
	//
	//sc := make(chan os.Signal, 1)
	//signal.Notify(sc,
	//	syscall.SIGHUP,
	//	syscall.SIGINT,
	//	syscall.SIGTERM,
	//	syscall.SIGQUIT)
	//sig := <-sc
	//log.Printf("Got %s signal to exit", sig.String())
	//_ = svr.Stop()
}
