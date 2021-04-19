package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	dn "github.com/zilliztech/milvus-distributed/internal/datanode"
	dnc "github.com/zilliztech/milvus-distributed/internal/distributed/datanode"
	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

const retry = 10

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svr, err := dnc.New(ctx)
	if err != nil {
		panic(err)
	}

	log.Println("Datanode is", dn.Params.NodeID)

	// --- Master Service Client ---
	log.Println("Master service address:", dn.Params.MasterAddress)
	masterClient, err := msc.NewGrpcClient(dn.Params.MasterAddress, 20*time.Second)
	if err != nil {
		panic(err)
	}

	if err = masterClient.Init(); err != nil {
		panic(err)
	}

	if err = masterClient.Start(); err != nil {
		panic(err)
	}

	var cnt int
	for cnt = 0; cnt < retry; cnt++ {
		msStates, err := masterClient.GetComponentStates()
		if err != nil {
			continue
		}
		if msStates.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			continue
		}
		if msStates.State.StateCode != internalpb2.StateCode_HEALTHY {
			continue
		}
		break
	}
	if cnt >= retry {
		panic("Connect to master service failed")
	}

	if err := svr.SetMasterServiceInterface(masterClient); err != nil {
		panic(err)
	}

	// --- Data Service Client ---
	log.Println("Data service address: ", dn.Params.ServiceAddress)
	dataService := dsc.NewClient(dn.Params.ServiceAddress)
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
		panic("Connect to data service failed")
	}

	if err := svr.SetDataServiceInterface(dataService); err != nil {
		panic(err)
	}

	if err := svr.Init(); err != nil {
		panic(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	if err := svr.Start(); err != nil {
		panic(err)
	}

	<-ctx.Done()
	log.Println("Got signal to exit signal:", sig.String())

	svr.Stop()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	os.Exit(code)
}
