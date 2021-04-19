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
	ms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

const retry = 10
const interval = 200

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	svr, err := dnc.New(ctx)
	if err != nil {
		panic(err)
	}

	log.Println("Datanode is", dn.Params.NodeID)

	// --- Master Service Client ---
	ms.Params.Init()
	log.Println("Master service address:", dn.Params.MasterAddress)
	log.Println("Init master service client ...")
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
		time.Sleep(time.Duration(cnt*interval) * time.Millisecond)
		if cnt != 0 {
			log.Println("Master service isn't ready ...")
			log.Printf("Retrying getting master service's states in ... %v ms", interval)
		}

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
		panic("Master service isn't ready")
	}

	if err := svr.SetMasterServiceInterface(masterClient); err != nil {
		panic(err)
	}

	// --- Data Service Client ---
	log.Println("Data service address: ", dn.Params.ServiceAddress)
	log.Println("Init data service client ...")
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
		panic("Data service isn't ready")
	}

	if err := svr.SetDataServiceInterface(dataService); err != nil {
		panic(err)
	}

	if err := svr.Init(); err != nil {
		panic(err)
	}

	if err := svr.Start(); err != nil {
		panic(err)
	}
	log.Println("Data node successfully started ...")

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

	<-ctx.Done()
	log.Println("Got signal to exit signal:", sig.String())

	if err := svr.Stop(); err != nil {
		panic(err)
	}

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
