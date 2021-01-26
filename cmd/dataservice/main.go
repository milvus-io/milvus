package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/master"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	service := dataservice.NewGrpcService(ctx)

	master.Params.Init()
	client, err := masterservice.NewGrpcClient(fmt.Sprintf("%s:%d", master.Params.Address, master.Params.Port), 30*time.Second)
	if err != nil {
		panic(err)
	}
	log.Println("master client create complete")
	if err = client.Init(); err != nil {
		panic(err)
	}
	if err = client.Start(); err != nil {
		panic(err)
	}
	service.SetMasterClient(client)
	ticker := time.NewTicker(500 * time.Millisecond)
	tctx, tcancel := context.WithTimeout(ctx, 30*time.Second)
	defer func() {
		if err = client.Stop(); err != nil {
			panic(err)
		}
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
			panic("master timeout")
		}
		if states.State.StateCode == internalpb2.StateCode_INITIALIZING || states.State.StateCode == internalpb2.StateCode_HEALTHY {
			break
		}
	}

	if err = service.Init(); err != nil {
		panic(err)
	}
	if err = service.Start(); err != nil {
		panic(err)
	}
	sc := make(chan os.Signal)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-sc
	cancel()
	if err = service.Stop(); err != nil {
		panic(err)
	}
	log.Println("shut down data service")
}
