package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zilliztech/milvus-distributed/internal/master"
	masterParams "github.com/zilliztech/milvus-distributed/internal/master/paramtable"
	"go.uber.org/zap"
)

func main() {
	master.Init()

	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())

	etcdAddress, _ := masterParams.Params.EtcdAddress()
	etcdRootPath, _ := masterParams.Params.EtcdRootPath()
	pulsarAddr, _ := masterParams.Params.PulsarAddress()
	pulsarAddr = "pulsar://" + pulsarAddr
	defaultRecordSize := masterParams.Params.DefaultRecordSize()
	minimumAssignSize := masterParams.Params.MinimumAssignSize()
	segmentThreshold := masterParams.Params.SegmentThreshold()
	segmentExpireDuration := masterParams.Params.SegmentExpireDuration()
	numOfChannel, _ := masterParams.Params.TopicNum()
	nodeNum, _ := masterParams.Params.QueryNodeNum()
	statsChannel := masterParams.Params.StatsChannels()

	opt := master.Option{
		KVRootPath:            etcdRootPath,
		MetaRootPath:          etcdRootPath,
		EtcdAddr:              []string{etcdAddress},
		PulsarAddr:            pulsarAddr,
		ProxyIDs:              masterParams.Params.ProxyIDList(),
		PulsarProxyChannels:   masterParams.Params.ProxyTimeSyncChannels(),
		PulsarProxySubName:    masterParams.Params.ProxyTimeSyncSubName(),
		SoftTTBInterval:       masterParams.Params.SoftTimeTickBarrierInterval(),
		WriteIDs:              masterParams.Params.WriteIDList(),
		PulsarWriteChannels:   masterParams.Params.WriteTimeSyncChannels(),
		PulsarWriteSubName:    masterParams.Params.WriteTimeSyncSubName(),
		PulsarDMChannels:      masterParams.Params.DMTimeSyncChannels(),
		PulsarK2SChannels:     masterParams.Params.K2STimeSyncChannels(),
		DefaultRecordSize:     defaultRecordSize,
		MinimumAssignSize:     minimumAssignSize,
		SegmentThreshold:      segmentThreshold,
		SegmentExpireDuration: segmentExpireDuration,
		NumOfChannel:          numOfChannel,
		NumOfQueryNode:        nodeNum,
		StatsChannels:         statsChannel,
	}

	svr, err := master.CreateServer(ctx, &opt)
	if err != nil {
		log.Print("create server failed", zap.Error(err))
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

	if err := svr.Run(int64(masterParams.Params.Port())); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	<-ctx.Done()
	log.Print("Got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
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
