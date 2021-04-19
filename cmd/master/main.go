package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zilliztech/milvus-distributed/internal/master"
	"go.uber.org/zap"
)

func main() {
	master.Init()

	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())

	etcdAddress := master.Params.EtcdAddress()
	etcdRootPath := master.Params.EtcdRootPath()
	pulsarAddr := master.Params.PulsarAddress()
	defaultRecordSize := master.Params.DefaultRecordSize()
	minimumAssignSize := master.Params.MinimumAssignSize()
	segmentThreshold := master.Params.SegmentThreshold()
	segmentExpireDuration := master.Params.SegmentExpireDuration()
	numOfChannel := master.Params.TopicNum()
	nodeNum, _ := master.Params.QueryNodeNum()
	statsChannel := master.Params.StatsChannels()

	opt := master.Option{
		KVRootPath:            etcdRootPath,
		MetaRootPath:          etcdRootPath,
		EtcdAddr:              []string{etcdAddress},
		PulsarAddr:            pulsarAddr,
		ProxyIDs:              master.Params.ProxyIDList(),
		PulsarProxyChannels:   master.Params.ProxyTimeSyncChannels(),
		PulsarProxySubName:    master.Params.ProxyTimeSyncSubName(),
		SoftTTBInterval:       master.Params.SoftTimeTickBarrierInterval(),
		WriteIDs:              master.Params.WriteIDList(),
		PulsarWriteChannels:   master.Params.WriteTimeSyncChannels(),
		PulsarWriteSubName:    master.Params.WriteTimeSyncSubName(),
		PulsarDMChannels:      master.Params.DMTimeSyncChannels(),
		PulsarK2SChannels:     master.Params.K2STimeSyncChannels(),
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

	if err := svr.Run(int64(master.Params.Port())); err != nil {
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
