package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master"
	"go.uber.org/zap"
)

func main() {

	var yamlFile string
	flag.StringVar(&yamlFile, "yaml", "", "yaml file")
	flag.Parse()
	// flag.Usage()
	log.Println("yaml file: ", yamlFile)
	conf.LoadConfig(yamlFile)

	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	etcdAddr := conf.Config.Etcd.Address
	etcdAddr += ":"
	etcdAddr += strconv.FormatInt(int64(conf.Config.Etcd.Port), 10)

	svr, err := master.CreateServer(ctx, conf.Config.Etcd.Rootpath, conf.Config.Etcd.Rootpath, conf.Config.Etcd.Rootpath, []string{etcdAddr})
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

	grpcPort := int64(conf.Config.Master.Port)

	if err := svr.Run(grpcPort); err != nil {
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
