package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/zilliztech/milvus-distributed/internal/master"
	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
	"go.uber.org/zap"
)

func main() {

	var yamlFile string
	flag.StringVar(&yamlFile, "yaml", "", "yaml file")
	flag.Parse()
	// flag.Usage()
	log.Println("yaml file: ", yamlFile)

	err := gparams.GParams.LoadYaml(yamlFile)
	if err != nil {
		panic(err)
	}
	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	etcdAddress, _ := gparams.GParams.Load("etcd.address")
	etcdPort, _ := gparams.GParams.Load("etcd.port")
	etcdAddr := etcdAddress + ":" + etcdPort
	etcdRootPath, _ := gparams.GParams.Load("etcd.rootpath")
	svr, err := master.CreateServer(ctx, etcdRootPath, etcdRootPath, []string{etcdAddr})
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

	masterPort, _ := gparams.GParams.Load("master.port")
	grpcPort, err := strconv.ParseInt(masterPort, 10, 64)
	if err != nil {
		panic(err)
	}

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
