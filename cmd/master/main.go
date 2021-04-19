// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"syscall"
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
	svr, err := master.CreateServer(ctx)
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

	if err := svr.Run(); err != nil {
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
