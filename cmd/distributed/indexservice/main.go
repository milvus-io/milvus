// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	grpcindexserver "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice"
	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	grpcindexserver.Init()
	svr, err := grpcindexserver.CreateIndexServer(ctx)
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

	if err := svr.Start(); err != nil {
		log.Fatal("run builder server failed", zap.Error(err))
	}

	<-ctx.Done()
	log.Print("Got signal to exit", zap.String("signal", sig.String()))

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
