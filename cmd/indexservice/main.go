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
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/cmd/distributed/components"
	"github.com/milvus-io/milvus/internal/indexservice"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/logutil"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	indexservice.Params.Init()
	logutil.SetupLogger(&indexservice.Params.Log)
	s, err := components.NewIndexService(ctx)
	if err != nil {
		log.Debug("create server failed", zap.Error(err))
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

	if err := s.Run(); err != nil {
		log.Fatal("run builder server failed", zap.Error(err))
	}

	<-ctx.Done()
	log.Debug("Got signal to exit", zap.String("signal", sig.String()))

	if err := s.Stop(); err != nil {
		log.Fatal("stop server failed", zap.Error(err))
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
