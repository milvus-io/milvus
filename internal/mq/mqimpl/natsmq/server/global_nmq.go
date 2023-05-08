// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// Nmq is global natsmq instance that will be initialized only once
var Nmq *server.Server

// once is used to init global natsmq
var once sync.Once

// InitNatsMQ init global natsmq single instance
func InitNatsMQ(storeDir string) error {
	var finalErr error
	once.Do(func() {
		opts, initializeTimeout := getServerOption(storeDir)
		log.Debug("try to initialize global nmq", zap.Reflect("options", opts), zap.Duration("timeout", initializeTimeout))
		Nmq, finalErr = server.NewServer(opts)
		log.Debug("initialize nmq finished", zap.Error(finalErr))
		if finalErr != nil {
			return
		}
		go Nmq.Start()
		// Wait for server to be ready for connections
		if !Nmq.ReadyForConnections(initializeTimeout) {
			finalErr = fmt.Errorf("nmq is not ready with timeout %s", initializeTimeout)
			log.Warn("nmq is not ready", zap.Duration("timeout", initializeTimeout), zap.Reflect("options", opts))
		}
	})
	return finalErr
}

// getServerOption get nats server option from config.
func getServerOption(storeDir string) (*server.Options, time.Duration) {
	params := paramtable.Get()
	opts := &server.Options{
		Host:               "127.0.0.1", // Force to use loopback address.
		Port:               params.NatsmqCfg.Server.Port.GetAsInt(),
		MaxPayload:         params.NatsmqCfg.Server.MaxPayload.GetAsInt32(),
		MaxPending:         params.NatsmqCfg.Server.MaxPending.GetAsInt64(),
		JetStream:          true,
		JetStreamMaxMemory: params.NatsmqCfg.Server.MaxMemoryStore.GetAsInt64(),
		JetStreamMaxStore:  params.NatsmqCfg.Server.MaxFileStore.GetAsInt64(),
		StoreDir:           storeDir,
		Debug:              params.NatsmqCfg.Server.Monitor.Debug.GetAsBool(),
		Logtime:            params.NatsmqCfg.Server.Monitor.LogTime.GetAsBool(),
		LogFile:            params.NatsmqCfg.Server.Monitor.LogFile.GetValue(),
		LogSizeLimit:       params.NatsmqCfg.Server.Monitor.LogSizeLimit.GetAsInt64(),
	}
	return opts, time.Duration(params.NatsmqCfg.Server.InitializeTimeout.GetAsInt()) * time.Millisecond
}

// CloseNatsMQ is used to close global natsmq
func CloseNatsMQ() {
	log.Debug("Closing Natsmq!")
	if Nmq != nil {
		// Shut down the server.
		Nmq.Shutdown()
		// Wait for server shutdown.
		Nmq.WaitForShutdown()
		Nmq = nil
	}
}
