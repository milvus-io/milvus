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
)

// Nmq is global natsmq instance that will be initialized only once
var Nmq *server.Server

// once is used to init global natsmq
var once sync.Once

// InitNatsMQ init global natsmq single instance
func InitNatsMQ(storeDir string) error {
	var finalErr error
	once.Do(func() {
		log.Debug("initializing global nmq", zap.String("storeDir", storeDir))
		opts := &server.Options{
			JetStream: true,
			StoreDir:  storeDir,
		}
		Nmq, finalErr = server.NewServer(opts)
		if finalErr != nil {
			return
		}
		go Nmq.Start()
		// Wait for server to be ready for connections
		// TODO: Make waiting time a param.
		if !Nmq.ReadyForConnections(4 * time.Second) {
			finalErr = fmt.Errorf("invalid consumer config: empty topic")
			return
		}
	})
	return finalErr
}

// CloseNatsMQ is used to close global natsmq
func CloseNatsMQ() {
	log.Debug("Closing Natsmq!")
	if Nmq != nil {
		// Shut down the server.
		Nmq.Shutdown()
		// Wait for server shutdown.
		Nmq.WaitForShutdown()
	}
}
