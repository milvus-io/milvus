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
	"os"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

// Rmq is global rocksmq instance that will be initialized only once
var Rmq *rocksmq

// once is used to init global rocksmq
var once sync.Once

// InitRocksMQ init global rocksmq single instance
func InitRocksMQ(path string) error {
	var finalErr error
	once.Do(func() {
		log.Debug("initializing global rmq", zap.String("path", path))
		var fi os.FileInfo
		fi, finalErr = os.Stat(path)
		if os.IsNotExist(finalErr) {
			finalErr = os.MkdirAll(path, os.ModePerm)
			if finalErr != nil {
				return
			}
		} else {
			if !fi.IsDir() {
				errMsg := "can't create a directory because there exists a file with the same name"
				finalErr = errors.New(errMsg)
				return
			}
		}
		Rmq, finalErr = NewRocksMQ(path, nil)
	})
	return finalErr
}

// CloseRocksMQ is used to close global rocksmq
func CloseRocksMQ() {
	log.Debug("Close Rocksmq!")
	if Rmq != nil && Rmq.store != nil {
		Rmq.Close()
	}
}
