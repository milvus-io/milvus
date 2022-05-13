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

package client

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/mq/mqimpl/pebblemq/server"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/stretchr/testify/assert"

	"go.uber.org/zap"
)

func newTopicName() string {
	return fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
}

func newConsumerName() string {
	return fmt.Sprintf("my-consumer-%v", time.Now().Nanosecond())
}

func newMockPebbleMQ() server.PebbleMQ {
	var pebblemq server.PebbleMQ
	return pebblemq
}

func newMockClient() *client {
	client, _ := newClient(Options{
		Server: newMockPebbleMQ(),
	})
	return client
}

func newPebbleMQ(t *testing.T, rmqPath string) server.PebbleMQ {
	pebblePath := rmqPath
	rmq, err := server.NewPebbleMQ(pebblePath, nil)
	assert.NoError(t, err)
	return rmq
}

func removePath(rmqPath string) {
	// remove path pebblemq created
	pebblePath := rmqPath
	err := os.RemoveAll(pebblePath)
	if err != nil {
		log.Error("Failed to call os.removeAll.", zap.Any("path", pebblePath))
	}
	metaPath := rmqPath + "_meta_kv"
	err = os.RemoveAll(metaPath)
	if err != nil {
		log.Error("Failed to call os.removeAll.", zap.Any("path", metaPath))
	}
}
