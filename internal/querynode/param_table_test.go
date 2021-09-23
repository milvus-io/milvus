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

package querynode

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_PulsarAddress(t *testing.T) {
	address := Params.PulsarAddress
	split := strings.Split(address, ":")
	assert.Equal(t, "pulsar", split[0])
	assert.Equal(t, "6650", split[len(split)-1])
}

func TestParamTable_minio(t *testing.T) {
	t.Run("Test endPoint", func(t *testing.T) {
		endPoint := Params.MinioEndPoint
		equal := endPoint == "localhost:9000" || endPoint == "minio:9000"
		assert.Equal(t, equal, true)
	})

	t.Run("Test accessKeyID", func(t *testing.T) {
		accessKeyID := Params.MinioAccessKeyID
		assert.Equal(t, accessKeyID, "minioadmin")
	})

	t.Run("Test secretAccessKey", func(t *testing.T) {
		secretAccessKey := Params.MinioSecretAccessKey
		assert.Equal(t, secretAccessKey, "minioadmin")
	})

	t.Run("Test useSSL", func(t *testing.T) {
		useSSL := Params.MinioUseSSLStr
		assert.Equal(t, useSSL, false)
	})
}

func TestParamTable_statsServiceTimeInterval(t *testing.T) {
	interval := Params.StatsPublishInterval
	assert.Equal(t, 1000, interval)
}

func TestParamTable_searchMsgStreamReceiveBufSize(t *testing.T) {
	bufSize := Params.SearchReceiveBufSize
	assert.Equal(t, int64(512), bufSize)
}

func TestParamTable_searchResultMsgStreamReceiveBufSize(t *testing.T) {
	bufSize := Params.SearchResultReceiveBufSize
	assert.Equal(t, int64(64), bufSize)
}

func TestParamTable_searchPulsarBufSize(t *testing.T) {
	bufSize := Params.SearchPulsarBufSize
	assert.Equal(t, int64(512), bufSize)
}

func TestParamTable_flowGraphMaxQueueLength(t *testing.T) {
	length := Params.FlowGraphMaxQueueLength
	assert.Equal(t, int32(1024), length)
}

func TestParamTable_flowGraphMaxParallelism(t *testing.T) {
	maxParallelism := Params.FlowGraphMaxParallelism
	assert.Equal(t, int32(1024), maxParallelism)
}

func TestParamTable_msgChannelSubName(t *testing.T) {
	Params.QueryNodeID = 3
	Params.initMsgChannelSubName()
	name := Params.MsgChannelSubName
	assert.Equal(t, name, "by-dev-queryNode-3")
}

func TestParamTable_statsChannelName(t *testing.T) {
	Params.Init()
	name := Params.StatsChannelName
	assert.Equal(t, name, "by-dev-query-node-stats")
}

func TestParamTable_QueryTimeTickChannel(t *testing.T) {
	Params.Init()
	ch := Params.QueryTimeTickChannelName
	assert.Equal(t, ch, "by-dev-queryTimeTick")
}

func TestParamTable_metaRootPath(t *testing.T) {
	path := Params.MetaRootPath
	fmt.Println(path)
}
