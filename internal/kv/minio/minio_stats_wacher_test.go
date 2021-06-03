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

package miniokv

/*

import (
	"context"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func TestMinioStats(t *testing.T) {
	var p paramtable.BaseTable
	p.Init()
	endPoint, _ := p.Load("_MinioAddress")
	accessKeyID, _ := p.Load("minio.accessKeyID")
	secretAccessKey, _ := p.Load("minio.secretAccessKey")
	useSSLStr, _ := p.Load("minio.useSSL")
	useSSL, _ := strconv.ParseBool(useSSLStr)
	option := &Option{
		Address:           endPoint,
		AccessKeyID:       accessKeyID,
		SecretAccessKeyID: secretAccessKey,
		UseSSL:            useSSL,
		BucketName:        "teststats",
		CreateBucket:      true,
	}
	cli, err := NewMinIOKV(context.TODO(), option)
	assert.Nil(t, err)

	startCh := make(chan struct{})
	receiveCh := make(chan struct{})

	w := NewMinioStatsWatcher(cli.minioClient, "teststats")
	w.helper.eventAfterStartWatch = func() {
		var e struct{}
		startCh <- e
	}
	w.helper.eventAfterNotify = func() {
		var e struct{}
		receiveCh <- e
	}
	go w.StartBackground(context.TODO())

	<-startCh
	err = cli.Save("a", string([]byte{65, 65, 65, 65, 65}))
	assert.Nil(t, err)

	<-receiveCh

	size := w.GetObjectCreateSize()
	assert.EqualValues(t, 5, size)
}
*/
