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

package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/stretchr/testify/assert"
)

func TestDmlChannels(t *testing.T) {
	const (
		dmlChanPrefix      = "rootcoord-dml"
		totalDmlChannelNum = 2
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory := msgstream.NewPmsFactory()
	Params.Init()

	m := map[string]interface{}{
		"pulsarAddress":  Params.PulsarAddress,
		"receiveBufSize": 1024,
		"pulsarBufSize":  1024}
	err := factory.SetParams(m)
	assert.Nil(t, err)

	dml := newDmlChannels(ctx, factory, dmlChanPrefix, totalDmlChannelNum)
	chanNames := dml.listChannels()
	assert.Equal(t, 0, len(chanNames))

	randStr := funcutil.RandomString(8)
	assert.Panics(t, func() { dml.addChannels(randStr) })
	assert.Panics(t, func() { dml.broadcast([]string{randStr}, nil) })
	assert.Panics(t, func() { dml.broadcastMark([]string{randStr}, nil) })
	assert.Panics(t, func() { dml.removeChannels(randStr) })

	// dml_xxx_0 => {chanName0, chanName2}
	// dml_xxx_1 => {chanName1}
	chanName0 := dml.getChannelName()
	dml.addChannels(chanName0)
	assert.Equal(t, 1, dml.getChannelNum())

	chanName1 := dml.getChannelName()
	dml.addChannels(chanName1)
	assert.Equal(t, 2, dml.getChannelNum())

	chanName2 := dml.getChannelName()
	dml.addChannels(chanName2)
	assert.Equal(t, 2, dml.getChannelNum())

	dml.removeChannels(chanName0)
	assert.Equal(t, 2, dml.getChannelNum())

	dml.removeChannels(chanName1)
	assert.Equal(t, 1, dml.getChannelNum())

	dml.removeChannels(chanName0)
	assert.Equal(t, 0, dml.getChannelNum())
}
