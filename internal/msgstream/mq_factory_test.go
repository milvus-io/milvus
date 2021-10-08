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

package msgstream

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPmsFactory(t *testing.T) {
	pmsFactory := NewPmsFactory()

	pulsarAddress, _ := Params.Load("_PulsarAddress")
	m := map[string]interface{}{
		"PulsarAddress":  pulsarAddress,
		"receiveBufSize": 1024,
		"pulsarBufSize":  1024,
	}
	pmsFactory.SetParams(m)

	ctx := context.Background()
	_, err := pmsFactory.NewMsgStream(ctx)
	assert.Nil(t, err)

	_, err = pmsFactory.NewTtMsgStream(ctx)
	assert.Nil(t, err)

	_, err = pmsFactory.NewQueryMsgStream(ctx)
	assert.Nil(t, err)
}

func TestPmsFactory_SetParams(t *testing.T) {
	pmsFactory := (*PmsFactory)(nil)

	pulsarAddress, _ := Params.Load("_PulsarAddress")
	m := map[string]interface{}{
		"PulsarAddress":  pulsarAddress,
		"receiveBufSize": 1024,
		"pulsarBufSize":  1024,
	}
	err := pmsFactory.SetParams(m)
	assert.NotNil(t, err)
}

func TestRmsFactory(t *testing.T) {
	os.Setenv("ROCKSMQ_PATH", "/tmp/milvus")
	defer os.Unsetenv("ROCKSMQ_PATH")

	rmsFactory := NewRmsFactory()

	m := map[string]interface{}{
		"ReceiveBufSize": 1024,
		"RmqBufSize":     1024,
	}
	rmsFactory.SetParams(m)

	ctx := context.Background()
	_, err := rmsFactory.NewMsgStream(ctx)
	assert.Nil(t, err)

	_, err = rmsFactory.NewTtMsgStream(ctx)
	assert.Nil(t, err)

	_, err = rmsFactory.NewQueryMsgStream(ctx)
	assert.Nil(t, err)
}

func TestRmsFactory_SetParams(t *testing.T) {
	rmsFactory := (*RmsFactory)(nil)

	m := map[string]interface{}{
		"ReceiveBufSize": 1024,
		"RmqBufSize":     1024,
	}
	err := rmsFactory.SetParams(m)
	assert.NotNil(t, err)
}
