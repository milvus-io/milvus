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

package msgstream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPmsFactory(t *testing.T) {
	pmsFactory := NewPmsFactory(&Params.ServiceParam)

	ctx := context.Background()
	_, err := pmsFactory.NewMsgStream(ctx)
	assert.NoError(t, err)

	_, err = pmsFactory.NewTtMsgStream(ctx)
	assert.NoError(t, err)

	err = pmsFactory.NewMsgStreamDisposer(ctx)([]string{"hello"}, "xx")
	assert.NoError(t, err)
}

func TestPmsFactoryWithAuth(t *testing.T) {
	config := &Params.ServiceParam
	Params.Save(Params.PulsarCfg.AuthPlugin.Key, "token")
	Params.Save(Params.PulsarCfg.AuthParams.Key, "token:fake_token")
	defer func() {
		Params.Save(Params.PulsarCfg.AuthPlugin.Key, "")
		Params.Save(Params.PulsarCfg.AuthParams.Key, "")
	}()
	pmsFactory := NewPmsFactory(config)

	ctx := context.Background()
	_, err := pmsFactory.NewMsgStream(ctx)
	assert.NoError(t, err)

	_, err = pmsFactory.NewTtMsgStream(ctx)
	assert.NoError(t, err)

	Params.Save(Params.PulsarCfg.AuthParams.Key, "")
	pmsFactory = NewPmsFactory(config)

	ctx = context.Background()
	_, err = pmsFactory.NewMsgStream(ctx)
	assert.Error(t, err)

	_, err = pmsFactory.NewTtMsgStream(ctx)
	assert.Error(t, err)

}

func TestKafkaFactory(t *testing.T) {
	kmsFactory := NewKmsFactory(&Params.ServiceParam)

	ctx := context.Background()
	_, err := kmsFactory.NewMsgStream(ctx)
	assert.NoError(t, err)

	_, err = kmsFactory.NewTtMsgStream(ctx)
	assert.NoError(t, err)

	// err = kmsFactory.NewMsgStreamDisposer(ctx)([]string{"hello"}, "xx")
	// assert.NoError(t, err)
}
