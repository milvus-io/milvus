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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestPmsFactory(t *testing.T) {
	pmsFactory := NewPmsFactory(&Params.ServiceParam)

	err := pmsFactory.NewMsgStreamDisposer(context.Background())([]string{"hello"}, "xx")
	assert.NoError(t, err)

	tests := []struct {
		description   string
		withTimeout   bool
		ctxTimeouted  bool
		expectedError bool
	}{
		{"normal ctx", false, false, false},
		{"timeout ctx not timeout", true, false, false},
		{"timeout ctx timeout", true, true, true},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			var cancel context.CancelFunc
			ctx := context.Background()
			if test.withTimeout {
				if test.ctxTimeouted {
					ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-1*time.Minute))
				} else {
					ctx, cancel = context.WithTimeout(ctx, time.Second*10)
				}
				defer cancel()
			}
			stream, err := pmsFactory.NewMsgStream(ctx)
			if test.expectedError {
				assert.Error(t, err)
				assert.Nil(t, stream)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, stream)
			}

			ttStream, err := pmsFactory.NewTtMsgStream(ctx)
			if test.expectedError {
				assert.Error(t, err)
				assert.Nil(t, ttStream)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, ttStream)
			}
		})
	}
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

	tests := []struct {
		description   string
		withTimeout   bool
		ctxTimeouted  bool
		expectedError bool
	}{
		{"normal ctx", false, false, false},
		{"timeout ctx not timeout", true, false, false},
		{"timeout ctx timeout", true, true, true},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			var cancel context.CancelFunc
			ctx := context.Background()
			if test.withTimeout {
				if test.ctxTimeouted {
					ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-1*time.Minute))
				} else {
					ctx, cancel = context.WithTimeout(ctx, time.Second*10)
				}
				defer cancel()
			}
			stream, err := kmsFactory.NewMsgStream(ctx)
			if test.expectedError {
				assert.Error(t, err)
				assert.Nil(t, stream)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, stream)
			}

			ttStream, err := kmsFactory.NewTtMsgStream(ctx)
			if test.expectedError {
				assert.Error(t, err)
				assert.Nil(t, ttStream)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, ttStream)
			}
		})
	}
}

func TestRmsFactory(t *testing.T) {
	defer os.Unsetenv("ROCKSMQ_PATH")
	paramtable.Init()

	dir := t.TempDir()

	rmsFactory := NewRocksmqFactory(dir, &paramtable.Get().ServiceParam)

	ctx := context.Background()
	_, err := rmsFactory.NewMsgStream(ctx)
	assert.NoError(t, err)

	_, err = rmsFactory.NewTtMsgStream(ctx)
	assert.NoError(t, err)
}
