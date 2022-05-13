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

	"github.com/stretchr/testify/assert"
)

func TestPmsFactory(t *testing.T) {
	pmsFactory := NewPmsFactory(&Params.PulsarCfg)

	ctx := context.Background()
	_, err := pmsFactory.NewMsgStream(ctx)
	assert.Nil(t, err)

	_, err = pmsFactory.NewTtMsgStream(ctx)
	assert.Nil(t, err)

	_, err = pmsFactory.NewQueryMsgStream(ctx)
	assert.Nil(t, err)
}

func TestRmsFactory(t *testing.T) {
	defer os.Unsetenv("PEBBLEMQ_PATH")

	dir := t.TempDir()

	rmsFactory := NewRmsFactory(dir)

	ctx := context.Background()
	_, err := rmsFactory.NewMsgStream(ctx)
	assert.Nil(t, err)

	_, err = rmsFactory.NewTtMsgStream(ctx)
	assert.Nil(t, err)

	_, err = rmsFactory.NewQueryMsgStream(ctx)
	assert.Nil(t, err)
}

func TestKafkaFactory(t *testing.T) {
	kmsFactory := NewKmsFactory(&Params.KafkaCfg)

	ctx := context.Background()
	_, err := kmsFactory.NewMsgStream(ctx)
	assert.Nil(t, err)

	_, err = kmsFactory.NewTtMsgStream(ctx)
	assert.Nil(t, err)

	_, err = kmsFactory.NewQueryMsgStream(ctx)
	assert.Nil(t, err)
}
