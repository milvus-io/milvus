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

	nmqimplserver "github.com/milvus-io/milvus/internal/mq/mqimpl/natsmq/server"
)

func TestNmqFactory(t *testing.T) {
	dir := t.TempDir()

	nmqFactory := NewNmqFactory(dir)

	ctx := context.Background()
	_, err := nmqFactory.NewMsgStream(ctx)
	assert.Nil(t, err)

	_, err = nmqFactory.NewTtMsgStream(ctx)
	assert.Nil(t, err)

	_, err = nmqFactory.NewQueryMsgStream(ctx)
	assert.Nil(t, err)

	err = nmqFactory.NewMsgStreamDisposer(ctx)([]string{"hello"}, "xx")
	assert.Nil(t, err)

	nmqimplserver.CloseNatsMQ()
}

func TestNmqFactoryBadPath(t *testing.T) {
	badDir := "/bad bin"
	nmqFactory := NewNmqFactory(badDir)

	ctx := context.Background()
	_, err := nmqFactory.NewMsgStream(ctx)
	assert.Error(t, err)

	_, err = nmqFactory.NewTtMsgStream(ctx)
	assert.Error(t, err)

	_, err = nmqFactory.NewQueryMsgStream(ctx)
	assert.Error(t, err)

	err = nmqFactory.NewMsgStreamDisposer(ctx)([]string{"hello"}, "xx")
	assert.Error(t, err)
	nmqimplserver.CloseNatsMQ()
}
