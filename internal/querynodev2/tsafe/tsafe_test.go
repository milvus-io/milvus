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

package tsafe

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

type TSafeTestSuite struct {
	suite.Suite
	tSafeReplica Manager
	channel      string
	time         Timestamp
}

func (suite *TSafeTestSuite) SetupSuite() {
	suite.channel = "test-channel"
	suite.time = uint64(time.Now().Unix())
}

func (suite *TSafeTestSuite) SetupTest() {
	suite.tSafeReplica = NewTSafeReplica()
}

// test Basic use of TSafeReplica
func (suite *TSafeTestSuite) TestBasic() {
	suite.tSafeReplica.Add(context.Background(), suite.channel, ZeroTimestamp)
	t, err := suite.tSafeReplica.Get(suite.channel)
	suite.NoError(err)
	suite.Equal(ZeroTimestamp, t)

	// Add listener
	globalWatcher := suite.tSafeReplica.WatchChannel(suite.channel)
	channelWatcher := suite.tSafeReplica.Watch()
	defer globalWatcher.Close()
	defer channelWatcher.Close()

	// Test Set tSafe
	suite.tSafeReplica.Set(suite.channel, suite.time)
	t, err = suite.tSafeReplica.Get(suite.channel)
	suite.NoError(err)
	suite.Equal(suite.time, t)

	// Test listener
	select {
	case <-globalWatcher.On():
	default:
		suite.Fail("global watcher should be triggered")
	}

	select {
	case <-channelWatcher.On():
	default:
		suite.Fail("channel watcher should be triggered")
	}
}

func (suite *TSafeTestSuite) TestRemoveAndInvalid() {
	suite.tSafeReplica.Add(context.Background(), suite.channel, ZeroTimestamp)
	t, err := suite.tSafeReplica.Get(suite.channel)
	suite.NoError(err)
	suite.Equal(ZeroTimestamp, t)

	suite.tSafeReplica.Remove(context.Background(), suite.channel)
	_, err = suite.tSafeReplica.Get(suite.channel)
	suite.Error(err)

	err = suite.tSafeReplica.Set(suite.channel, suite.time)
	suite.Error(err)
}

func TestTSafe(t *testing.T) {
	suite.Run(t, new(TSafeTestSuite))
}
