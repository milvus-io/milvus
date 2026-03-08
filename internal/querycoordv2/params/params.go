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

package params

import (
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"

	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var Params *paramtable.ComponentParam = paramtable.Get()

var ErrFailedAllocateID = errors.New("failed to allocate ID")

// GenerateEtcdConfig returns a etcd config with a random root path,
// NOTE: for test only
func GenerateEtcdConfig() *paramtable.EtcdConfig {
	config := &Params.EtcdCfg
	rand.Seed(time.Now().UnixNano())
	suffix := "-test-querycoord" + strconv.FormatInt(rand.Int63(), 10)

	Params.Save("etcd.rootPath", config.MetaRootPath.GetValue()+suffix)
	// Due to switching etcd path mid test cases, we need to update the cached client
	// that is used by default
	kvfactory.CloseEtcdClient()

	return &Params.EtcdCfg
}

func RandomMetaRootPath() string {
	return "test-query-coord-" + strconv.FormatInt(rand.Int63(), 10)
}

func RandomIncrementIDAllocator() func() (int64, error) {
	id := rand.Int63() / 2
	return func() (int64, error) {
		return atomic.AddInt64(&id, 1), nil
	}
}

func ErrorIDAllocator() func() (int64, error) {
	return func() (int64, error) {
		return 0, ErrFailedAllocateID
	}
}
