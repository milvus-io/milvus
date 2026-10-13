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

package tikv

import (
	"os"
	"testing"

	"github.com/tikv/client-go/v2/testutils"
	tilib "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var txnClient *txnkv.Client

// pdAddrEnv names the environment variable with the PD address of a real TiKV
// for these tests. When it is not set, the tests use the mock TiKV.
const pdAddrEnv = "MILVUS_TIKV_TEST_PD"

// usingMockTiKV is true when the tests run on the mock TiKV.
var usingMockTiKV bool

// skipOnMockTiKV skips a test that needs the pessimistic transaction behavior
// of a real TiKV. The mock TiKV checks a write conflict at prewrite against
// the start timestamp even on a key that the transaction locked
// pessimistically, and real TiKV does not.
func skipOnMockTiKV(t *testing.T) {
	if usingMockTiKV {
		t.Skipf("needs a real TiKV for pessimistic locks; set %s to its PD address", pdAddrEnv)
	}
}

// creates a local TiKV Store for testing purpose.
func setupLocalTiKV() {
	setupLocalTxn()
}

func setupLocalTxn() {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	if err != nil {
		panic(err)
	}
	testutils.BootstrapWithSingleStore(cluster)
	store, err := tilib.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	if err != nil {
		panic(err)
	}
	txnClient = &txnkv.Client{KVStore: store}
}

// Connects to a remote TiKV service for testing purpose at the PD address pdsn.
func setupRemoteTiKV(pdsn string) {
	var err error
	txnClient, err = txnkv.NewClient([]string{pdsn})
	if err != nil {
		panic(err)
	}
}

func setupTiKV() {
	if pd := os.Getenv(pdAddrEnv); pd != "" {
		setupRemoteTiKV(pd)
		return
	}
	usingMockTiKV = true
	setupLocalTiKV()
}

func TestMain(m *testing.M) {
	paramtable.Init()
	setupTiKV()
	code := m.Run()
	os.Exit(code)
}
