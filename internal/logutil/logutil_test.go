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

// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/stretchr/testify/assert"
)

func TestZapWrapper(t *testing.T) {
	proxy.Params.Init()

	SetupLogger(&proxy.Params.Log)

	wrapper := GetZapWrapper()
	assert.NotNil(t, wrapper)

	ret := wrapper.V(3)
	assert.True(t, ret)

	wrapper.Info()
	wrapper.Infoln()
	wrapper.Infof("")
	wrapper.Warning()
	wrapper.Warningln()
	wrapper.Warningf("")
	wrapper.Error()
	wrapper.Errorln()
	wrapper.Errorf("")

	// wrapper.Fatal()
	// wrapper.Fatalln()
	// wrapper.Fatalf("")
}
