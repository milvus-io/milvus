// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expr

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type managedPyUDFRuntime interface {
	pyudf.Runtime
	fileresource.Listener
}

var globalPyUDFRuntime = mustNewGlobalPyUDFRuntime(
	func() (pyudf.Config, error) { return pyudf.NewConfig(paramtable.Get()) },
	func(ctx context.Context, config pyudf.Config) (managedPyUDFRuntime, error) {
		return pyudf.NewProductionRuntime(ctx, config)
	},
)

func mustNewGlobalPyUDFRuntime(
	newConfig func() (pyudf.Config, error),
	newRuntime func(context.Context, pyudf.Config) (managedPyUDFRuntime, error),
) managedPyUDFRuntime {
	if newConfig == nil || newRuntime == nil {
		panic(merr.WrapErrServiceInternalMsg("py_udf: global runtime dependencies are nil"))
	}

	config, err := newConfig()
	if err != nil {
		panic(merr.Wrap(err, "py_udf: initialize global runtime configuration"))
	}
	runtime, err := newRuntime(context.Background(), config)
	if err != nil {
		panic(merr.Wrap(err, "py_udf: initialize global runtime"))
	}
	if runtime == nil {
		panic(merr.WrapErrServiceInternalMsg("py_udf: global runtime initializer returned nil"))
	}
	return runtime
}

func init() {
	fileresource.RegisterListener("pyudf", globalPyUDFRuntime)
}

var (
	_ pyudf.Runtime         = globalPyUDFRuntime
	_ fileresource.Listener = globalPyUDFRuntime
)
