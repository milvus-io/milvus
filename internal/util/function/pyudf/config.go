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

package pyudf

import (
	"context"
	"strconv"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Config is an immutable snapshot of embedded PyUDF runtime settings.
type Config struct {
	Enabled              bool
	LoadTimeout          time.Duration
	ExecutorThreads      int
	MaxQueueSize         int
	InstancesPerResource int
}

// NewConfig strictly parses and validates an embedded PyUDF runtime configuration snapshot.
func NewConfig(params *paramtable.ComponentParam) (Config, error) {
	if params == nil {
		return Config{}, merr.WrapErrServiceInternalMsg("py_udf: component parameters are nil")
	}

	enabledValue := params.FunctionCfg.PyUDFEnabled.GetValue()
	enabled, err := strconv.ParseBool(enabledValue)
	if err != nil {
		return Config{}, invalidConfig("enabled", enabledValue)
	}
	loadTimeoutValue := params.FunctionCfg.PyUDFLoadTimeout.GetValue()
	loadTimeout, err := time.ParseDuration(loadTimeoutValue)
	if err != nil || loadTimeout <= 0 {
		defaultValue := params.FunctionCfg.PyUDFLoadTimeout.DefaultValue
		loadTimeout, err = time.ParseDuration(defaultValue)
		if err != nil || loadTimeout <= 0 {
			return Config{}, invalidConfig("loadTimeout default", defaultValue)
		}
		mlog.Warn(context.TODO(), "invalid PyUDF load timeout, using default value",
			mlog.String("value", loadTimeoutValue),
			mlog.String("defaultValue", defaultValue))
	}
	executorThreadsValue := params.FunctionCfg.PyUDFExecutorThreads.GetValue()
	executorThreads, err := strconv.Atoi(executorThreadsValue)
	if err != nil || executorThreads <= 0 {
		return Config{}, invalidConfig("executorThreads", executorThreadsValue)
	}
	maxQueueSizeValue := params.FunctionCfg.PyUDFMaxQueueSize.GetValue()
	maxQueueSize, err := strconv.Atoi(maxQueueSizeValue)
	if err != nil || maxQueueSize < 0 {
		return Config{}, invalidConfig("maxQueueSize", maxQueueSizeValue)
	}
	instancesPerResourceValue := params.FunctionCfg.PyUDFInstancesPerResource.GetValue()
	instancesPerResource, err := strconv.Atoi(instancesPerResourceValue)
	if err != nil || instancesPerResource <= 0 {
		return Config{}, invalidConfig("instancesPerResource", instancesPerResourceValue)
	}

	return Config{
		Enabled:              enabled,
		LoadTimeout:          loadTimeout,
		ExecutorThreads:      executorThreads,
		MaxQueueSize:         maxQueueSize,
		InstancesPerResource: instancesPerResource,
	}, nil
}

func invalidConfig(name, value string) error {
	return merr.WrapErrServiceInternalMsg("py_udf: invalid function.pyUDF.%s value %q", name, value)
}
