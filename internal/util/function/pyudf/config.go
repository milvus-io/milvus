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
	"math"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Config is an effective, restart-only configuration snapshot. Client connection
// count is not a concurrency limit; only the worker gRPC server limits RPCs.
type Config struct {
	Enabled            bool
	Address            string
	RPCTimeout         time.Duration
	ConnectionPoolSize int
	MaxMessageBytes    int
	Server             ServerConfig
}

type ServerConfig struct {
	WorkerCount       int
	GRPCConcurrency   int
	MaxConcurrentRPCs int
	ShutdownTimeout   time.Duration
}

const (
	disabledReason       = "function.pyUDF.enabled is false"
	MaxErrorMessageBytes = 8 << 10
	minRPCTimeout        = 5 * time.Second
	maxRPCTimeout        = 300 * time.Second
	minShutdownTimeout   = 5 * time.Second
	maxShutdownTimeout   = 60 * time.Second
)

type runtimeConfiguration struct {
	once  sync.Once
	value atomic.Pointer[Config]
	err   error
}

var processConfig = &runtimeConfiguration{}

// initialize freezes configuration during Proxy startup, including disabled
// state. A failed initialization requires a process restart as well.
func (r *runtimeConfiguration) initialize(ctx context.Context) error {
	r.once.Do(func() {
		config := Config{}
		if configValue(ctx, &paramtable.Get().FunctionCfg.PyUDFEnabled, strconv.ParseBool) {
			config, r.err = NewConfig(ctx)
			if r.err != nil {
				return
			}
		}
		r.value.Store(&config)
	})
	return r.err
}

// RuntimeConfig returns a copy of the startup configuration. Request execution
// must not reread paramtable: the Python workers retain their startup settings.
func RuntimeConfig() (Config, error) {
	if config := processConfig.value.Load(); config != nil {
		return *config, nil
	}
	return Config{}, merr.WrapErrServiceUnavailableMsg("py_udf: runtime configuration is not initialized")
}

// configValue logs invalid settings before replacing them with their defaults.
// Parse the original string so paramtable's typed getters cannot hide errors.
func configValue[T any](ctx context.Context, item *paramtable.ParamItem, parse func(string) (T, error)) T {
	raw := item.GetValue()
	value, err := parse(raw)
	if err != nil {
		mlog.Error(ctx, "invalid PyUDF configuration, using default",
			mlog.String("key", item.Key), mlog.String("value", raw),
			mlog.String("default", item.DefaultValue), mlog.Err(err))
		value, _ = parse(item.DefaultValue)
	}
	return value
}

func configInt(ctx context.Context, item *paramtable.ParamItem, minimum, maximum int) int {
	return configValue(ctx, item, func(raw string) (int, error) {
		value, err := strconv.Atoi(raw)
		if err != nil {
			return 0, err
		}
		if value < minimum || value > maximum {
			return 0, merr.WrapErrServiceInternalMsg("py_udf: %s must be in [%d, %d]", item.Key, minimum, maximum)
		}
		return value, nil
	})
}

func configDuration(ctx context.Context, item *paramtable.ParamItem, minimum, maximum time.Duration) time.Duration {
	return configValue(ctx, item, func(raw string) (time.Duration, error) {
		value, err := time.ParseDuration(raw)
		if err != nil {
			return 0, err
		}
		return value, validateDuration(item.Key, value, minimum, maximum)
	})
}

// NewConfig logs malformed or out-of-range settings and uses their defaults.
func NewConfig(ctx context.Context) (Config, error) {
	f := &paramtable.Get().FunctionCfg
	c := Config{
		Enabled: configValue(ctx, &f.PyUDFEnabled, strconv.ParseBool),
		Address: configValue(ctx, &f.PyUDFAddress, func(raw string) (string, error) {
			return raw, validateAddress(raw)
		}),
		RPCTimeout:         configDuration(ctx, &f.PyUDFRPCTimeout, minRPCTimeout, maxRPCTimeout),
		ConnectionPoolSize: configInt(ctx, &f.PyUDFConnectionPoolSize, 1, 1024),
		MaxMessageBytes:    configInt(ctx, &f.PyUDFMaxMessageBytes, 1<<20, 1<<30),
		Server: ServerConfig{
			WorkerCount:       configInt(ctx, &f.PyUDFWorkerCount, 1, 256),
			GRPCConcurrency:   configInt(ctx, &f.PyUDFGRPCConcurrency, 1, 1024),
			MaxConcurrentRPCs: configInt(ctx, &f.PyUDFMaxConcurrentRPCs, 1, 65536),
			ShutdownTimeout:   configDuration(ctx, &f.PyUDFShutdownTimeout, minShutdownTimeout, maxShutdownTimeout),
		},
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Validate checks representation/range constraints. A successful local bind in
// P3 will verify that Address actually belongs to this host.
func (c Config) Validate() error {
	if err := validateAddress(c.Address); err != nil {
		return err
	}
	for _, item := range []struct {
		name  string
		value int
	}{
		{"connectionPoolSize", c.ConnectionPoolSize},
		{"server.workerCount", c.Server.WorkerCount},
		{"server.grpcConcurrency", c.Server.GRPCConcurrency},
		{"server.maxConcurrentRPCs", c.Server.MaxConcurrentRPCs},
	} {
		if item.value < 1 || int64(item.value) > math.MaxInt32 {
			return merr.WrapErrServiceInternalMsg("py_udf: %s must be a positive int32", item.name)
		}
	}
	if c.MaxMessageBytes < 1<<20 || c.MaxMessageBytes > 1<<30 {
		return merr.WrapErrServiceInternalMsg("py_udf: maxMessageBytes must be between 1 MiB and 1 GiB")
	}
	if err := validateDuration("rpcTimeout", c.RPCTimeout, minRPCTimeout, maxRPCTimeout); err != nil {
		return err
	}
	return validateDuration("server.shutdownTimeout", c.Server.ShutdownTimeout, minShutdownTimeout, maxShutdownTimeout)
}

func validateAddress(address string) error {
	endpoint, err := netip.ParseAddrPort(address)
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "py_udf: invalid address")
	}
	ip := endpoint.Addr()
	if !ip.Is4() || ip.IsUnspecified() || ip.IsMulticast() ||
		ip == netip.AddrFrom4([4]byte{255, 255, 255, 255}) ||
		endpoint.Port() == 0 || endpoint.String() != address {
		return merr.WrapErrServiceInternalMsg("py_udf: address requires a concrete IPv4 literal and canonical port in 1..65535")
	}
	return nil
}

func validateDuration(name string, value, minimum, maximum time.Duration) error {
	if value < minimum || value > maximum || value%time.Millisecond != 0 {
		return merr.WrapErrServiceInternalMsg("py_udf: %s must be in [%s, %s] and use whole milliseconds", name, minimum, maximum)
	}
	return nil
}

// SupervisorArgs returns module arguments for exec.Command (never shell text).
func (c Config) SupervisorArgs() ([]string, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return []string{
		"--address", c.Address,
		"--worker-count", strconv.Itoa(c.Server.WorkerCount),
		"--grpc-concurrency", strconv.Itoa(c.Server.GRPCConcurrency),
		"--max-concurrent-rpcs", strconv.Itoa(c.Server.MaxConcurrentRPCs),
		"--max-message-bytes", strconv.Itoa(c.MaxMessageBytes),
		"--shutdown-timeout-ms", strconv.FormatInt(c.Server.ShutdownTimeout.Milliseconds(), 10),
	}, nil
}

// CheckEnabled keeps the disabled path independent of worker-only settings.
func CheckEnabled() error {
	config, err := RuntimeConfig()
	if err != nil {
		return err
	}
	if !config.Enabled {
		return merr.WrapErrParameterInvalidMsg("py_udf: %s", disabledReason)
	}
	return nil
}
