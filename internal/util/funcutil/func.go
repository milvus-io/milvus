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

package funcutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/go-basic/ipv4"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func CheckGrpcReady(ctx context.Context, targetCh chan error) {
	select {
	case <-time.After(100 * time.Millisecond):
		targetCh <- nil
	case <-ctx.Done():
		return
	}
}

func CheckPortAvailable(port int) bool {
	addr := ":" + strconv.Itoa(port)
	listener, err := net.Listen("tcp", addr)
	if listener != nil {
		listener.Close()
	}
	return err == nil
}

func GetAvailablePort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

func GetLocalIP() string {
	return ipv4.LocalIP()
}

func WaitForComponentStates(ctx context.Context, service types.Component, serviceName string, states []internalpb.StateCode, attempts uint, sleep time.Duration) error {
	checkFunc := func() error {
		resp, err := service.GetComponentStates(ctx)
		if err != nil {
			return err
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}

		meet := false
		for _, state := range states {
			if resp.State.StateCode == state {
				meet = true
				break
			}
		}
		if !meet {
			msg := fmt.Sprintf("WaitForComponentStates, not meet, %s current state:%d", serviceName, resp.State.StateCode)
			return errors.New(msg)
		}
		return nil
	}
	return retry.Do(ctx, checkFunc, retry.Attempts(attempts), retry.Sleep(sleep))
}

func WaitForComponentInitOrHealthy(ctx context.Context, service types.Component, serviceName string, attempts uint, sleep time.Duration) error {
	return WaitForComponentStates(ctx, service, serviceName, []internalpb.StateCode{internalpb.StateCode_Initializing, internalpb.StateCode_Healthy}, attempts, sleep)
}

func WaitForComponentInit(ctx context.Context, service types.Component, serviceName string, attempts uint, sleep time.Duration) error {
	return WaitForComponentStates(ctx, service, serviceName, []internalpb.StateCode{internalpb.StateCode_Initializing}, attempts, sleep)
}

func WaitForComponentHealthy(ctx context.Context, service types.Component, serviceName string, attempts uint, sleep time.Duration) error {
	return WaitForComponentStates(ctx, service, serviceName, []internalpb.StateCode{internalpb.StateCode_Healthy}, attempts, sleep)
}

func ParseIndexParamsMap(mStr string) (map[string]string, error) {
	buffer := make(map[string]interface{})
	err := json.Unmarshal([]byte(mStr), &buffer)
	if err != nil {
		return nil, errors.New("Unmarshal params failed")
	}
	ret := make(map[string]string)
	for key, value := range buffer {
		valueStr := fmt.Sprintf("%v", value)
		ret[key] = valueStr
	}
	return ret, nil
}
