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

package componentutil

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

// WaitForComponentStates wait for component's state to be one of the specific states
func WaitForComponentStates[T interface {
	GetComponentStates(ctx context.Context, _ *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error)
}](ctx context.Context, client T, serviceName string, states []commonpb.StateCode, attempts uint, sleep time.Duration) error {
	checkFunc := func() error {
		resp, err := client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
		if err != nil {
			return err
		}

		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return merr.Error(resp.GetStatus())
		}

		meet := false
		for _, state := range states {
			if resp.State.StateCode == state {
				meet = true
				break
			}
		}
		if !meet {
			return fmt.Errorf(
				"WaitForComponentStates, not meet, %s current state: %s",
				serviceName,
				resp.State.StateCode.String())
		}
		log.Info("WaitForComponentStates success", zap.String("current state", resp.State.StateCode.String()))
		return nil
	}
	return retry.Do(ctx, checkFunc, retry.Attempts(attempts), retry.Sleep(sleep))
}

// WaitForComponentInitOrHealthy wait for component's state to be initializing or healthy
func WaitForComponentInitOrHealthy[T interface {
	GetComponentStates(ctx context.Context, _ *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error)
}](ctx context.Context, client T, serviceName string, attempts uint, sleep time.Duration) error {
	return WaitForComponentStates(ctx, client, serviceName, []commonpb.StateCode{commonpb.StateCode_Initializing, commonpb.StateCode_Healthy}, attempts, sleep)
}

// WaitForComponentInit wait for component's state to be initializing
func WaitForComponentInit[T interface {
	GetComponentStates(ctx context.Context, _ *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error)
}](ctx context.Context, client T, serviceName string, attempts uint, sleep time.Duration) error {
	return WaitForComponentStates(ctx, client, serviceName, []commonpb.StateCode{commonpb.StateCode_Initializing}, attempts, sleep)
}

// WaitForComponentHealthy wait for component's state to be healthy
func WaitForComponentHealthy[T interface {
	GetComponentStates(ctx context.Context, _ *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error)
}](ctx context.Context, client T, serviceName string, attempts uint, sleep time.Duration) error {
	return WaitForComponentStates(ctx, client, serviceName, []commonpb.StateCode{commonpb.StateCode_Healthy}, attempts, sleep)
}

func CheckHealthRespWithErr(err error) *milvuspb.CheckHealthResponse {
	if err != nil {
		return CheckHealthRespWithErrMsg(err.Error())
	}
	return CheckHealthRespWithErrMsg()
}

func CheckHealthRespWithErrMsg(errMsg ...string) *milvuspb.CheckHealthResponse {
	if len(errMsg) != 0 {
		return &milvuspb.CheckHealthResponse{Status: merr.Success(), IsHealthy: false, Reasons: errMsg}
	}
	return &milvuspb.CheckHealthResponse{Status: merr.Success(), IsHealthy: true, Reasons: []string{}}
}
