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
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/proto/pyudfpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Return a response synchronously so the test can expire the actual Execute
// context during result conversion, after its post-RPC activity check.
type completionTestWorker struct {
	pyudfpb.PyUDFWorkerClient
	response *pyudfpb.ExecuteResponse
	ctx      context.Context
}

func (w *completionTestWorker) Execute(ctx context.Context, _ *pyudfpb.ExecuteRequest, _ ...grpc.CallOption) (*pyudfpb.ExecuteResponse, error) {
	w.ctx = ctx
	return w.response, nil
}

func TestClientCompletionPreservesErrorAndContext(t *testing.T) {
	for _, outcome := range []string{"worker_error", "success"} {
		for _, completion := range []string{"active", "canceled", "deadline"} {
			t.Run(outcome+"/"+completion, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				client := &Client{rpcTimeout: time.Hour, conns: []*grpc.ClientConn{nil}}
				if completion == "deadline" {
					client.rpcTimeout = 50 * time.Millisecond
				}
				worker := &completionTestWorker{}
				defer mockey.Mock(pyudfpb.NewPyUDFWorkerClient).Return(worker).Build().UnPatch()
				finish := func() {
					switch completion {
					case "canceled":
						cancel()
						<-worker.ctx.Done()
					case "deadline":
						<-worker.ctx.Done()
					}
				}
				failure := &pyudfpb.ExecuteError{Code: pyudfpb.ErrorCode_UDF_FAILED, Message: "transform_query: model execution failed"}
				original := executionError(failure)
				allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { allocator.AssertSize(t, 0) })
				if outcome == "worker_error" {
					worker.response = &pyudfpb.ExecuteResponse{Result: &pyudfpb.ExecuteResponse_Error{Error: failure}}
					defer mockey.Mock(executionError).To(func(*pyudfpb.ExecuteError) error {
						finish()
						return original
					}).Build().UnPatch()
				} else {
					worker.response = &pyudfpb.ExecuteResponse{Result: &pyudfpb.ExecuteResponse_Outputs{Outputs: []byte("controlled result")}}
					defer mockey.Mock(decodeOutputs).To(func(context.Context, []byte) ([]*arrow.Chunked, error) {
						builder := array.NewInt64Builder(allocator)
						builder.Append(42)
						values := builder.NewArray()
						builder.Release()
						column := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{values})
						values.Release()
						finish()
						return []*arrow.Chunked{column}, nil
					}).Build().UnPatch()
				}
				outputs, err := client.Execute(ctx, clientRequest(t))
				// Keep failure paths leak-free even if an assertion detects a regression.
				defer releaseColumns(outputs)
				if completion == "active" {
					if outcome == "worker_error" {
						require.Same(t, original, err)
						require.Equal(t, merr.Code(merr.ErrFunctionFailed), merr.Code(err))
						require.Nil(t, outputs)
					} else {
						require.NoError(t, err)
						require.Len(t, outputs, 1)
						require.Equal(t, int64(42), outputs[0].Chunk(0).(*array.Int64).Value(0))
					}
					return
				}
				expected := context.Canceled
				if completion == "deadline" {
					expected = context.DeadlineExceeded
				}
				require.ErrorIs(t, err, expected)
				require.Nil(t, outputs)
				status := merr.Status(err)
				require.Equal(t, merr.Code(expected), status.Code)
				require.False(t, status.Retriable)
				require.NotEqual(t, "true", status.ExtraInfo[merr.InputErrorFlagKey])
				if outcome == "worker_error" {
					require.ErrorIs(t, err, original)
					require.ErrorIs(t, err, merr.ErrFunctionFailed)
					require.Contains(t, status.Reason, failure.Message)
				} else {
					allocator.AssertSize(t, 0) // Execute must release late successful output.
				}
			})
		}
	}
}
