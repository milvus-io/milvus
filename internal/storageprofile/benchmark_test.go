// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storageprofile

import (
	"context"
	"testing"
)

func BenchmarkOperationRecorder(b *testing.B) {
	attribution := Attribution{
		Component:     "querynode",
		WorkloadClass: WorkloadClassRequestPath,
		WorkloadKind:  WorkloadKindSearch,
		StorageRole:   StorageRolePersistent,
		BackendKind:   BackendKindS3Compatible,
	}
	meta := OperationMeta{
		Operation:           StorageOperationRangeRead,
		BytesRequested:      4096,
		RequestedBytesKnown: true,
	}

	b.Run("baseline", func(b *testing.B) {
		var completed uint64
		for i := 0; i < b.N; i++ {
			completed += 4096
		}
		if completed == 0 {
			b.Fatal("unreachable")
		}
	})

	b.Run("aggregate_only", func(b *testing.B) {
		ctx := WithAttribution(context.Background(), attribution)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			operation := BeginOperation(ctx, meta)
			operation.AddCompletedBytes(4096)
			operation.Finish(OperationResult{SizeKnown: true})
		}
	})

	b.Run("summary", func(b *testing.B) {
		recorder := NewRecorder(attribution)
		ctx := WithRecorder(WithAttribution(context.Background(), attribution), recorder)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			operation := BeginOperation(ctx, meta)
			operation.AddCompletedBytes(4096)
			operation.Finish(OperationResult{SizeKnown: true})
		}
		_ = recorder.Snapshot()
	})
}
