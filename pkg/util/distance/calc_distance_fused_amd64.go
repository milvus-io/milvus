// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under
// the License.

package distance

import (
	"context"
	"math"

	"golang.org/x/sys/cpu"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/distance/asm"
)

// Fused decoder kernels, same hook pattern as the float32 L2/IP/Cosine SIMD
// path. The pure-Go implementations in calc_distance.go remain the fallback on
// machines without the required instruction sets. Milvus's minimum CPU
// requirement (SSE4.2) does not guarantee F16C, so the detection is mandatory.
func init() {
	if cpu.X86.HasAVX2 {
		mlog.Info(context.TODO(), "Hook AVX2 for fused int8/bfloat16 distance kernels")
		int8FloatDistanceImpl = simdInt8FloatDistance
		bfloat16FloatDistanceImpl = simdBFloat16FloatDistance
	}
	if asm.HasF16C() && cpu.X86.HasAVX2 {
		mlog.Info(context.TODO(), "Hook F16C for fused float16 distance kernel")
		float16FloatDistanceImpl = simdFloat16FloatDistance
	}
}

// simdFloat16FloatDistance decodes each row into a reused dim-sized float32
// buffer (F16C) and scores it with the SIMD float32 kernels, so no per-block
// []float32 intermediate is materialized.
func simdFloat16FloatDistance(dim int64, left []float32, right []byte, metric string) ([]float32, error) {
	metricUpper, rightNum, err := validateDecodedDistanceArgs(dim, left, right, dim*2, metric)
	if err != nil {
		return nil, err
	}
	rowBuf := make([]float32, dim)
	distances := make([]float32, rightNum)
	normL := l2Norm(left)
	for row := int64(0); row < rightNum; row++ {
		base := row * dim * 2
		asm.DecodeFP16ToFloat32(rowBuf, right[base:base+dim*2])
		distances[row] = denseSIMDScore(metricUpper, left, rowBuf, normL)
	}
	return distances, nil
}

// simdInt8FloatDistance is the AVX2 fused int8 kernel.
func simdInt8FloatDistance(dim int64, left []float32, right []byte, metric string) ([]float32, error) {
	metricUpper, rightNum, err := validateDecodedDistanceArgs(dim, left, right, dim, metric)
	if err != nil {
		return nil, err
	}
	rowBuf := make([]float32, dim)
	distances := make([]float32, rightNum)
	normL := l2Norm(left)
	for row := int64(0); row < rightNum; row++ {
		base := row * dim
		asm.DecodeInt8ToFloat32(rowBuf, right[base:base+dim])
		distances[row] = denseSIMDScore(metricUpper, left, rowBuf, normL)
	}
	return distances, nil
}

// simdBFloat16FloatDistance is the AVX2 fused bfloat16 kernel.
func simdBFloat16FloatDistance(dim int64, left []float32, right []byte, metric string) ([]float32, error) {
	metricUpper, rightNum, err := validateDecodedDistanceArgs(dim, left, right, dim*2, metric)
	if err != nil {
		return nil, err
	}
	rowBuf := make([]float32, dim)
	distances := make([]float32, rightNum)
	normL := l2Norm(left)
	for row := int64(0); row < rightNum; row++ {
		base := row * dim * 2
		asm.DecodeBFloat16ToFloat32(rowBuf, right[base:base+dim*2])
		distances[row] = denseSIMDScore(metricUpper, left, rowBuf, normL)
	}
	return distances, nil
}

// denseSIMDScore scores one decoded row against the reference using the SIMD
// float32 kernels, reusing asm.IP for IP/COSINE and asm.L2 for L2.
func denseSIMDScore(metric string, left, row []float32, normL float32) float32 {
	switch metric {
	case L2:
		return asm.L2(row, left)
	case IP:
		return asm.IP(row, left)
	case COSINE:
		ip := asm.IP(row, left)
		if normL == 0 {
			return 0
		}
		normR := asm.IP(row, row)
		if normR == 0 {
			return 0
		}
		return ip / float32(math.Sqrt(float64(normL)*float64(normR)))
	default:
		return 0
	}
}

func l2Norm(v []float32) float32 {
	return asm.IP(v, v)
}
