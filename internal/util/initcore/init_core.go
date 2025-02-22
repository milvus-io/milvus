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

package initcore

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include <stdint.h>
#include "common/init_c.h"
#include "segcore/segcore_init_c.h"
#include "storage/storage_c.h"
*/
import "C"

import (
	"fmt"
	"path"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func InitLocalChunkManager(path string) {
	CLocalRootPath := C.CString(path)
	defer C.free(unsafe.Pointer(CLocalRootPath))
	C.InitLocalChunkManagerSingleton(CLocalRootPath)
}

func InitTraceConfig(params *paramtable.ComponentParam) {
	sampleFraction := C.float(params.TraceCfg.SampleFraction.GetAsFloat())
	nodeID := C.int(paramtable.GetNodeID())
	exporter := C.CString(params.TraceCfg.Exporter.GetValue())
	jaegerURL := C.CString(params.TraceCfg.JaegerURL.GetValue())
	otlpMethod := C.CString(params.TraceCfg.OtlpMethod.GetValue())
	endpoint := C.CString(params.TraceCfg.OtlpEndpoint.GetValue())
	otlpSecure := params.TraceCfg.OtlpSecure.GetAsBool()
	defer C.free(unsafe.Pointer(exporter))
	defer C.free(unsafe.Pointer(jaegerURL))
	defer C.free(unsafe.Pointer(endpoint))
	defer C.free(unsafe.Pointer(otlpMethod))

	config := C.CTraceConfig{
		exporter:       exporter,
		sampleFraction: sampleFraction,
		jaegerURL:      jaegerURL,
		otlpEndpoint:   endpoint,
		otlpMethod:     otlpMethod,
		oltpSecure:     (C.bool)(otlpSecure),
		nodeID:         nodeID,
	}
	// oltp grpc may hangs forever, add timeout logic at go side
	timeout := params.TraceCfg.InitTimeoutSeconds.GetAsDuration(time.Second)
	callWithTimeout(func() {
		C.InitTrace(&config)
	}, func() {
		panic("init segcore tracing timeout, See issue #33483")
	}, timeout)
}

func ResetTraceConfig(params *paramtable.ComponentParam) {
	sampleFraction := C.float(params.TraceCfg.SampleFraction.GetAsFloat())
	nodeID := C.int(paramtable.GetNodeID())
	exporter := C.CString(params.TraceCfg.Exporter.GetValue())
	jaegerURL := C.CString(params.TraceCfg.JaegerURL.GetValue())
	endpoint := C.CString(params.TraceCfg.OtlpEndpoint.GetValue())
	otlpMethod := C.CString(params.TraceCfg.OtlpMethod.GetValue())
	otlpSecure := params.TraceCfg.OtlpSecure.GetAsBool()
	defer C.free(unsafe.Pointer(exporter))
	defer C.free(unsafe.Pointer(jaegerURL))
	defer C.free(unsafe.Pointer(endpoint))
	defer C.free(unsafe.Pointer(otlpMethod))

	config := C.CTraceConfig{
		exporter:       exporter,
		sampleFraction: sampleFraction,
		jaegerURL:      jaegerURL,
		otlpEndpoint:   endpoint,
		otlpMethod:     otlpMethod,
		oltpSecure:     (C.bool)(otlpSecure),
		nodeID:         nodeID,
	}

	// oltp grpc may hangs forever, add timeout logic at go side
	timeout := params.TraceCfg.InitTimeoutSeconds.GetAsDuration(time.Second)
	callWithTimeout(func() {
		C.SetTrace(&config)
	}, func() {
		panic("set segcore tracing timeout, See issue #33483")
	}, timeout)
}

func callWithTimeout(fn func(), timeoutHandler func(), timeout time.Duration) {
	if timeout > 0 {
		ch := make(chan struct{})
		go func() {
			defer close(ch)
			fn()
		}()
		select {
		case <-ch:
		case <-time.After(timeout):
			timeoutHandler()
		}
	} else {
		fn()
	}
}

func InitRemoteChunkManager(params *paramtable.ComponentParam) error {
	cAddress := C.CString(params.MinioCfg.Address.GetValue())
	cBucketName := C.CString(params.MinioCfg.BucketName.GetValue())
	cAccessKey := C.CString(params.MinioCfg.AccessKeyID.GetValue())
	cAccessValue := C.CString(params.MinioCfg.SecretAccessKey.GetValue())
	cRootPath := C.CString(params.MinioCfg.RootPath.GetValue())
	cStorageType := C.CString(params.CommonCfg.StorageType.GetValue())
	cIamEndPoint := C.CString(params.MinioCfg.IAMEndpoint.GetValue())
	cCloudProvider := C.CString(params.MinioCfg.CloudProvider.GetValue())
	cLogLevel := C.CString(params.MinioCfg.LogLevel.GetValue())
	cRegion := C.CString(params.MinioCfg.Region.GetValue())
	cSslCACert := C.CString(params.MinioCfg.SslCACert.GetValue())
	cGcpCredentialJSON := C.CString(params.MinioCfg.GcpCredentialJSON.GetValue())
	defer C.free(unsafe.Pointer(cAddress))
	defer C.free(unsafe.Pointer(cBucketName))
	defer C.free(unsafe.Pointer(cAccessKey))
	defer C.free(unsafe.Pointer(cAccessValue))
	defer C.free(unsafe.Pointer(cRootPath))
	defer C.free(unsafe.Pointer(cStorageType))
	defer C.free(unsafe.Pointer(cIamEndPoint))
	defer C.free(unsafe.Pointer(cLogLevel))
	defer C.free(unsafe.Pointer(cRegion))
	defer C.free(unsafe.Pointer(cCloudProvider))
	defer C.free(unsafe.Pointer(cSslCACert))
	defer C.free(unsafe.Pointer(cGcpCredentialJSON))
	storageConfig := C.CStorageConfig{
		address:             cAddress,
		bucket_name:         cBucketName,
		access_key_id:       cAccessKey,
		access_key_value:    cAccessValue,
		root_path:           cRootPath,
		storage_type:        cStorageType,
		iam_endpoint:        cIamEndPoint,
		cloud_provider:      cCloudProvider,
		useSSL:              C.bool(params.MinioCfg.UseSSL.GetAsBool()),
		sslCACert:           cSslCACert,
		useIAM:              C.bool(params.MinioCfg.UseIAM.GetAsBool()),
		log_level:           cLogLevel,
		region:              cRegion,
		useVirtualHost:      C.bool(params.MinioCfg.UseVirtualHost.GetAsBool()),
		requestTimeoutMs:    C.int64_t(params.MinioCfg.RequestTimeoutMs.GetAsInt64()),
		gcp_credential_json: cGcpCredentialJSON,
	}

	status := C.InitRemoteChunkManagerSingleton(storageConfig)
	return HandleCStatus(&status, "InitRemoteChunkManagerSingleton failed")
}

func InitMmapManager(params *paramtable.ComponentParam) error {
	mmapDirPath := params.QueryNodeCfg.MmapDirPath.GetValue()
	cMmapChunkManagerDir := C.CString(path.Join(mmapDirPath, "/mmap_chunk_manager/"))
	cCacheReadAheadPolicy := C.CString(params.QueryNodeCfg.ReadAheadPolicy.GetValue())
	defer C.free(unsafe.Pointer(cMmapChunkManagerDir))
	defer C.free(unsafe.Pointer(cCacheReadAheadPolicy))
	diskCapacity := params.QueryNodeCfg.DiskCapacityLimit.GetAsUint64()
	diskLimit := uint64(float64(params.QueryNodeCfg.MaxMmapDiskPercentageForMmapManager.GetAsUint64()*diskCapacity) * 0.01)
	mmapFileSize := params.QueryNodeCfg.FixedFileSizeForMmapManager.GetAsFloat() * 1024 * 1024
	mmapConfig := C.CMmapConfig{
		cache_read_ahead_policy:  cCacheReadAheadPolicy,
		mmap_path:                cMmapChunkManagerDir,
		disk_limit:               C.uint64_t(diskLimit),
		fix_file_size:            C.uint64_t(mmapFileSize),
		growing_enable_mmap:      C.bool(params.QueryNodeCfg.GrowingMmapEnabled.GetAsBool()),
		scalar_index_enable_mmap: C.bool(params.QueryNodeCfg.MmapScalarIndex.GetAsBool()),
		scalar_field_enable_mmap: C.bool(params.QueryNodeCfg.MmapScalarField.GetAsBool()),
		vector_index_enable_mmap: C.bool(params.QueryNodeCfg.MmapVectorIndex.GetAsBool()),
		vector_field_enable_mmap: C.bool(params.QueryNodeCfg.MmapVectorField.GetAsBool()),
	}
	status := C.InitMmapManager(mmapConfig)
	return HandleCStatus(&status, "InitMmapManager failed")
}

func InitInterminIndexConfig(params *paramtable.ComponentParam) error {
	enableInterminIndex := C.bool(params.QueryNodeCfg.EnableInterminSegmentIndex.GetAsBool())
	C.SegcoreSetEnableInterminSegmentIndex(enableInterminIndex)

	nlist := C.int64_t(params.QueryNodeCfg.InterimIndexNlist.GetAsInt64())
	C.SegcoreSetNlist(nlist)

	nprobe := C.int64_t(params.QueryNodeCfg.InterimIndexNProbe.GetAsInt64())
	C.SegcoreSetNprobe(nprobe)

	subDim := C.int64_t(params.QueryNodeCfg.InterimIndexSubDim.GetAsInt64())
	C.SegcoreSetSubDim(subDim)

	refineRatio := C.float(params.QueryNodeCfg.InterimIndexRefineRatio.GetAsFloat())
	C.SegcoreSetRefineRatio(refineRatio)

	denseVecIndexType := C.CString(params.QueryNodeCfg.DenseVectorInterminIndexType.GetValue())
	defer C.free(unsafe.Pointer(denseVecIndexType))
	status := C.SegcoreSetDenseVectorInterminIndexType(denseVecIndexType)
	return HandleCStatus(&status, "InitInterminIndexConfig failed")
}

func CleanRemoteChunkManager() {
	C.CleanRemoteChunkManagerSingleton()
}

func CleanGlogManager() {
	C.SegcoreCloseGlog()
}

// HandleCStatus deals with the error returned from CGO
func HandleCStatus(status *C.CStatus, extraInfo string) error {
	if status.error_code == 0 {
		return nil
	}
	errorCode := status.error_code
	errorName, ok := commonpb.ErrorCode_name[int32(errorCode)]
	if !ok {
		errorName = "UnknownError"
	}
	errorMsg := C.GoString(status.error_msg)
	defer C.free(unsafe.Pointer(status.error_msg))

	finalMsg := fmt.Sprintf("[%s] %s", errorName, errorMsg)
	logMsg := fmt.Sprintf("%s, C Runtime Exception: %s\n", extraInfo, finalMsg)
	log.Warn(logMsg)
	return errors.New(finalMsg)
}
