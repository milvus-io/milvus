package canalyzer

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>	// free
#include "segcore/tokenizer_c.h"
#include "segcore/token_stream_c.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"unsafe"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	AnalyzerConfigPrefix = "function.analyzer."
	LinderaDictURLKey    = "lindera_download_urls"
	DefaultDictPathKey   = "default_dict_path"
	ResourceMapKey       = "resource_map"
	ResourcePathKey      = "resource_path"
	StorageNameKey       = "storage_name"
)

var (
	initMu             sync.Mutex
	optionsInitialized bool
)

func InitOptions() error {
	initMu.Lock()
	defer initMu.Unlock()

	if optionsInitialized {
		return nil
	}

	if err := updateParams(); err != nil {
		return err
	}

	paramtable.Get().WatchKeyPrefix(AnalyzerConfigPrefix, config.NewHandler("analyzer.runtime_options", func(*config.Event) {
		if err := updateParams(); err != nil {
			mlog.Error(context.TODO(), "update analyzer option failed", mlog.Err(err))
		}
	}))
	optionsInitialized = true
	return nil
}

func BuildRuntimeOptions() map[string]any {
	cfg := paramtable.Get()
	return map[string]any{
		LinderaDictURLKey:  buildLinderaDownloadURLs(cfg.FunctionCfg.LinderaDownloadUrls.GetValue()),
		DefaultDictPathKey: cfg.FunctionCfg.LocalResourcePath.GetValue(),
		ResourcePathKey:    pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID()),
	}
}

func buildLinderaDownloadURLs(values map[string]string) map[string][]string {
	urls := make(map[string][]string, len(values))
	for kind, value := range values {
		urls[strings.TrimPrefix(kind, ".")] = paramtable.ParseAsStings(value)
	}
	return urls
}

func updateParams() error {
	bytes, err := json.Marshal(BuildRuntimeOptions())
	if err != nil {
		return err
	}

	paramPtr := C.CString(string(bytes))
	defer C.free(unsafe.Pointer(paramPtr))

	status := C.set_tokenizer_option(paramPtr)
	return HandleCStatus(&status, "failed to init segcore analyzer option")
}

func BuildExtraResourceInfo(storage string, resources []*internalpb.FileResourceInfo) (string, error) {
	result := map[string]any{}
	result[StorageNameKey] = storage

	resultMap := map[string]int64{}
	for _, resource := range resources {
		resultMap[resource.GetName()] = resource.GetId()
	}
	result[ResourceMapKey] = resultMap
	bytes, err := json.Marshal(result)
	if err != nil {
		return "", errors.Wrap(err, "marshal extra resource info failed")
	}
	return string(bytes), nil
}

func UpdateGlobalResourceInfo(resourceMap map[string]int64) error {
	bytes, err := json.Marshal(map[string]any{"resource_map": resourceMap})
	if err != nil {
		return errors.Wrap(err, "marshal global resource info failed")
	}

	paramPtr := C.CString(string(bytes))
	defer C.free(unsafe.Pointer(paramPtr))

	status := C.set_tokenizer_option(paramPtr)
	if err := HandleCStatus(&status, "failed to update global resource info"); err != nil {
		return errors.Wrap(err, "update global resource info failed")
	}
	return nil
}

func NewAnalyzer(param string, extraInfo string) (interfaces.Analyzer, error) {
	paramPtr := C.CString(param)
	defer C.free(unsafe.Pointer(paramPtr))

	extraInfoPtr := C.CString(extraInfo)
	defer C.free(unsafe.Pointer(extraInfoPtr))

	var ptr C.CTokenizer
	status := C.create_tokenizer(paramPtr, extraInfoPtr, &ptr)
	if err := HandleCStatus(&status, "failed to create analyzer"); err != nil {
		return nil, err
	}

	return NewCAnalyzer(ptr), nil
}

func ValidateAnalyzer(param string, extraInfo string) ([]int64, error) {
	paramPtr := C.CString(param)
	defer C.free(unsafe.Pointer(paramPtr))

	extraInfoPtr := C.CString(extraInfo)
	defer C.free(unsafe.Pointer(extraInfoPtr))

	result := C.validate_tokenizer(paramPtr, extraInfoPtr)
	if err := HandleCStatus(&result.status, "failed to validate tokenizer"); err != nil {
		return nil, err
	}

	cIds := unsafe.Slice((*int64)(unsafe.Pointer(result.resource_ids)), result.resource_ids_count)
	goIds := make([]int64, len(cIds))
	copy(goIds, cIds)
	C.free(unsafe.Pointer(result.resource_ids))
	return goIds, nil
}
