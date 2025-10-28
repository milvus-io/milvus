package canalyzer

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>	// free
#include "segcore/tokenizer_c.h"
#include "segcore/token_stream_c.h"
*/
import "C"

import (
	"encoding/json"
	"sync"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	LinderaDictURLKey = "lindera_download_urls"
	ResourceMapKey    = "resource_map"
	DictPathKey       = "local_dict_path"
	ResourcePathKey   = "resource_path"
)

var initOnce sync.Once

func InitOptions() {
	initOnce.Do(func() {
		UpdateParams()
	})
}

func UpdateParams() {
	cfg := paramtable.Get()
	params := map[string]any{}
	params[LinderaDictURLKey] = cfg.FunctionCfg.LinderaDownloadUrls.GetValue()
	params[DictPathKey] = cfg.FunctionCfg.LocalResourcePath.GetValue()

	bytes, err := json.Marshal(params)
	if err != nil {
		log.Panic("init analyzer option failed", zap.Error(err))
	}

	paramPtr := C.CString(string(bytes))
	defer C.free(unsafe.Pointer(paramPtr))

	status := C.set_tokenizer_option(paramPtr)
	if err := HandleCStatus(&status, "failed to init segcore analyzer option"); err != nil {
		log.Panic("init analyzer option failed", zap.Error(err))
	}
}

func NewAnalyzer(param string) (interfaces.Analyzer, error) {
	paramPtr := C.CString(param)
	defer C.free(unsafe.Pointer(paramPtr))

	var ptr C.CTokenizer
	status := C.create_tokenizer(paramPtr, &ptr)
	if err := HandleCStatus(&status, "failed to create analyzer"); err != nil {
		return nil, err
	}

	return NewCAnalyzer(ptr), nil
}

func ValidateAnalyzer(param string) error {
	paramPtr := C.CString(param)
	defer C.free(unsafe.Pointer(paramPtr))

	status := C.validate_tokenizer(paramPtr)
	if err := HandleCStatus(&status, "failed to create tokenizer"); err != nil {
		return err
	}
	return nil
}
