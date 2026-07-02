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
	"strings"
	"sync"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	AnalyzerConfigPrefix = "function.analyzer."
	LinderaDictURLKey    = "lindera_download_urls"
	DefaultDictPathKey   = "default_dict_path"
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

	if err := UpdateParams(); err != nil {
		return err
	}

	paramtable.Get().WatchKeyPrefix(AnalyzerConfigPrefix, config.NewHandler("analyzer.runtime_options", func(*config.Event) {
		if err := updateParams(); err != nil {
			log.Error("update analyzer option failed", zap.Error(err))
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
	}
}

func buildLinderaDownloadURLs(values map[string]string) map[string][]string {
	urls := make(map[string][]string, len(values))
	for kind, value := range values {
		urls[strings.TrimPrefix(kind, ".")] = paramtable.ParseAsStings(value)
	}
	return urls
}

func UpdateParams() error {
	return updateParams()
}

func updateParams() error {
	bytes, err := json.Marshal(BuildRuntimeOptions())
	if err != nil {
		return err
	}

	paramPtr := C.CString(string(bytes))
	defer C.free(unsafe.Pointer(paramPtr))

	status := C.set_tokenizer_option(paramPtr)
	if err := HandleCStatus(&status, "failed to init segcore analyzer option"); err != nil {
		return err
	}
	return nil
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
