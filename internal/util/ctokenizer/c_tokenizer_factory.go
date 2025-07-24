package ctokenizer

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>	// free
#include "segcore/tokenizer_c.h"
#include "segcore/token_stream_c.h"
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/tokenizerapi"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func NewTokenizer(param string) (tokenizerapi.Tokenizer, error) {
	param, err := CheckAndFillParams(param)
	if err != nil {
		return nil, err
	}

	paramPtr := C.CString(param)
	defer C.free(unsafe.Pointer(paramPtr))

	var ptr C.CTokenizer
	status := C.create_tokenizer(paramPtr, &ptr)
	if err := HandleCStatus(&status, "failed to create tokenizer"); err != nil {
		return nil, err
	}

	return NewCTokenizer(ptr), nil
}

func CheckAndFillParams(params string) (string, error) {
	var paramMaps map[string]any
	flag := false
	err := json.Unmarshal([]byte(params), &paramMaps)
	if err != nil {
		return "", merr.WrapErrAsInputError(fmt.Errorf("unmarshal analyzer params failed with json error: %s", err.Error()))
	}

	tokenizer, ok := paramMaps["tokenizer"]
	if !ok {
		// skip check if no tokenizer params
		return params, nil
	}

	switch value := tokenizer.(type) {
	case string:
		return params, nil
	case map[string]any:
		flag, err = CheckAndFillTokenizerParams(value)
		if err != nil {
			return "", err
		}
	default:
		return "", merr.WrapErrAsInputError(fmt.Errorf("analyzer params set tokenizer with unkonwn type"))
	}

	// remarshal json params if params map was changed.
	if flag {
		bytes, err := json.Marshal(paramMaps)
		if err != nil {
			return "", merr.WrapErrAsInputError(fmt.Errorf("marshal analyzer params failed with json error: %s", err.Error()))
		}
		return string(bytes), nil
	}
	return params, nil
}

func CheckAndFillTokenizerParams(params map[string]any) (bool, error) {
	v, ok := params["type"]
	if !ok {
		return false, merr.WrapErrAsInputError(fmt.Errorf("costom tokenizer must set type"))
	}

	tokenizerType, ok := v.(string)
	if !ok {
		return false, merr.WrapErrAsInputError(fmt.Errorf("costom tokenizer type must be string"))
	}

	switch tokenizerType {
	case "lindera":
		cfg := paramtable.Get()

		if _, ok := params["dict_build_dir"]; ok {
			return false, merr.WrapErrAsInputError(fmt.Errorf("costom tokenizer dict_build_dir was system params, should not be set"))
		}
		params["dict_build_dir"] = cfg.FunctionCfg.LocalDictionaryPath.GetValue()

		v, ok := params["dict_kind"]
		if !ok {
			return false, merr.WrapErrAsInputError(fmt.Errorf("lindera tokenizer must set dict_kind"))
		}
		dictKind, ok := v.(string)
		if !ok {
			return false, merr.WrapErrAsInputError(fmt.Errorf("lindera tokenizer dict kind must be string"))
		}
		dictUrlsMap := cfg.FunctionCfg.LinderaDownloadUrls.GetValue()

		if _, ok := params["download_urls"]; ok {
			return false, merr.WrapErrAsInputError(fmt.Errorf("costom tokenizer download_urls was system params, should not be set"))
		}

		if value, ok := dictUrlsMap["."+dictKind]; ok {
			params["download_urls"] = paramtable.ParseAsStings(value)
		}
		return true, nil
	default:
		return false, nil
	}
}

func ValidateTokenizer(param string) error {
	param, err := CheckAndFillParams(param)
	if err != nil {
		return err
	}

	paramPtr := C.CString(param)
	defer C.free(unsafe.Pointer(paramPtr))

	status := C.validate_tokenizer(paramPtr)
	if err := HandleCStatus(&status, "failed to create tokenizer"); err != nil {
		return err
	}
	return nil
}
