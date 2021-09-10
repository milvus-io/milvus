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

package proxy

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

func Test_errInvalidNumRows(t *testing.T) {
	invalidNumRowsList := []uint32{
		0,
		16384,
	}

	for _, invalidNumRows := range invalidNumRowsList {
		log.Info("Test_errInvalidNumRows",
			zap.Error(errInvalidNumRows(invalidNumRows)))
	}
}

func Test_errNumRowsLessThanOrEqualToZero(t *testing.T) {
	invalidNumRowsList := []uint32{
		0,
		16384,
	}

	for _, invalidNumRows := range invalidNumRowsList {
		log.Info("Test_errNumRowsLessThanOrEqualToZero",
			zap.Error(errNumRowsLessThanOrEqualToZero(invalidNumRows)))
	}
}

func Test_errEmptyFieldData(t *testing.T) {
	log.Info("Test_errEmptyFieldData",
		zap.Error(errEmptyFieldData))
}

func Test_errFieldsLessThanNeeded(t *testing.T) {
	cases := []struct {
		fieldsNum int
		neededNum int
	}{
		{0, 1},
		{1, 2},
	}

	for _, test := range cases {
		log.Info("Test_errFieldsLessThanNeeded",
			zap.Error(errFieldsLessThanNeeded(test.fieldsNum, test.neededNum)))
	}
}

func Test_errUnsupportedDataType(t *testing.T) {
	unsupportedDTypes := []schemapb.DataType{
		schemapb.DataType_None,
	}

	for _, dType := range unsupportedDTypes {
		log.Info("Test_errUnsupportedDataType",
			zap.Error(errUnsupportedDataType(dType)))
	}
}

func Test_errUnsupportedDType(t *testing.T) {
	unsupportedDTypes := []string{
		"bytes",
		"None",
	}

	for _, dType := range unsupportedDTypes {
		log.Info("Test_errUnsupportedDType",
			zap.Error(errUnsupportedDType(dType)))
	}
}

func Test_errInvalidDim(t *testing.T) {
	invalidDimList := []int{
		0,
		-1,
	}

	for _, invalidDim := range invalidDimList {
		log.Info("Test_errInvalidDim",
			zap.Error(errInvalidDim(invalidDim)))
	}
}

func Test_errDimLessThanOrEqualToZero(t *testing.T) {
	invalidDimList := []int{
		0,
		-1,
	}

	for _, invalidDim := range invalidDimList {
		log.Info("Test_errDimLessThanOrEqualToZero",
			zap.Error(errDimLessThanOrEqualToZero(invalidDim)))
	}
}

func Test_errDimShouldDivide8(t *testing.T) {
	invalidDimList := []int{
		0,
		1,
		7,
	}

	for _, invalidDim := range invalidDimList {
		log.Info("Test_errDimShouldDivide8",
			zap.Error(errDimShouldDivide8(invalidDim)))
	}
}

func Test_msgProxyIsUnhealthy(t *testing.T) {
	ids := []UniqueID{
		1,
	}

	for _, id := range ids {
		log.Info("Test_msgProxyIsUnhealthy",
			zap.String("msg", msgProxyIsUnhealthy(id)))
	}
}

func Test_errProxyIsUnhealthy(t *testing.T) {
	ids := []UniqueID{
		1,
	}

	for _, id := range ids {
		log.Info("Test_errProxyIsUnhealthy",
			zap.Error(errProxyIsUnhealthy(id)))
	}
}
