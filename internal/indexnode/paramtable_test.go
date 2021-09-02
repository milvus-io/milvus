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

package indexnode

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_Init(t *testing.T) {
	Params.Init()
}

func TestParamTable_Address(t *testing.T) {
	address := Params.Address
	fmt.Println(address)
}

func TestParamTable_MinIOAddress(t *testing.T) {
	address := Params.MinIOAddress
	fmt.Println(address)
}

func TestParamTable_MinIOAccessKeyID(t *testing.T) {
	accessKeyID := Params.MinIOAccessKeyID
	assert.Equal(t, accessKeyID, "minioadmin")
}

func TestParamTable_MinIOSecretAccessKey(t *testing.T) {
	secretAccessKey := Params.MinIOSecretAccessKey
	assert.Equal(t, secretAccessKey, "minioadmin")
}

func TestParamTable_MinIOUseSSL(t *testing.T) {
	useSSL := Params.MinIOUseSSL
	assert.Equal(t, useSSL, false)
}

func TestParamTable_MinIORegion(t *testing.T) {
	region := Params.MinIORegion
	assert.Equal(t, region, "")
}
