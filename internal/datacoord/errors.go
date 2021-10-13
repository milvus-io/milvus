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

package datacoord

import (
	"errors"
	"fmt"
)

// errNilKvClient stands for a nil kv client is detected when initialized
var errNilKvClient = errors.New("kv client not initialized")

// serverNotServingErrMsg used for Status Reason when datacoord is not healthy
const serverNotServingErrMsg = "DataCoord is not serving"

// errors for VerifyResponse
var errNilResponse = errors.New("response is nil")
var errNilStatusResponse = errors.New("response has nil status")
var errUnknownResponseType = errors.New("unknown response type")

func msgDataCoordIsUnhealthy(coordID UniqueID) string {
	return fmt.Sprintf("data coord %d is not ready", coordID)
}

func errDataCoordIsUnhealthy(coordID UniqueID) error {
	return errors.New(msgDataCoordIsUnhealthy(coordID))
}
