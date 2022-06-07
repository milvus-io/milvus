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

package grpcclient

import (
	"errors"
	"fmt"
)

// ErrConnect is the instance for errors.Is target usage.
var ErrConnect errConnect

// make sure ErrConnect implements error.
var _ error = errConnect{}

// errConnect error instance returned when dial error returned.
type errConnect struct {
	addr string
	err  error
}

// Error implements error interface.
func (e errConnect) Error() string {
	return fmt.Sprintf("failed to connect %s, reason: %s", e.addr, e.err.Error())
}

// Is checks err is ErrConnect to make errors.Is work.
func (e errConnect) Is(err error) bool {
	var ce errConnect
	return errors.As(err, &ce)
}

// wrapErrConnect wrap connection error and related address to ErrConnect.
func wrapErrConnect(addr string, err error) error {
	return errConnect{addr: addr, err: err}
}
