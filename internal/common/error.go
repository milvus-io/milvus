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

package common

import "fmt"

type IgnorableError struct {
	msg string
}

func (i *IgnorableError) Error() string {
	return i.msg
}

func NewIgnorableError(err error) error {
	return &IgnorableError{
		msg: err.Error(),
	}
}

func IsIgnorableError(err error) bool {
	_, ok := err.(*IgnorableError)
	return ok
}

var _ error = &KeyNotExistError{}

func NewKeyNotExistError(key string) error {
	return &KeyNotExistError{key: key}
}

func IsKeyNotExistError(err error) bool {
	_, ok := err.(*KeyNotExistError)
	return ok
}

type KeyNotExistError struct {
	key string
}

func (k *KeyNotExistError) Error() string {
	return fmt.Sprintf("there is no value on key = %s", k.key)
}
