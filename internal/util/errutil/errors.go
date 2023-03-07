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

// errutil package provides utility for errors handling.
package errutil

import (
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
)

type multiErrors struct {
	errs []error
}

func (e multiErrors) Unwrap() error {
	if len(e.errs) <= 1 {
		return nil
	}
	return multiErrors{
		errs: e.errs[1:],
	}
}

func (e multiErrors) Error() string {
	final := e.errs[0]
	for i := 1; i < len(e.errs); i++ {
		final = errors.Wrap(e.errs[i], final.Error())
	}
	return final.Error()
}

func (e multiErrors) Is(err error) bool {
	for _, item := range e.errs {
		if errors.Is(item, err) {
			return true
		}
	}
	return false
}

func Combine(errs ...error) error {
	errs = lo.Filter(errs, func(err error, _ int) bool { return err != nil })
	if len(errs) == 0 {
		return nil
	}
	return multiErrors{
		errs,
	}
}
