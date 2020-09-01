// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package typeutil

import (
	"strconv"
	"strings"

	"github.com/czs007/suvlim/errors"
)

// StringSlice is more friendly to json encode/decode
type StringSlice []string

// MarshalJSON returns the size as a JSON string.
func (s StringSlice) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(strings.Join(s, ","))), nil
}

// UnmarshalJSON parses a JSON string into the bytesize.
func (s *StringSlice) UnmarshalJSON(text []byte) error {
	data, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	if len(data) == 0 {
		*s = []string{}
		return nil
	}
	*s = strings.Split(data, ",")
	return nil
}
