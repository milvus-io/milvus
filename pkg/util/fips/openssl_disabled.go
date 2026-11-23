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

//go:build !milvus_fips

package fips

// MaybeEnableOpenSSLFIPS reports false: this is not a MILVUS_FIPS_ENABLED=ON build.
func MaybeEnableOpenSSLFIPS() bool {
	return false
}

// Require has nothing to check outside a MILVUS_FIPS_ENABLED=ON build.
func Require() error {
	return nil
}

// Status describes the FIPS state of the process for the startup banner.
func Status() string {
	return "off"
}
