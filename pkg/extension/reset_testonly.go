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

//go:build test

package extension

// ResetForTest clears the installed provider so a test can install another.
//
// It is built only under the "test" tag and therefore does not exist in a
// production binary. That matters: clearing the provider at runtime would drop
// every consumer back to the native path, which for a form that declared a
// capability as required means running without it - the exact failure
// SetProvider's requirement check exists to prevent.
func ResetForTest() { installed.Store(nil) }
