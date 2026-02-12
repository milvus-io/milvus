/*
 * Copyright 2022 ByteDance Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tool

import (
	"os"
)

func init() {
	// set MOCKEY_CHECK_GCFLAGS=false to disable this check
	checkGCflags()
}

func checkGCflags() int {
	if flag := os.Getenv("MOCKEY_CHECK_GCFLAGS"); flag != "false" && !IsGCFlagsSet() {
		println(`
Mockey check failed, please add -gcflags="all=-N -l".
(Set env MOCKEY_CHECK_GCFLAGS=false to disable gcflags check) 
		`)
	}
	return 0
}
