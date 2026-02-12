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

package inst

import (
	"fmt"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func Test_x26MOVZ(t *testing.T) {
	convey.Convey("Test_x26MOVZ", t, func() {
		inst := fmt.Sprintf("%x", x26MOVZ(0x123456789abcef01))
		convey.So(inst, convey.ShouldEqual, "3ae09dd2")
	})
}

func Test_x26MOVK(t *testing.T) {
	convey.Convey("Test_x26MOVK", t, func() {
		inst := fmt.Sprintf("%x", x26MOVK(0x123456789abcef01, 1))
		convey.So(inst, convey.ShouldEqual, "9a57b3f2")
	})
}
