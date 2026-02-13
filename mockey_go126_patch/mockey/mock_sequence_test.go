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

package mockey

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestSequenceRace(t *testing.T) {
	PatchConvey("test sequence race", t, func(c convey.C) {
		fn := func() int { return -1 * int(math.Abs(1)) }
		mocker := Mock(fn).Return(Sequence(0).Then(1)).Build()

		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			v := fn()
			c.So(v, convey.ShouldNotEqual, -1)
		}()
		go func() {
			defer wg.Done()
			v := fn()
			c.So(v, convey.ShouldNotEqual, -1)
		}()
		wg.Wait()
		c.So(mocker.MockTimes(), convey.ShouldEqual, 2)
	})
}

func TestSequence(t *testing.T) {
	PatchConvey("test sequence", t, func() {
		fn := func() (string, int) {
			fmt.Println("original fn")
			return "fn: not here", -1
		}

		PatchConvey("normal", func() {
			tests := []struct {
				Value1 string
				Value2 int
				Times  int
			}{
				{"Alice", 2, 3},
				{"Bob", 3, 1},
				{"Tom", 4, 1},
				{"Jerry", 5, 2},
			}

			seq := Sequence("Admin", 1)
			for _, r := range tests {
				seq.Then(r.Value1, r.Value2).Times(r.Times)
			}
			Mock(fn).Return(seq).Build()

			for repeat := 3; repeat != 0; repeat-- {
				v1, v2 := fn()
				convey.So(v1, convey.ShouldEqual, "Admin")
				convey.So(v2, convey.ShouldEqual, 1)
				for _, r := range tests {
					for rIndex := 0; rIndex < r.Times; rIndex++ {
						v1, v2 := fn()
						convey.So(v1, convey.ShouldEqual, r.Value1)
						convey.So(v2, convey.ShouldEqual, r.Value2)
					}
				}
			}
		})

		PatchConvey("first empty", func() {
			tests := []struct {
				Value1 string
				Value2 int
				Times  int
			}{
				{"Admin", 1, 1},
				{"Alice", 2, 3},
				{"Bob", 3, 1},
				{"Tom", 4, 1},
				{"Jerry", 5, 2},
			}

			seq := Sequence()
			for _, r := range tests {
				seq.Then(r.Value1, r.Value2).Times(r.Times)
			}
			Mock(fn).Return(seq).Build()

			for repeat := 3; repeat != 0; repeat-- {
				for _, r := range tests {
					for rIndex := 0; rIndex < r.Times; rIndex++ {
						v1, v2 := fn()
						convey.So(v1, convey.ShouldEqual, r.Value1)
						convey.So(v2, convey.ShouldEqual, r.Value2)
					}
				}
			}
		})

		PatchConvey("all empty", func() {
			Mock(fn).Return(Sequence()).Build()
			convey.So(func() { fn() }, convey.ShouldPanicWith, "sequence is empty")
		})
	})
}
