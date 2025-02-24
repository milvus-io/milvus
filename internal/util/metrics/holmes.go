/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package metrics

import (
	"sync"
	"time"

	"mosn.io/holmes"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	once      sync.Once
	closeOnce sync.Once
	h         *holmes.Holmes
)

func InitHolmes() {
	once.Do(func() {
		pt := paramtable.Get()
		if !pt.HolmesCfg.Enable.GetAsBool() {
			return
		}

		options := []holmes.Option{
			holmes.WithDumpPath(pt.HolmesCfg.DumpPath.GetValue()),
			holmes.WithBinaryDump(),
			holmes.WithCGroup(true),
			holmes.WithCollectInterval(pt.HolmesCfg.CollectInterval.GetAsDuration(time.Second).String()),
		}

		if pt.HolmesCfg.EnableDumpProfile.GetAsBool() {
			options = append(options, holmes.WithCPUDump(
				pt.HolmesCfg.ProfileMinCPU.GetAsInt(),
				pt.HolmesCfg.ProfileDiffCPU.GetAsInt(),
				pt.HolmesCfg.ProfileAbsCPU.GetAsInt(),
				pt.HolmesCfg.ProfileCooldown.GetAsDuration(time.Second),
			))
		}

		h, _ = holmes.New(options...)

		if pt.HolmesCfg.EnableDumpProfile.GetAsBool() {
			h.EnableCPUDump()
		}

		h.Start()
	})
}

func CloseHolmes() {
	// make sure init done
	InitHolmes()
	closeOnce.Do(func() {
		if h != nil {
			h.Stop()
			h = nil
		}
	})
}
