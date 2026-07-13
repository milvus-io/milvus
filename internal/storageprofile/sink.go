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

package storageprofile

import "context"

type SummarySink interface {
	Publish(ctx context.Context, profile *StorageProfile) error
	Close(ctx context.Context) error
}

type NoopSummarySink struct{}

func (NoopSummarySink) Publish(context.Context, *StorageProfile) error { return nil }
func (NoopSummarySink) Close(context.Context) error                    { return nil }

type ProfilePresenter interface {
	Present(ctx context.Context, profile *StorageProfile) error
}

type AccessLogField struct {
	Key   string
	Value string
}

type AccessLogProfileAdapter interface {
	Fields(profile *StorageProfile) []AccessLogField
}
