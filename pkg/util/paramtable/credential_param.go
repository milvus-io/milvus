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

package paramtable

type credentialConfig struct {
	Credential ParamGroup `refreshable:"true"`
}

func (p *credentialConfig) init(base *BaseTable) {
	p.Credential = ParamGroup{
		KeyPrefix: "credential.",
		Version:   "2.6.0",
		Export:    true,
		DocFunc: func(key string) string {
			switch key {
			case "apikey1.apikey":
				return "Your apikey credential"
			case "aksk1.access_key_id":
				return "Your access_key_id"
			case "aksk1.secret_access_key":
				return "Your secret_access_key"
			case "gcp1.credential_json":
				return "base64 based gcp credential data"
			default:
				return ""
			}
		},
	}
	p.Credential.Init(base.mgr)
}

func (p *credentialConfig) GetCredentials() map[string]string {
	return p.Credential.GetValue()
}
