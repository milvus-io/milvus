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

package milvus

import (
	"reflect"
	"testing"
)

func TestNativeProfileDefaultAndHelp(t *testing.T) {
	option := "--native-profile"
	environ := []string{"MALLOC_CONF=prof:true,lg_prof_sample:17", "KEEP=value"}
	for _, tc := range []struct {
		args, want []string
	}{
		{args: []string{"milvus"}},
		{args: []string{"milvus", "run", "querynode", "--run-with-subprocess"}},
		{args: []string{"milvus", "run", "querynode", "--alias", option}},
		{args: []string{"milvus", "run", "querynode", "-alias", "-native-profile"}},
		{args: []string{"milvus", "run", "querynode", "--", option}},
		{args: []string{"milvus", "run", "querynode", "positional", option}},
		{args: []string{"milvus", "run", "querynode", "--help", option}},
		{args: []string{"milvus", "run", "querynode", "--unknown", option}},
		{args: []string{"milvus", "stop", "querynode", option}},
		{args: []string{"milvus", "run", "querynode", option, "--help", option}, want: []string{"milvus", "run", "querynode", "--help", option}},
		{args: []string{"milvus", "run", "querynode", "--native-profile=false"}, want: []string{"milvus", "run", "querynode"}},
		{args: []string{"milvus", "run", "querynode", option, "--native-profile=false"}, want: []string{"milvus", "run", "querynode"}},
	} {
		before := append([]string(nil), tc.args...)
		args, env, err := prepareNativeProfile(tc.args, environ)
		want := tc.want
		if want == nil {
			want = before
		}
		if err != nil || env != nil || !reflect.DeepEqual(args, want) || !reflect.DeepEqual(tc.args, before) {
			t.Fatalf("default/help %v = %v, %v, %v", tc.args, args, env, err)
		}
	}
}

func TestNativeProfilePreparesSubprocess(t *testing.T) {
	environ := []string{"KEEP=value", "MALLOC_CONF=background_thread:true,prof:false,prof_active:false,prof_thread_active_init:false,lg_prof_sample:17,prof_final:true"}
	beforeEnv := append([]string(nil), environ...)
	wantEnv := append(append([]string(nil), environ...), environ[1]+",prof:true,prof_active:true,prof_thread_active_init:true")
	for _, tc := range []struct{ args, want []string }{
		{[]string{"milvus", "run", "querynode", "--native-profile", "--alias", "node"}, []string{"milvus", "run", "querynode", "--alias", "node", "--run-with-subprocess"}},
		{[]string{"milvus", "run", "querynode", "--run-with-subprocess", "--native-profile=true"}, []string{"milvus", "run", "querynode", "--run-with-subprocess"}},
		{[]string{"milvus", "--run-with-subprocess", "run", "querynode", "-native-profile"}, []string{"milvus", "--run-with-subprocess", "run", "querynode"}},
		{[]string{"milvus", "run", "standalone", "--native-profile"}, []string{"milvus", "run", "standalone", "--run-with-subprocess"}},
	} {
		before := append([]string(nil), tc.args...)
		clean, env, err := prepareNativeProfile(tc.args, environ)
		if err != nil || !reflect.DeepEqual(clean, tc.want) || !reflect.DeepEqual(env, wantEnv) {
			t.Fatalf("subprocess %v = %v, %v, %v", tc.args, clean, env, err)
		}
		if !reflect.DeepEqual(tc.args, before) || !reflect.DeepEqual(environ, beforeEnv) {
			t.Fatal("profiling modified caller args/environment")
		}
	}
}

func TestNativeProfileInvalidValue(t *testing.T) {
	for _, option := range []string{"--native-profile=", "--native-profile=garbage", "--native-profile=/profiles"} {
		_, env, err := prepareNativeProfile([]string{"milvus", "run", "querynode", option}, nil)
		if err == nil || env != nil {
			t.Fatalf("invalid value %q accepted: %v, %v", option, env, err)
		}
	}
	_, _, err := prepareNativeProfile([]string{"milvus", "run", "querynode", "--native-profile", "--native-profile=bad"}, nil)
	if err == nil {
		t.Fatal("invalid value after an earlier profiling option must fail")
	}
}
