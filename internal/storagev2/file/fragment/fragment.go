// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fragment

import "github.com/milvus-io/milvus/pkg/proto/storagev2pb"

type FragmentType int32

const (
	kUnknown FragmentType = 0
	kData    FragmentType = 1
	kDelete  FragmentType = 2
)

type Fragment struct {
	fragmentId int64
	files      []string
}

type FragmentVector []Fragment

func ToFilesVector(fragments []Fragment) []string {
	files := make([]string, 0)
	for _, fragment := range fragments {
		files = append(files, fragment.files...)
	}
	return files
}

func NewFragment() Fragment {
	return Fragment{
		files: make([]string, 0),
	}
}

func (f *Fragment) AddFile(file string) {
	f.files = append(f.files, file)
}

func (f *Fragment) Files() []string {
	return f.files
}

func (f *Fragment) FragmentId() int64 {
	return f.fragmentId
}

func (f *Fragment) SetFragmentId(fragmentId int64) {
	f.fragmentId = fragmentId
}

func (f *Fragment) ToProtobuf() *storagev2pb.Fragment {
	fragment := &storagev2pb.Fragment{}
	fragment.Id = f.fragmentId
	fragment.Files = append(fragment.Files, f.files...)
	return fragment
}

func FromProtobuf(fragment *storagev2pb.Fragment) Fragment {
	newFragment := NewFragment()
	newFragment.SetFragmentId(fragment.GetId())
	newFragment.files = append(newFragment.files, fragment.Files...)
	return newFragment
}
