// Copyright 2015 Netflix, Inc.
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

package metrics_test

import "testing"

type tags map[string]string

var (
	tgs = tags{
		"fooo1": "baar1",
		"fooo2": "baar2",
		"fooo3": "baar3",
		"fooo4": "baar4",
		"fooo5": "baar5",
		"fooo6": "baar6",
		"fooo7": "baar7",
		"fooo8": "baar8",
	}
)

func TagsBuildStringAppend(tgs tags) string {
	var ret string
	for k, v := range tgs {
		ret += "|"
		ret += k
		ret += "*"
		ret += v
	}
	return ret
}

func BenchmarkTagsBuildStringAppend(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = TagsBuildStringAppend(tgs)
	}
}

func TagsBuildStringByteSlicePrealloc(tgs tags) string {
	var size int

	for k, v := range tgs {
		size += len(k) + len(v) + 2
	}

	ret := make([]byte, 0, size)

	for k, v := range tgs {
		ret = append(ret, byte('|'))
		ret = append(ret, k...)
		ret = append(ret, byte('*'))
		ret = append(ret, v...)
	}

	return string(ret)
}

func BenchmarkTagsBuildStringByteSlicePrealloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = TagsBuildStringByteSlice(tgs)
	}
}

func TagsBuildStringByteSlice(tgs tags) string {
	var ret []byte

	for k, v := range tgs {
		ret = append(ret, byte('|'))
		ret = append(ret, k...)
		ret = append(ret, byte('*'))
		ret = append(ret, v...)
	}

	return string(ret)
}

func BenchmarkTagsBuildStringByteSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = TagsBuildStringByteSlice(tgs)
	}
}
