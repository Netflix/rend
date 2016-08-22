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

var tags = map[string]string{
	"foo1": "bar1",
	"foo2": "bar2",
	"foo3": "bar3",
	"foo4": "bar4",
	"foo5": "bar5",
	"foo6": "bar6",
	"foo7": "bar7",
	"foo8": "bar8",
}

func TagsBuildStringAppend(tags map[string]string) string {
	var ret string
	for k, v := range tags {
		ret += "|"
		ret += k
		ret += "*"
		ret += v
	}
	return ret
}

func BenchmarkTagsBuildStringAppend(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = TagsBuildStringAppend(tags)
	}
}

func TagsBuildStringByteSlice(tags map[string]string) string {
	var size int

	for k, v := range tags {
		size += len(k) + len(v) + 2
	}

	ret := make([]byte, size)

	for k, v := range tags {
		ret = append(ret, byte('|'))
		ret = append(ret, k...)
		ret = append(ret, byte('*'))
		ret = append(ret, v...)
	}

	return string(ret)
}

func BenchmarkTagsBuildStringByteSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = TagsBuildStringByteSlice(tags)
	}
}
