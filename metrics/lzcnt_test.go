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
package metrics

import (
	"testing"
	"testing/quick"
)

// Derived from Damian Gryski's go-bits package
// Copyright 2015 Damian Gryski, under MIT license
// See the NOTICE file for more details.
// https://github.com/dgryski/go-bits
func TestQuickLzcnt(t *testing.T) {
	f := func(x uint64) bool {
		return lzcnt(x) == lzcntSlowForTestingOnly(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("lzcnt != lzcntSlowForTestingOnly: %v: ", err)
	}
}

func lzcntSlowForTestingOnly(x uint64) uint64 {
	var n uint64
	for x&0x8000000000000000 == 0 {
		n++
		x <<= 1
	}
	return n
}
