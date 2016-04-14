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

// +build !amd64

package metrics

// Function copied from Damian Gryski's go-bits package
// Copyright 2015 Damian Gryski, under MIT license
// See the NOTICE file for more details.
// https://github.com/dgryski/go-bits
func lzcnt(x uint64) uint64 {
	var n uint64

	n = 1

	if (x >> 32) == 0 {
		n = n + 32
		x = x << 32
	}
	if (x >> (32 + 16)) == 0 {
		n = n + 16
		x = x << 16
	}

	if (x >> (32 + 16 + 8)) == 0 {
		n = n + 8
		x = x << 8
	}

	if (x >> (32 + 16 + 8 + 4)) == 0 {
		n = n + 4
		x = x << 4
	}

	if (x >> (32 + 16 + 8 + 4 + 2)) == 0 {
		n = n + 2
		x = x << 2
	}

	n = n - (x >> 63)
	return uint64(n)
}
