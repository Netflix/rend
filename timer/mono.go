// Original license header:
//
// Copyright (C) 2014 Space Monkey, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package monotime provides a monotonic timer for Go 1.2
// Go 1.3 will support monotonic time on its own.

// This file has been altered to support a more start -> end timing cycle instead
// of a monotonically increasing time. Rend does not need exact time from any time
// in the past other than the one found by start. In other words, the usefulness
// of the original package has been limited.

// License header for Rend:
//
// Copyright 2016 Netflix, Inc.
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

package timer

// Now returns the current monotonic time, with an arbitrary starting time.
// It is not useful for telling the current wall clock time
func Now() uint64 {
	sec, nsec := monotime()
	return uint64(sec)*1000000000 + uint64(nsec)
}

// Since returns the duration in nanoseconds since the given starting time.
func Since(start uint64) uint64 {
	return Now() - start
}
