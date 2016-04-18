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

package chunked

import (
	"math"
	"strconv"
)

func metaKey(key []byte) []byte {
	// no need to copy, the header returned will point to the same array
	// just with a longer len. It might get copied if the runtime decides
	// to grow the slice.
	return append(key, ([]byte("-meta"))...)
}

func chunkKey(key []byte, chunk int) []byte {
	// TODO: POOL ME PLEASE
	// or maybe not since pooling adds interface{} conversion overhead anyway
	//
	// no need to copy, the header returned will point to the same array
	// just with a longer len. It might get copied if the runtime decides
	// to grow the slice.
	return strconv.AppendInt(key, int64(-chunk), 10)
}

func chunkSliceIndices(chunkSize, chunkNum, totalLength int) (int, int) {
	// Indices for slicing. End is exclusive
	start := chunkSize * chunkNum
	end := int(math.Min(float64(start+chunkSize), float64(totalLength)))

	return start, end
}
