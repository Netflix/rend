/**
 * Copyright 2015 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Functions related to keys
 */
package local

import "fmt"
import "math"

func metaKey(key []byte) []byte {
    keyCopy := make([]byte, len(key))
    copy(keyCopy, key)
    return append(keyCopy, ([]byte("_meta"))...)
}

func chunkKey(key []byte, chunk int) []byte {
    keyCopy := make([]byte, len(key))
    copy(keyCopy, key)
    chunkStr := fmt.Sprintf("_%v", chunk)
    return append(keyCopy, []byte(chunkStr)...)
}

func chunkSliceIndices(chunkSize, chunkNum, totalLength int) (int, int) {
    // Indices for slicing. End is exclusive
    start := chunkSize * chunkNum
    end := int(math.Min(float64(start + chunkSize), float64(totalLength)))

    return start, end
}
