/**
 * Functions related to keys
 */
package local

import "fmt"
import "math"

func metaKey(key []byte) []byte {
    return append(key, ([]byte("_meta"))...)
}

func chunkKey(key []byte, chunk int) []byte {
    chunkStr := fmt.Sprintf("_%v", chunk)
    return append(key, []byte(chunkStr)...)
}

func chunkSliceIndices(chunkSize, chunkNum, totalLength int) (int, int) {
    // Indices for slicing. End is exclusive
    start := chunkSize * chunkNum
    end := int(math.Min(float64(start + chunkSize), float64(totalLength)))

    return start, end
}
