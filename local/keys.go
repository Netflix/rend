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
