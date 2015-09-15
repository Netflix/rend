/**
 * Functions related to keys
 */
package util

import "fmt"
import "math"

func MetaKey(key []byte) []byte {
	return []byte(fmt.Sprintf("%s_meta", key))
}

func ChunkKey(key []byte, chunk int) []byte {
	return []byte(fmt.Sprintf("%s_%d", key, chunk))
}

func ChunkSliceIndices(chunkSize, chunkNum, totalLength int) (int, int) {
	// Indices for slicing. End is exclusive
	start := chunkSize * chunkNum
	end := int(math.Min(float64(start + chunkSize), float64(totalLength)))

	return start, end
}
