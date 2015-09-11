/**
 * Functions related to keys
 */
package util

func MetaKey(key string) string {
	return fmt.Sprintf("%s_meta", key)
}

func ChunkKey(key string, chunk int) string {
	return fmt.Sprintf("%s_%d", key, chunk)
}

// TODO: pass chunk size
func ChunkSliceIndices(chunkNum int, totalLength int) (int, int) {
	// Indices for slicing. End is exclusive
	start := CHUNK_SIZE * chunkNum
	end := int(math.Min(float64(start+CHUNK_SIZE), float64(totalLength)))

	return start, end
}
