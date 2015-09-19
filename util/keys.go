/**
 * Functions related to keys
 */
package util

import "bytes"
import "encoding/binary"
import "math"

func MetaKey(key []byte) []byte {
    return append(key, ([]byte("_meta"))...)
}

func ChunkKey(key []byte, chunk int) []byte {
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.BigEndian, chunk)
	return append(key, buf.Bytes()...)
}

func ChunkSliceIndices(chunkSize, chunkNum, totalLength int) (int, int) {
	// Indices for slicing. End is exclusive
	start := chunkSize * chunkNum
	end := int(math.Min(float64(start + chunkSize), float64(totalLength)))

	return start, end
}
