/**
 * Functions related to keys
 */
package local

import "bytes"
import "encoding/binary"
import "math"

func metaKey(key []byte) []byte {
    return append(key, ([]byte("_meta"))...)
}

func chunkKey(key []byte, chunk int) []byte {
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.BigEndian, chunk)
	return append(key, buf.Bytes()...)
}

func chunkSliceIndices(chunkSize, chunkNum, totalLength int) (int, int) {
	// Indices for slicing. End is exclusive
	start := chunkSize * chunkNum
	end := int(math.Min(float64(start + chunkSize), float64(totalLength)))

	return start, end
}
