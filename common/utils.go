/**
 * Utility functions that produce repeatable data
 */
package common

import "crypto/rand"
import "fmt"
import "math"

// Tokens are used during set handling to uniquely identify
// a specific set
var tokens chan *[16]byte
func genTokens() [16]byte {
    for {
        retval := new([16]byte)
        rand.Read(retval[:])
        tokens <- retval
    }
}

func init() {
    // keep 1000 unique tokens around for write-heavy loads
    // otherwise we have to wait on a read from /dev/urandom
    tokens = make(chan *[16]byte, 1000)
    go genTokens()
}

func makeMetaKey(key string) string {
	return fmt.Sprintf("%s_meta", key)
}

func makeChunkKey(key string, chunk int) string {
	return fmt.Sprintf("%s_%d", key, chunk)
}

// set <key> <flags> <exptime> <bytes>\r\n
func makeSetCommand(key string, exptime string, size int) string {
	return fmt.Sprintf("set %s 0 %s %d\r\n", key, exptime, size)
}

// get <key>*\r\n
// TODO: batch get
func makeGetCommand(key string) string {
	return fmt.Sprintf("get %s\r\n", key)
}

// delete <key>\r\n
func makeDeleteCommand(key string) string {
	return fmt.Sprintf("delete %s\r\n", key)
}

// touch <key> <exptime>\r\n
func makeTouchCommand(key string, exptime string) string {
	return fmt.Sprintf("touch %s %s\r\n", key, exptime)
}

// TODO: pass chunk size
func sliceIndices(chunkNum int, totalLength int) (int, int) {
	// Indices for slicing. End is exclusive
	start := CHUNK_SIZE * chunkNum
	end := int(math.Min(float64(start+CHUNK_SIZE), float64(totalLength)))

	return start, end
}
