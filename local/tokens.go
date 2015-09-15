/**
 * Background goroutine to keep 1000 tokens around for heavy write loads
 */
package local

import "crypto/rand"

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
