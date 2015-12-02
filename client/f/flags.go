package f

import "flag"
import "os"

// 2-character keys with 26 possibilities =        676 keys
// 3-character keys with 26 possibilities =     17,576 keys
// 4-character keys with 26 possibilities =    456,976 keys
// 5-character keys with 26 possibilities = 11,881,376 keys

var Binary bool
var Text bool
var KeyLength int
var NumOps int
var NumWorkers int
var Port int

// Flags
func init() {
    flag.BoolVar(&Binary, "binary", false, "Use the binary protocol. Cannot be combined with --text or -t.")
    flag.BoolVar(&Binary, "b", false, "Use the binary protocol. Cannot be combined with --text or -t. (shorthand)")

    flag.BoolVar(&Text, "text", false, "Use the text protocol (default). Cannot be combined with --binary or -b.")
    flag.BoolVar(&Text, "t", false, "Use the text protocol (default). Cannot be combined with --binary or -b. (shorthand)")

    flag.IntVar(&KeyLength, "key-length", 4, "Length in bytes of each key. Smaller values mean more overlap.")
    flag.IntVar(&KeyLength, "kl", 4, "Length in bytes of each key. Smaller values mean more overlap. (shorthand)")

    flag.IntVar(&NumOps, "num-ops", 1000000, "Total number of operations to perform.")
    flag.IntVar(&NumOps, "n", 1000000, "Total number of operations to perform. (shorthand)")

    flag.IntVar(&NumWorkers, "workers", 10, "Number of communication goroutines to run.")
    flag.IntVar(&NumWorkers, "w", 10, "Number of communication goroutines to run.")

    flag.IntVar(&Port, "port", 11212, "Port to connect to.")
    flag.IntVar(&Port, "p", 11212, "Port to connect to. (shorthand)")

    flag.Parse()

    if (Binary && Text) || KeyLength <= 0 || NumOps <= 0 {
        flag.Usage()
        os.Exit(1)
    }
}