package common

import "fmt"
import "math/rand"
import "net"

// constants and configuration
// No constant arrays :(
var letters = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandData(n int) []byte {
    b := make([]byte, n)

    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }

    return b
}

func Connect(host string, port int) (net.Conn, error) {
    conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", host, port))
    if err != nil { return nil, err }

    fmt.Println("Connected to memcached.")

    return conn, nil
}