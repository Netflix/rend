package rend

import "bufio"
import "fmt"
import "math/rand"
import "net"
import "strings"

// constants and configuration
// No constant arrays :(
var letters = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
const VERBOSE = false

func RandString(n int) string {
    b := make([]rune, n)

    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }

    return string(b)
}

func Connect(host string) (net.Conn, error) {
    conn, err := net.Dial("tcp", host + ":11212")
    if err != nil { return nil, err }

    fmt.Println("Connected to memcached.")

    return conn, nil
}

func Set(reader *bufio.Reader, writer *bufio.Writer, key string, value string) error {
    if VERBOSE { fmt.Printf("Setting key %v to value of length %v", key, len(value)) }

    fmt.Fprintf(writer, "set %v 0 0 %v\r\n", key, len(value))
    fmt.Fprintf(writer, "%v\r\n", value)
    writer.Flush()
    
    response, err := reader.ReadString('\n')
    if err != nil { return err }

    if VERBOSE { fmt.Println(response) }
    
    return nil
}

func Get(reader *bufio.Reader, writer *bufio.Writer, key string) error {
    if VERBOSE { fmt.Printf("Getting key %v", key) }

    fmt.Fprintf(writer, "get %v\r\n", key)
    writer.Flush()
    
    // read the header line
    response, err := reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }
    
    if strings.TrimSpace(response) == "END" {
        if VERBOSE { fmt.Println("Empty response / cache miss") }
        return nil
    }

    // then read the value
    response, err = reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }

    // then read the END
    response, err = reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }
    
    return nil
}

func Delete(reader *bufio.Reader, writer *bufio.Writer, key string) {
    //if VERBOSE {fmt.Println}
}

func Touch(reader *bufio.Reader, writer *bufio.Writer, key string) {
    // if VERBOSE
}
