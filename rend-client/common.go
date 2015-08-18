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

func Connect(host string, port int) (net.Conn, error) {
    conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", host, port))
    if err != nil { return nil, err }

    fmt.Println("Connected to memcached.")

    return conn, nil
}

func Set(reader *bufio.Reader, writer *bufio.Writer, key string, value string) error {
    if VERBOSE { fmt.Printf("Setting key %v to value of length %v\r\n", key, len(value)) }

    _, err := fmt.Fprintf(writer, "set %v 0 0 %v\r\n", key, len(value))
    if err != nil { return err }
    _, err = fmt.Fprintf(writer, "%v\r\n", value)
    if err != nil { return err }
    writer.Flush()
    
    response, err := reader.ReadString('\n')
    if err != nil { return err }

    if VERBOSE { fmt.Println(response) }
    
    if VERBOSE { fmt.Printf("Set key %v\r\n", key) }
    return nil
}

func Get(reader *bufio.Reader, writer *bufio.Writer, key string) error {
    if VERBOSE { fmt.Printf("Getting key %v\r\n", key) }

    _, err := fmt.Fprintf(writer, "get %v\r\n", key)
    if err != nil { return err }
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
    
    if VERBOSE { fmt.Printf("Got key %v\r\n", key) }
    return nil
}

func Delete(reader *bufio.Reader, writer *bufio.Writer, key string) error {
    if VERBOSE { fmt.Printf("Deleting key %s\r\n", key) }
    
    _, err := fmt.Fprintf(writer, "delete %s\r\n", key)
    if err != nil { return err }
    writer.Flush()
    
    response, err := reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }
    
    if VERBOSE { fmt.Printf("Deleted key %s\r\n", key) }
    return nil
}

func Touch(reader *bufio.Reader, writer *bufio.Writer, key string) error {
    if VERBOSE { fmt.Printf("Touching key %s\r\n", key) }
    
    _, err := fmt.Fprintf(writer, "touch %s 123456\r\n", key)
    if err != nil { return err }
    writer.Flush()
    
    response, err := reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }
    
    if VERBOSE { fmt.Printf("Touched key %s\r\n", key) }
    return nil
}
