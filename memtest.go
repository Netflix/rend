package main

import "bufio"
import "fmt"
import "math/rand"
import "net"
import "time"
import "sync"

type CacheItem struct {
    key string
    value string
}

// constants and configuration
// No constant arrays
var letters = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
const verbose = false
const numThreads = 10

// 4-character keys with 26 possibilities = 456,976 keys
// 5-character keys with 26 possibilities = 11,881,376 keys
const keyLength = 4
//const numKeys = 1000000
const numKeys = 100000

// concurrency bits
var wg sync.WaitGroup
var tasks chan *CacheItem

func main() {

    rand.Seed(time.Now().UTC().UnixNano())
    tasks = make(chan *CacheItem, 64)

    // spawn worker goroutines
    for i := 0; i < numThreads; i++ {
        wg.Add(1)
        go worker(connect("localhost"))
    }

    key := ""
    value := ""
    valLen := 0

    for i := 0; i < numKeys; i++ {
        key = randString(keyLength)

        // Random length between 1k and 10k
        valLen = rand.Intn(9 * 1024) + 1024
        value = randString(valLen)

        item := new(CacheItem)
        item.key = key
        item.value = value

        tasks <- item

        if i % 10000 == 0 {
            fmt.Printf("%v\r\n", i)
        }
    }

    close(tasks)
    wg.Wait()
}

func worker(conn net.Conn) {
    for item := range tasks {
        set(conn, item.key, item.value)
        //get(conn, item.key)
    }

    wg.Done()
}

func randString(n int) string {
    b := make([]rune, n)

    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }

    return string(b)
}

func connect(host string) net.Conn {
    conn, err := net.Dial("tcp", host + ":11212")

    if err != nil {
        panic(err)
    }

    println("Connected to memcached.")

    return conn
}

func set(conn net.Conn, key string, value string) {
    if verbose { println(fmt.Sprintf("Setting key %v to value of length %v", key, len(value))) }

    fmt.Fprintf(conn, "set %v 0 0 %v\r\n", key, len(value))
    fmt.Fprintf(conn, "%v\r\n", value)
    response, err := bufio.NewReader(conn).ReadString('\n')

    if err != nil { panic(err) }

    if verbose { print(response) }
}

func get(conn net.Conn, key string) {
    if verbose { println(fmt.Sprintf("Getting key %v", key)) }

    fmt.Fprintf(conn, "get %v\r\n", key)

    reader := bufio.NewReader(conn)

    // read the header line
    response, err := reader.ReadString('\n')

    if err != nil { panic(err) }

    if verbose { print(response) }

    // then read the value
    response, err = reader.ReadString('\n')

    if err != nil { panic(err) }

    //print(response)
    if verbose { println("(read the value)") }

    // then read the END
    response, err = reader.ReadString('\n')

    if err != nil { panic(err) }

    if verbose { print(response) }
}
