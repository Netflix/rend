package main

import "bufio"
import "fmt"
import "math/rand"
import "net"
import "time"
import "sync"

import "./rend"

type CacheItem struct {
    key string
    value string
}

// constants and configuration
const verbose = true
const numWorkers = 10

// 2-character keys with 26 possibilities =        676 keys
// 3-character keys with 26 possibilities =     17,576 keys
// 4-character keys with 26 possibilities =    456,976 keys
// 5-character keys with 26 possibilities = 11,881,376 keys
const keyLength = 4
const numKeys = 100000

func main() {

    rand.Seed(time.Now().UTC().UnixNano())
    tasks := make(chan *CacheItem)
    wg := new(sync.WaitGroup)
    wg.Add(numWorkers)

    // spawn worker goroutines
    for i := 0; i < numWorkers; i++ {
        conn, err := rend.Connect("localhost", 11212)
        if err != nil { fmt.Println("Error:", err.Error()) }
        go worker(conn, tasks, wg)
    }
    
    source := rend.RandString(10240)

    for i := 0; i < numKeys; i++ {
        // Random length between 1k and 10k
        valLen := rand.Intn(9 * 1024) + 1024

        item := new(CacheItem)
        item.key = rend.RandString(keyLength)
        item.value = source[:valLen]
        tasks <- item

        if i % 10000 == 0 {
            fmt.Printf("%v\r\n", i)
        }
    }

    close(tasks)
    wg.Wait()
}

func worker(conn net.Conn, tasks chan *CacheItem, wg *sync.WaitGroup) {
    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)
    
    for item := range tasks {
        rend.Set(reader, writer, item.key, item.value)
        rend.Get(reader, writer, item.key)
    }

    wg.Done()
}
