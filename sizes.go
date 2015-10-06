package main

import "bufio"
import "fmt"
import "math/rand"
import "sync"
import "time"

import "./client/common"
import "./client/textprot"

func main() {
    var t textprot.TextProt
    var wg sync.WaitGroup

    rand.Seed(time.Now().UTC().UnixNano())

    wg.Add(10)
    for i := 0; i < 10; i++ {
        go doStuff(t, wg)
    }

    wg.Wait()
}

func doStuff(prot common.Prot, wg sync.WaitGroup) {
    conn, err := common.Connect("localhost", 11212)
    if err != nil {
        panic("Couldn't connect")
    }

    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)

    for i := 0; i < 10240; i++ {
        key := common.RandData(4)
        value := common.RandData(i)

        prot.Set(reader, writer, key, value)
        prot.Get(reader, writer, key)
    }

    fmt.Println("Done.")
    wg.Done()
}
