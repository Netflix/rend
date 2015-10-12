package main

import "flag"
import "fmt"
import "io"
import "math/rand"
import "net"
import "os"
import "os/signal"
import "time"
import "sync"

import "./client/common"
import "./client/binprot"
import "./client/textprot"

type Task struct {
    cmd string
    key []byte
    value []byte
}

// constants and configuration
const VERBOSE = false

// 2-character keys with 26 possibilities =        676 keys
// 3-character keys with 26 possibilities =     17,576 keys
// 4-character keys with 26 possibilities =    456,976 keys
// 5-character keys with 26 possibilities = 11,881,376 keys

var binary bool
var text bool
var keyLength int
var numOps int
var numWorkers int

func init() {
    flag.BoolVar(&binary, "binary", false, "Use the binary protocol. Cannot be combined with --text or -t.")
    flag.BoolVar(&binary, "b", false, "Use the binary protocol. Cannot be combined with --text or -t. (shorthand)")

    flag.BoolVar(&text, "text", false, "Use the text protocol. Cannot be combined with --binary or -b.")
    flag.BoolVar(&text, "t", false, "Use the text protocol. Cannot be combined with --binary or -b. (shorthand)")

    flag.IntVar(&keyLength, "key-length", 4, "Length in bytes of each key. Smaller values mean more overlap.")
    flag.IntVar(&keyLength, "kl", 4, "Length in bytes of each key. Smaller values mean more overlap. (shorthand)")

    flag.IntVar(&numOps, "num-ops", 1000000, "Total number of operations to perform.")
    flag.IntVar(&numOps, "n", 1000000, "Total number of operations to perform. (shorthand)")

    flag.IntVar(&numWorkers, "workers", 10, "Number of communication goroutines to run.")
    flag.IntVar(&numWorkers, "w", 10, "Number of communication goroutines to run.")
}

func main() {
    sigs := make(chan os.Signal)
    signal.Notify(sigs, os.Interrupt)

    go func() {
        <-sigs
        panic("Keyboard Interrupt")
    }()

    flag.Parse()

    if (binary && text) || keyLength <= 0 || numOps <= 0 {
        flag.Usage()
        os.Exit(1)
    }

    var prot common.Prot
    if binary {
        var b binprot.BinProt
        prot = b
    } else {
        var t textprot.TextProt
        prot = t
    }

    rand.Seed(time.Now().UTC().UnixNano())
    
    fmt.Printf("Performing %v operations total, with %v communication goroutines\n", numOps, numWorkers)
    
    tasks := make(chan *Task)
    taskGens := new(sync.WaitGroup)
    comms := new(sync.WaitGroup)

    // TODO: Better math
    opsPerTask := numOps / 4 / numWorkers
    
    // spawn task generators
    for i := 0; i < numWorkers; i++ {
        taskGens.Add(4)
        go cmdGenerator(tasks, taskGens, opsPerTask, "set")
        go cmdGenerator(tasks, taskGens, opsPerTask, "get")
        go cmdGenerator(tasks, taskGens, opsPerTask, "delete")
        go cmdGenerator(tasks, taskGens, opsPerTask, "touch")
    }

    // spawn communicators
    for i := 0; i < numWorkers; i++ {
        comms.Add(1)
        
        conn, err := common.Connect("localhost", 11212)
        if err != nil {
            i--
            comms.Add(-1)
            continue
        }
        
        go communicator(prot, conn, tasks, comms)
    }

    // First wait for all the tasks to be generated,
    // then close the channel so the comm threads complete
    fmt.Println("Waiting for taskGens.")
    taskGens.Wait()
    fmt.Println("Task gens done.")
    close(tasks)
    fmt.Println("Tasks closed, waiting on comms.")
    comms.Wait()
    fmt.Println("Comms done.")
}

func cmdGenerator(tasks chan *Task, taskGens *sync.WaitGroup, numTasks int, cmd string) {
    for i := 0; i < numTasks; i++ {
        task := new(Task)
        task.cmd = cmd
        task.key = common.RandData(keyLength)

        if cmd == "set" {
            // Random length between 1k and 10k
            valLen := rand.Intn(9 * 1024) + 1024
            task.value = common.RandData(valLen)
        }
        
        tasks <- task
    }
    
    fmt.Println(cmd, "gen done")
    
    taskGens.Done()
}

func communicator(prot common.Prot, conn net.Conn, tasks chan *Task, comms *sync.WaitGroup) {
    for item := range tasks {
        var err error
        
        switch item.cmd {
            case "set":    err = prot.Set   (conn, item.key, item.value)
            case "get":    err = prot.Get   (conn, item.key)
            case "delete": err = prot.Delete(conn, item.key)
            case "touch":  err = prot.Touch (conn, item.key)
        }
        
        if err != nil {
            if err != binprot.ERR_KEY_NOT_FOUND {
                fmt.Printf("Error performing operation %s on key %s: %s\n", item.cmd, item.key, err.Error())
            }
            // if the socket was closed, stop. Otherwise keep on hammering.
            if err == io.EOF {
                break
            }
        }
    }
    
    fmt.Println("comm done")

    comms.Done()
}
