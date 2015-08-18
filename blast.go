package main

import "bufio"
import "fmt"
import "math/rand"
import "net"
import "time"
import "sync"

import "./rend-client"

type Task struct {
    cmd string
    key string
    value string
}

// constants and configuration
const VERBOSE = false
const NUM_TASK_GENS = 5
const NUM_COMMS = 10

// 2-character keys with 26 possibilities =        676 keys
// 3-character keys with 26 possibilities =     17,576 keys
// 4-character keys with 26 possibilities =    456,976 keys
// 5-character keys with 26 possibilities = 11,881,376 keys
const KEY_LENGTH = 4
const ITERS_PER_GEN = 10000

func main() {
    rand.Seed(time.Now().UTC().UnixNano())
    
    fmt.Printf("Performing %v operations total\n", ITERS_PER_GEN * NUM_TASK_GENS * 4)
    
    tasks := make(chan *Task)
    taskGens := new(sync.WaitGroup)
    comms := new(sync.WaitGroup)
    
    // spawn task generators
    for i := 0; i < NUM_TASK_GENS; i++ {
        taskGens.Add(4)
        go setGenerator(tasks, taskGens, ITERS_PER_GEN)
        go cmdGenerator(tasks, taskGens, ITERS_PER_GEN, "get")
        go cmdGenerator(tasks, taskGens, ITERS_PER_GEN, "delete")
        go cmdGenerator(tasks, taskGens, ITERS_PER_GEN, "touch")
    }

    // spawn communicators
    for i := 0; i < NUM_COMMS; i++ {
        comms.Add(1)
        
        conn, err := rend.Connect("localhost", 11212)
        if err != nil {
            i--
            comms.Add(-1)
            continue
        }
        
        go communicator(conn, tasks, comms)
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

func setGenerator(tasks chan *Task, taskGens *sync.WaitGroup, numTasks int) {
    for i := 0; i < numTasks; i++ {
        // Random length between 1k and 10k
        valLen := rand.Intn(9 * 1024) + 1024
        
        task := new(Task)
        task.cmd = "set"
        task.key = rend.RandString(4)
        task.value = rend.RandString(valLen)
        
        tasks <- task
    }
    
    fmt.Println("set gen done")
    
    taskGens.Done()
}

func cmdGenerator(tasks chan *Task, taskGens *sync.WaitGroup, numTasks int, cmd string) {
    for i := 0; i < numTasks; i++ {
        task := new(Task)
        task.cmd = cmd
        task.key = rend.RandString(4)
        
        tasks <- task
    }
    
    fmt.Println(cmd, "gen done")
    
    taskGens.Done()
}

func communicator(conn net.Conn, tasks chan *Task, comms *sync.WaitGroup) {
    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)
    
    for item := range tasks {
        var err error
        
        switch item.cmd {
            case "set":
                err = rend.Set(reader, writer, item.key, item.value)
            case "get":
                err = rend.Get(reader, writer, item.key)
            case "delete":
                err = rend.Delete(reader, writer, item.key)
            case "touch":
                err = rend.Touch(reader, writer, item.key)
        }
        
        if err != nil {
            fmt.Printf("Error performing operation %s on key %s: %s", item.cmd, item.key, err.Error())
        }
    }
    
    fmt.Println("comm done")

    comms.Done()
}
