package main

import "bufio"
import "fmt"
import "math/rand"
import "net"
import "time"
import "sync"

import "./rend"

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
const ITERS_PER_GEN = 1000

func main() {
    rand.Seed(time.Now().UTC().UnixNano())
    
    fmt.Printf("Performing %v operations total\n", ITERS_PER_GEN * NUM_TASK_GENS * 2)
    
    tasks := make(chan *Task, 10000)
    taskGens := new(sync.WaitGroup)
    comms := new(sync.WaitGroup)
    
    // spawn task generators
    for i := 0; i < NUM_TASK_GENS; i++ {
        taskGens.Add(2)
        go setGenerator(tasks, taskGens, ITERS_PER_GEN)
        go getGenerator(tasks, taskGens, ITERS_PER_GEN)
    }

    // spawn communicators
    for i := 0; i < NUM_COMMS; i++ {
        comms.Add(1)
        go communicator(rend.Connect("localhost"), tasks, comms)
    }

    // First wait for all the tasks to be generated,
    // then close the channel so the comm threads complete
    println("Waiting for taskGens.")
    taskGens.Wait()
    println("Task gens done.")
    close(tasks)
    println("Tasks closed, waiting on comms.")
    comms.Wait()
    println("Comms done.")
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
    
    println("set gen done")
    
    taskGens.Done()
}

func getGenerator(tasks chan *Task, taskGens *sync.WaitGroup, numTasks int) {
    for i := 0; i < numTasks; i++ {
        task := new(Task)
        task.cmd = "get"
        task.key = rend.RandString(4)
        
        tasks <- task
    }
    
    println("get gen done")
    
    taskGens.Done()
}

func communicator(conn net.Conn, tasks chan *Task, comms *sync.WaitGroup) {
    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)
    
    for item := range tasks {
        if item.cmd == "get" {
            rend.Get(reader, writer, item.key)
        } else {
            rend.Set(reader, writer, item.key, item.value)
        }
    }
    
    println("comm done")

    comms.Done()
}
