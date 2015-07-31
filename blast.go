package main

import "bufio"
import "fmt"
import "math/rand"
import "net"
import "time"
import "strings"
import "sync"

type Task struct {
    cmd string
    key string
    value string
}

// constants and configuration
// No constant arrays :(
var letters = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
const VERBOSE = false
const NUM_TASK_GENS = 5
const NUM_COMMS = 10

// 2-character keys with 26 possibilities =        676 keys
// 3-character keys with 26 possibilities =     17,576 keys
// 4-character keys with 26 possibilities =    456,976 keys
// 5-character keys with 26 possibilities = 11,881,376 keys
const KEY_LENGTH = 4
const ITERS_PER_GEN = 100000

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
        go communicator(connect("localhost"), tasks, comms)
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
        task.key = randString(4)
        task.value = randString(valLen)
        
        tasks <- task
    }
    
    println("set gen done")
    
    taskGens.Done()
}

func getGenerator(tasks chan *Task, taskGens *sync.WaitGroup, numTasks int) {
    for i := 0; i < numTasks; i++ {
        task := new(Task)
        task.cmd = "get"
        task.key = randString(4)
        
        tasks <- task
    }
    
    println("get gen done")
    
    taskGens.Done()
}

func communicator(conn net.Conn, tasks chan *Task, comms *sync.WaitGroup) {
    for item := range tasks {
        if item.cmd == "get" {
            get(conn, item.key)
        } else {
            set(conn, item.key, item.value)
        }
    }
    
    println("comm done")

    comms.Done()
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
    if err != nil { panic(err) }

    println("Connected to memcached.")

    return conn
}

func set(conn net.Conn, key string, value string) {
    if VERBOSE { println(fmt.Sprintf("Setting key %v to value of length %v", key, len(value))) }

    fmt.Fprintf(conn, "set %v 0 0 %v\r\n", key, len(value))
    fmt.Fprintf(conn, "%v\r\n", value)
    response, err := bufio.NewReader(conn).ReadString('\n')

    if err != nil { panic(err) }

    if VERBOSE { print(response) }
}

func get(conn net.Conn, key string) {
    if VERBOSE { println(fmt.Sprintf("Getting key %v", key)) }

    fmt.Fprintf(conn, "get %v\r\n", key)

    reader := bufio.NewReader(conn)

    // read the header line
    response, err := reader.ReadString('\n')
    if err != nil { panic(err) }
    if VERBOSE { print(response) }
    
    if strings.TrimSpace(response) == "END" {
        if VERBOSE { println("Empty response / cache miss") }
        return
    }

    // then read the value
    response, err = reader.ReadString('\n')
    if err != nil { panic(err) }
    if VERBOSE { print(response) }

    // then read the END
    response, err = reader.ReadString('\n')
    if err != nil { panic(err) }
    if VERBOSE { print(response) }
}
