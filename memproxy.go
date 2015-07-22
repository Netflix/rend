/**
 * Memproxy is a proxy for memcached that will split the data input
 * into fixed-size chunks for storage. It will reassemble the data
 * on retrieval with set.
 */
package main

import "bufio"
import "bytes"
import "encoding/binary"
import "fmt"
import "io"
import "math"
import "net"
import "strconv"
import "strings"

const verbose = false

const CHUNK_SIZE = 1024
const CHUNK_SIZE_STR = "1024"

type SetCmdLine struct {
    cmd     string
    key     string
    flags   string
    exptime string
    length  int
}

type GetCmdLine struct {
    cmd string
    keys []string
}

// TODO: Flags?
const METADATA_SIZE = 12
const METADATA_SIZE_STR = "12"
type Metadata struct {
    length    int32
    numChunks int32
    chunkSize int32
}

func main() {
    server, err := net.Listen("tcp", ":11212")
    
    if err != nil { print(err.Error()) }
    
    for {
        remote, err := server.Accept()
        
        if err != nil {
            print(err.Error())
            continue
        }
    
        local, err := net.Dial("tcp", ":11211")
        
        if err != nil {
            print(err.Error())
            continue
        }
        
        go handleConnection(remote, local)
    }
}

// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
func handleConnection(remote net.Conn, local net.Conn) {
    remoteReader := bufio.NewReader(remote)
    remoteWriter := bufio.NewWriter(remote)
    localReader  := bufio.NewReader(local)
    localWriter  := bufio.NewWriter(local)
    
    for {
        data, err := remoteReader.ReadString('\n')
        
        if err != nil {
            if err == io.EOF {
                println("Connection closed")
            } else {
                println(err.Error())
            }
            remote.Close()
            return
        }
        
        clParts := strings.Split(data, " ")
        
        switch clParts[0] {
            case "set":
                length, err := strconv.Atoi(strings.TrimSpace(clParts[4]))
                
                if err != nil {
                    print(err.Error())
                    remoteWriter.WriteString("CLIENT_ERROR length is not a valid integer")
                    remoteWriter.Flush()
                    remote.Close()
                }
                
                cmd := SetCmdLine {
                    cmd:     clParts[0],
                    key:     clParts[1],
                    flags:   clParts[2],
                    exptime: clParts[3],
                    length:  length,
                }
                
                err = handleSet(cmd, remoteReader, remoteWriter, localReader, localWriter)
                
                if err != nil {
                    print(err.Error())
                    remoteWriter.WriteString("ERROR could not process command")
                    remoteWriter.Flush()
                    remote.Close()
                }
                
            case "get":
                println("doing something for get")
        }
    }
}

func handleSet(cmd SetCmdLine, remoteReader *bufio.Reader, remoteWriter *bufio.Writer,
                                localReader *bufio.Reader,  localWriter *bufio.Writer) error {
    // Make a new slice to hold the content
    // For now this seems performant enough
    buf := make([]byte, cmd.length)
    
    // Read into an explicitly sized buffer
    // because there may be \r's and \n's in the data
    _, err := io.ReadFull(remoteReader, buf)
    
    // TODO: real error handling for less bytes
    if err != nil { return err }
    
    // Consume the \r\n at the end of the data
    _, err = remoteReader.ReadBytes('\n')
    if err != nil { return err }
    
    numChunks := int32(math.Ceil(float64(cmd.length) / float64(CHUNK_SIZE)))
    
    metaKey := strings.Join([]string { cmd.key, "_meta"}, "")
    metaData := Metadata {
        numChunks: numChunks,
        chunkSize: CHUNK_SIZE,
    }
    
    if verbose {
        println(metaKey)
        print("numChunks: ")
        println(numChunks)
    }
    
    metaDataBuf := new(bytes.Buffer)
    // TODO: handle write failure
    binary.Write(metaDataBuf, binary.LittleEndian, metaData)
    
    // <command name> <key> <flags> <exptime> <bytes>\r\n
    // Write metadata key
    fmt.Fprintf(localWriter, "set %s 0 %s %s\r\n", metaKey, cmd.exptime, METADATA_SIZE_STR)
    if verbose { fmt.Printf("set %s 0 %s %s\r\n", metaKey, cmd.exptime, METADATA_SIZE_STR) }
    
    // Write metadata value
    // TODO: Handle write failure
    localWriter.Write(metaDataBuf.Bytes())
    localWriter.WriteString("\r\n")
    localWriter.Flush()
    
    // Read server's response
    // TODO: Error handling of ERROR response
    response, err := localReader.ReadString('\n')
    
    if verbose { println(response) }
    
    // Write all the data chunks
    // TODO: Clean up if a data chunk write fails
    // Failure can mean the write failing at the I/O level
    // or at the memcached level, e.g. response == ERROR
    for i := 0; i < int(numChunks); i++ {
        // Build this chunk's key
        key := strings.Join([]string { cmd.key, "_", strconv.Itoa(i) }, "")
        
        if verbose { println(key) }
        
        // Indices for slicing. End is exclusive
        start := CHUNK_SIZE * i
        end := int(math.Min(float64(start + CHUNK_SIZE), float64(cmd.length)))
        
        chunkBuf := buf[start:end]
        
        // Pad the data to always be CHUNK_SIZE
        if (end-start) < CHUNK_SIZE {
            padding := CHUNK_SIZE - (end-start)
            padtext := bytes.Repeat([]byte{byte(0)}, padding)
            chunkBuf = append(chunkBuf, padtext...)
        }
        
        // Write the key
        fmt.Fprintf(localWriter, "set %s 0 %s %s\r\n", key, cmd.exptime, CHUNK_SIZE_STR)
        if verbose { fmt.Printf("set %s 0 %s %s\r\n", key, cmd.exptime, CHUNK_SIZE_STR) }
            
        // Write the value
        // TODO: Handle write failure
        numWritten, err := localWriter.Write(chunkBuf)
        
        if verbose {
            print("numWritten: ")
            println(numWritten)
        }
        
        if err != nil { return err }
        
        localWriter.WriteString("\r\n")
        localWriter.Flush()
        
        // Read server's response
        // TODO: Error handling of ERROR response
        //response, err := localReader.ReadString('\n')
        response, _ := localReader.ReadString('\n')
        
        if verbose { println(response) }
    }
    
    // TODO: Error handling for less bytes
    //written, err := writer.WriteString("STORED\r\n")
    _, err = remoteWriter.WriteString("STORED\r\n")
    if err != nil { return err }
    
    err = remoteWriter.Flush()
    if err != nil { return err }
    
    if verbose { println("stored!") }
    return nil
}