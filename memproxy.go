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

const ENCODING = binary.LittleEndian

type SetCmdLine struct {
    cmd     string
    key     string
    flags   int
    exptime string
    length  int
}

type GetCmdLine struct {
    cmd string
    keys []string
}

const METADATA_SIZE = 16
type Metadata struct {
    length    int32
    origFlags int32
    numChunks int32
    chunkSize int32
}

func main() {
    server, err := net.Listen("tcp", ":11212")
    
    if err != nil { print(err.Error()) }
    
    for {
        remote, err := server.Accept()
        
        if err != nil {
            fmt.Println(err.Error())
            continue
        }
    
        local, err := net.Dial("tcp", ":11211")
        
        if err != nil {
            fmt.Println(err.Error())
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
                fmt.Println("Connection closed")
            } else {
                fmt.Println(err.Error())
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
                    fmt.Fprintf(remoteWriter, "CLIENT_ERROR length is not a valid integer")
                    remote.Close()
                }
                
                flags, err := strconv.Atoi(strings.TrimSpace(clParts[2]))
                
                if err != nil {
                    print(err.Error())
                    fmt.Fprintf(remoteWriter, "CLIENT_ERROR length is not a valid integer")
                    remote.Close()
                }
                
                cmd := SetCmdLine {
                    cmd:     clParts[0],
                    key:     clParts[1],
                    flags:   flags,
                    exptime: clParts[3],
                    length:  length,
                }
                
                err = handleSet(cmd, remoteReader, remoteWriter, localReader, localWriter)
                
                if err != nil {
                    print(err.Error())
                    fmt.Fprintf(remoteWriter, "ERROR could not process command")
                    remote.Close()
                }
                
            case "get":
                cmd := GetCmdLine {
                    cmd:  clParts[0],
                    keys: clParts[1:],
                }
                
                err = handleGet(cmd, remoteReader, remoteWriter, localReader, localWriter)
                
                if err != nil {
                    print(err.Error())
                    fmt.Fprintf(remoteWriter, "ERROR could not process command")
                    remote.Close()
                }
        }
    }
}

func handleSet(cmd SetCmdLine, remoteReader *bufio.Reader, remoteWriter *bufio.Writer,
                                localReader *bufio.Reader,  localWriter *bufio.Writer) error {
    // Read in the data from the remote connection
    buf, err := (remoteReader, cmd.length)
    
    numChunks := int(math.Ceil(float64(cmd.length) / float64(CHUNK_SIZE)))
    
    metaKey := makeMetaKey(cmd.key)
    metaData := Metadata {
        length:    int32(cmd.length),
        numChunks: int32(numChunks),
        chunkSize: CHUNK_SIZE,
    }
    
    if verbose {
        fmt.Println("metaKey:", metaKey)
        fmt.Println("numChunks:", numChunks)
    }
    
    metaDataBuf := new(bytes.Buffer)
    binary.Write(metaDataBuf, ENCODING, metaData)
    
    // Write metadata key
    localCmd := makeSetCommand(metaKey, cmd.exptime, METADATA_SIZE)
    err = setLocal(localWriter, localCmd, metaDataBuf.Bytes())
    if err != nil { return err }
    
    // Read server's response
    // TODO: Error handling of ERROR response
    response, err := localReader.ReadString('\n')
    
    if verbose { fmt.Println(response) }
    
    // Write all the data chunks
    // TODO: Clean up if a data chunk write fails
    // Failure can mean the write failing at the I/O level
    // or at the memcached level, e.g. response == ERROR
    for i := 0; i < numChunks; i++ {
        // Build this chunk's key
        key := makeChunkKey(cmd.key, i)
        
        if verbose { fmt.Println(key) }
        
        // indices for slicing, end exclusive
        start, end := sliceIndices(i, cmd.length)
        
        chunkBuf := buf[start:end]
        
        // Pad the data to always be CHUNK_SIZE
        if (end-start) < CHUNK_SIZE {
            padding := CHUNK_SIZE - (end-start)
            padtext := bytes.Repeat([]byte{byte(0)}, padding)
            chunkBuf = append(chunkBuf, padtext...)
        }
        
        // Write the key
        localCmd = makeSetCommand(key, cmd.exptime, CHUNK_SIZE)
        err = setLocal(localWriter, localCmd, chunkBuf)
        if err != nil { return err }
        
        // Read server's response
        // TODO: Error handling of ERROR response from memcached
        response, _ := localReader.ReadString('\n')
        
        if verbose { fmt.Println(response) }
    }
    
    // TODO: Error handling for less bytes
    //numWritten, err := writer.WriteString("STORED\r\n")
    _, err = remoteWriter.WriteString("STORED\r\n")
    if err != nil { return err }
    
    err = remoteWriter.Flush()
    if err != nil { return err }
    
    if verbose { fmt.Println("stored!") }
    return nil
}

func handleGet(cmd GetCmdLine, remoteReader *bufio.Reader, remoteWriter *bufio.Writer,
                                localReader *bufio.Reader,  localWriter *bufio.Writer) error {
    // read index
    // make buf
    // for numChunks do
    //   read chunk, append to buffer
    // send response
    
    for _, key := range cmd.keys {
        metaKey := makeMetaKey(key)
        
        if verbose { fmt.Println("metaKey:", metaKey) }
        
        // Read in the metadata for number of chunks, chunk size, etc.
        metaBytes, err := getLocal(localReader, localWriter, metaKey, METADATA_SIZE)
        if err != nil { return err }
        
        metaDataBuf := new(bytes.Buffer)
        metaDataBuf.Write(metaBytes)
        
        var metaData Metadata
        binary.Read(metaDataBuf, ENCODING, metaData)
        
        dataBuf := make([]byte, metaData.length)
        
        for i := 0; i < metaData.numChunks; i++ {
            chunkKey := makeChunkKey(key, i)
            
            // indices for slicing, end exclusive
            start, end := sliceIndices(i, metaData.length)
            
            chunkBuf := dataBuf[start:end]
            
            readDataIntoBuf
        }
    }
    
    return nil
}

func makeMetaKey(key string) string {
    return fmt.Sprintf("%s_meta", key)
}

func makeChunkKey(key string, chunk int) string {
    return fmt.Sprintf("$s_%d", key, chunk)
}

// <command name> <key> <flags> <exptime> <bytes>\r\n
func makeSetCommand(key string, exptime string, size int) string {
    return fmt.Sprintf("set %s 0 %s %d\r\n", key, exptime, size)
}

func sliceIndices(chunkNum, totalLength) int, int {
    // Indices for slicing. End is exclusive
    start := CHUNK_SIZE * chunkNum
    end := int(math.Min(float64(start + CHUNK_SIZE), float64(totalLength)))
    
    return start, end
}

func readData(reader *bufio.Reader, length int) []byte, err {
    // Make a new slice to hold the content. For now this seems performant enough.
    buf := make([]byte, cmd.length)
    
    // Read into an explicitly sized buffer because there may be \r's and \n's in the data
    err := readDataIntoBuf(reader, buf)
    if err != nil { return nil, err }
    
    return buf, nil 
}

func readDataIntoBuf(reader *bufio.Reader, buf []byte) err {
    // TODO: real error handling for not enough bytes
    _, err := io.ReadFull(reader, buf)
    if err != nil { return err }
    
    // Consume the \r\n at the end of the data
    _, err = remoteReader.ReadBytes('\n')
    if err != nil { return err }
    
    return nil
}

func setLocal(localWriter *bufio.Writer, cmd string, data []byte) error {
    // Write key/cmd
    if verbose { fmt.Println("cmd:", cmd) }
    _, err := localWriter.WriteString(cmd)
    if err != nil { return err }
    
    // Write value
    _, err = localWriter.Write(data)
    if err != nil { return err }
    
    // Write data end marker
    _, err = localWriter.WriteString("\r\n")
    if err != nil { return err }
    
    localWriter.Flush()
    return nil
}

// TODO: Batch get
func getLocal(localReader *bufio.Reader, localWriter *bufio.writer, cmd string, length int) []byte, error {
    // Write key/cmd
    if verbose { fmt.Println("cmd:", cmd) }
    _, err := fmt.Fprintf(localWriter, cmd)
    if err != nil { return nil, err }
    
    // Read in value
    data, err := readData(localReader, length)
    if err != nil { return nil, err }
    
    return data, nil
}

// TODO: Batch get
func getLocalIntoBuf(localReader *bufio.Reader, localWriter *bufio.writer, cmd string, length int) []byte, error {
    // Write key/cmd
    if verbose { fmt.Println("cmd:", cmd) }
    _, err := fmt.Fprintf(localWriter, cmd)
    if err != nil { return nil, err }
    
    // Read in value
    data, err := readData(localReader, length)
    if err != nil { return nil, err }
    
    return data, nil
}
