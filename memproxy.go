/**
 * Memproxy is a proxy for memcached that will split the data input
 * into fixed-size chunks for storage. It will reassemble the data
 * on retrieval with set.
 */
package main

import "bufio"
import "bytes"
import "crypto/rand"
import "encoding/binary"
import "errors"
import "fmt"
import "io"
import "math"
import "net"
import "strconv"
import "strings"

import "./binary"
import "./common"
import "./text"

const verbose = false

func main() {
    server, err := net.Listen("tcp", ":11212")
    
    if err != nil { print(err.Error()) }
    
    for {
        remote, err := server.Accept()
        
        if err != nil {
            fmt.Println(err.Error())
            remote.Close()
            continue
        }
        
        local, err := net.Dial("tcp", ":11211")
        
        if err != nil {
            fmt.Println(err.Error())
            if local != nil { local.Close() }
            remote.Close()
            continue
        }
        
        go handleConnection(remote, local)
    }
}

func handleConnection(remote net.Conn, local net.Conn) {
    remoteReader := bufio.NewReader(remote)
    remoteWriter := bufio.NewWriter(remote)
    localReader  := bufio.NewReader(local)
    localWriter  := bufio.NewWriter(local)
    
    var binary   bool
    var request  interface{}
    var err      error
    
    for {
        if isBinaryRequest(remoteReader) {
            binary = true
            request = binary.Parse(remoteReader)
        } else {
            binary = false
            request = text.Parse(remoteReader)
        }
        
        // TODO: handle nil
        switch request.(type) {
            case common.SetRequest:
                err = common.HandleSet(request, remoteReader, localReader, localWriter)
                
            case common.DeleteRequest:
                err = common.HandleDelete(request, localReader, localWriter)
                
            case common.TouchRequest:
                err = common.HandleTouch(request, localReader, localWriter)
                
            case common.GetRequest:
                response, errChan := common.HandleSet(request, localReader, localWriter)
                
                for {
                    select {
                        case res, ok := <-response:
                            if !ok { response = nil }
                            // do something to write stuff
                        case err, ok = <-errChan:
                            if !ok { errChan = nil }
                            break
                    }
                    
                    if response == nil && errChan == nil {
                        break
                    }
                }
        }
        
        if err != nil {
            // separate fatal errors from "expected"
            fmt.Println("Error while processing request. Closing connection. Error:")
            fmt.Println(err.Error())
            // use proper serializer to respond here
            remote.Close()
        }
    }
}

func isBinaryRequest(reader *bufio.Reader) bool, err {
    headerByte, err := reader.Peek(1)
    if err != nil { return false, err }
    return headerByte == binary.HeaderByte
}
